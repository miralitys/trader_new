from __future__ import annotations

import csv
import io
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.core.celery_app import celery_app
from app.domain.enums import BacktestStatus
from app.domain.schemas.backtest import BacktestRunRequest
from app.engines.backtest_engine import BacktestEngine
from app.repositories.market_data import MarketDataRepository
from app.repositories.strategy import StrategyRepository
from app.repositories.trading import TradingRepository
from app.strategies.registry import get_strategy
from app.strategies.types import CandleInput


class BacktestService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.market_repo = MarketDataRepository(db)
        self.strategy_repo = StrategyRepository(db)
        self.trading_repo = TradingRepository(db)
        self.engine = BacktestEngine()

    def create_run(self, payload: BacktestRunRequest) -> dict:
        params = payload.model_dump(mode="json")
        run = self.trading_repo.create_backtest_run(strategy_id=payload.strategy_id, params=params)
        self.db.commit()
        celery_app.send_task("app.background.tasks.run_backtest", args=[run.id])
        return {"run_id": run.id, "status": run.status}

    def execute_run(self, run_id: int) -> dict:
        run = self.trading_repo.get_backtest_run(run_id)
        if run is None:
            raise ValueError("backtest run not found")
        strategy_row = self.strategy_repo.get_strategy(run.strategy_id)
        config_row = self.strategy_repo.get_config(run.strategy_id)
        if strategy_row is None or config_row is None:
            raise ValueError("strategy or config missing")

        self.trading_repo.update_backtest_run(
            run,
            status=BacktestStatus.RUNNING.value,
            started_at=datetime.now(timezone.utc),
            error_message=None,
        )
        self.db.commit()

        try:
            params = run.params
            strategy_impl = get_strategy(strategy_row.key)
            base_config = {**config_row.settings, **params.get("config_overrides", {})}
            exchange = self.market_repo.get_exchange_by_slug("coinbase")
            if exchange is None:
                raise ValueError("coinbase exchange missing")

            combos = [(symbol, timeframe) for symbol in params["symbols"] for timeframe in params["timeframes"]]
            if not combos:
                raise ValueError("backtest requires at least one symbol and timeframe")
            allocation = float(params["initial_capital"]) / len(combos)

            all_trades: list[dict] = []
            all_equity: list[dict] = []
            for symbol_code, timeframe_code in combos:
                symbol = self.market_repo.get_symbol(exchange.id, symbol_code)
                timeframe = self.market_repo.get_timeframe(timeframe_code)
                if symbol is None or timeframe is None:
                    continue
                candles = self.market_repo.list_candles(
                    symbol_id=symbol.id,
                    timeframe_id=timeframe.id,
                    start=datetime.fromisoformat(params["start"]),
                    end=datetime.fromisoformat(params["end"]),
                )
                candle_inputs = [
                    CandleInput(
                        open_time=item.open_time,
                        close_time=item.close_time,
                        open=item.open,
                        high=item.high,
                        low=item.low,
                        close=item.close,
                        volume=item.volume,
                    )
                    for item in candles
                ]
                result = self.engine.run(
                    strategy=strategy_impl,
                    config={
                        **base_config,
                        "position_size_pct": params["position_sizing"],
                        "fee_bps": params["fee_bps"],
                        "slippage_bps": params["slippage_bps"],
                    },
                    candles=candle_inputs,
                    symbol=symbol_code,
                    timeframe=timeframe_code,
                    initial_capital=allocation,
                )
                all_trades.extend(result["trades"])
                all_equity.extend(
                    [{**point, "symbol": symbol_code, "timeframe": timeframe_code} for point in result["equity_curve"]]
                )

            summary = self._combined_summary(all_trades, float(params["initial_capital"]))
            self.trading_repo.save_backtest_result(
                run_id,
                summary=summary,
                equity_curve=all_equity,
                trades_json=all_trades,
            )
            self.trading_repo.update_backtest_run(
                run,
                status=BacktestStatus.COMPLETED.value,
                ended_at=datetime.now(timezone.utc),
            )
            self.trading_repo.create_log(
                category="backtest",
                level="info",
                message=f"Backtest {run_id} completed",
                strategy_id=run.strategy_id,
                context={"trades": len(all_trades)},
            )
            self.db.commit()
            return {"run_id": run_id, "status": "completed"}
        except Exception as exc:
            self.trading_repo.update_backtest_run(
                run,
                status=BacktestStatus.FAILED.value,
                ended_at=datetime.now(timezone.utc),
                error_message=str(exc),
            )
            self.trading_repo.create_log(
                category="backtest",
                level="error",
                message=f"Backtest {run_id} failed",
                strategy_id=run.strategy_id,
                context={"error": str(exc)},
            )
            self.db.commit()
            raise

    def list_runs(self) -> list:
        return self.trading_repo.list_backtest_runs()

    def get_run_result(self, run_id: int) -> dict:
        run = self.trading_repo.get_backtest_run(run_id)
        if run is None:
            raise ValueError("backtest run not found")
        result = self.trading_repo.get_backtest_result(run_id)
        return {
            "run": run,
            "summary": result.summary if result else {},
            "equity_curve": result.equity_curve if result else [],
            "trades": result.trades_json if result else [],
        }

    def export(self, run_id: int, format_name: str) -> str | dict:
        result = self.get_run_result(run_id)
        if format_name == "json":
            return result
        if format_name == "csv":
            buffer = io.StringIO()
            writer = csv.DictWriter(
                buffer,
                fieldnames=[
                    "symbol",
                    "timeframe",
                    "entry_time",
                    "exit_time",
                    "entry_price",
                    "exit_price",
                    "quantity",
                    "gross_pnl",
                    "net_pnl",
                    "fee",
                    "slippage",
                    "reason",
                ],
            )
            writer.writeheader()
            writer.writerows(result["trades"])
            return buffer.getvalue()
        raise ValueError("unsupported export format")

    def _combined_summary(self, trades: list[dict], initial_capital: float) -> dict:
        from app.services.metrics import calculate_trade_metrics

        metrics = calculate_trade_metrics(
            [
                {
                    **trade,
                    "entry_time": datetime.fromisoformat(trade["entry_time"]),
                    "exit_time": datetime.fromisoformat(trade["exit_time"]),
                    "net_pnl": float(trade["net_pnl"]),
                }
                for trade in trades
            ],
            initial_capital,
        )
        return metrics
