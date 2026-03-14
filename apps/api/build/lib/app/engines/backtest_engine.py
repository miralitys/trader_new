from __future__ import annotations

from dataclasses import asdict
from typing import Any

from app.domain.enums import SignalAction
from app.engines.risk_engine import RiskEngine
from app.services.metrics import calculate_trade_metrics
from app.strategies.base import BaseStrategy
from app.strategies.types import CandleInput, PositionView, StrategyContext


class BacktestEngine:
    def __init__(self) -> None:
        self.risk_engine = RiskEngine()

    def run(
        self,
        *,
        strategy: BaseStrategy,
        config: dict,
        candles: list[CandleInput],
        symbol: str,
        timeframe: str,
        initial_capital: float,
        run_mode: str = "historical_backtest",
    ) -> dict[str, Any]:
        parsed_config = strategy.parse_config(config)
        cash = initial_capital
        position: PositionView | None = None
        trades: list[dict[str, Any]] = []
        equity_curve: list[dict[str, Any]] = []

        for index in range(len(candles)):
            window = candles[: index + 1]
            if len(window) < strategy.warmup_candles(parsed_config):
                continue

            latest = window[-1]
            context = StrategyContext(
                strategy_id=0,
                strategy_key=strategy.key,
                symbol=symbol,
                timeframe=timeframe,
                cash=cash,
                run_mode=run_mode,
                config=parsed_config,
                position=position,
            )
            signal = strategy.generate_signal(window, context)
            risk = self.risk_engine.evaluate(strategy, signal, context)

            if signal.action == SignalAction.ENTER and risk.approved and position is None:
                execution = strategy.simulate_execution(signal, latest, context, risk)
                if execution.quantity > 0:
                    cash -= execution.notional + execution.fee
                    position = PositionView(
                        entry_price=execution.price,
                        quantity=execution.quantity,
                        stop_loss=signal.stop_loss,
                        take_profit=signal.take_profit,
                        opened_at=latest.open_time,
                    )
            elif signal.action == SignalAction.EXIT and risk.approved and position is not None:
                execution = strategy.simulate_execution(signal, latest, context, risk)
                gross_pnl = (execution.price - position.entry_price) * position.quantity
                net_pnl = gross_pnl - execution.fee
                cash += execution.notional - execution.fee
                trades.append(
                    {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "entry_time": position.opened_at,
                        "exit_time": latest.open_time,
                        "entry_price": position.entry_price,
                        "exit_price": execution.price,
                        "quantity": position.quantity,
                        "gross_pnl": round(gross_pnl, 4),
                        "net_pnl": round(net_pnl, 4),
                        "fee": round(execution.fee, 4),
                        "slippage": round(execution.slippage, 4),
                        "reason": signal.reason,
                    }
                )
                position = None

            equity = cash + (position.quantity * latest.close if position else 0.0)
            equity_curve.append({"time": latest.open_time.isoformat(), "equity": round(equity, 2)})

        metrics = calculate_trade_metrics(trades, initial_capital)
        metrics["ending_equity"] = round(equity_curve[-1]["equity"], 2) if equity_curve else initial_capital
        metrics["open_position"] = position is not None
        return {
            "summary": metrics,
            "equity_curve": equity_curve,
            "trades": [
                {key: value.isoformat() if hasattr(value, "isoformat") else value for key, value in trade.items()}
                for trade in trades
            ],
        }
