from __future__ import annotations

from datetime import datetime
from typing import Optional

from app.core.logging import get_logger
from app.db.session import SessionLocal
from app.engines.backtest_engine import BacktestEngine
from app.repositories.backtest_repository import BacktestRepository
from app.repositories.candle_repository import CandleRepository
from app.schemas.backtest import BacktestCandle, BacktestRequest, BacktestResponse
from app.strategies.registry import get_strategy
from app.utils.time import ensure_utc, utc_now

logger = get_logger(__name__)


class BacktestRunnerService:
    def __init__(self, engine: Optional[BacktestEngine] = None) -> None:
        self.engine = engine or BacktestEngine()

    def run_backtest(self, request: BacktestRequest) -> BacktestResponse:
        session = SessionLocal()
        run = None
        try:
            candle_repository = CandleRepository(session)
            backtest_repository = BacktestRepository(session)

            strategy = get_strategy(request.strategy_code)
            strategy_row = backtest_repository.ensure_strategy(
                code=strategy.key,
                name=strategy.name,
                description=strategy.description,
            )

            run = backtest_repository.create_run(
                strategy_id=strategy_row.id,
                params_json=request.model_dump(mode="json"),
            )
            session.commit()

            started_at = utc_now()
            backtest_repository.mark_running(run, started_at=started_at)
            session.commit()

            candles = candle_repository.list_candles(
                exchange_code=request.exchange_code,
                symbol_code=request.symbol,
                timeframe=request.timeframe,
                start_at=ensure_utc(request.start_at),
                end_at=ensure_utc(request.end_at),
            )
            if not candles:
                raise ValueError(
                    f"No candles found for {request.symbol} {request.timeframe} "
                    f"between {request.start_at.isoformat()} and {request.end_at.isoformat()}"
                )

            candle_payload = [
                BacktestCandle(
                    open_time=candle.open_time,
                    open=candle.open,
                    high=candle.high,
                    low=candle.low,
                    close=candle.close,
                    volume=candle.volume,
                )
                for candle in candles
            ]
            report = self.engine.run(
                request=request,
                strategy=strategy,
                candles=candle_payload,
                started_at=started_at,
                completed_at=utc_now,
            )
            report = report.model_copy(update={"run_id": run.id})

            backtest_repository.save_result(run.id, report)
            backtest_repository.mark_completed(run, completed_at=report.completed_at)
            session.commit()

            logger.info(
                "Backtest completed",
                extra={
                    "run_id": run.id,
                    "strategy_code": request.strategy_code,
                    "symbol": request.symbol,
                    "timeframe": request.timeframe,
                    "total_trades": report.metrics.total_trades,
                    "total_return_pct": str(report.metrics.total_return_pct),
                },
            )
            return report
        except Exception as exc:
            session.rollback()
            logger.exception(
                "Backtest failed",
                extra={
                    "strategy_code": request.strategy_code,
                    "symbol": request.symbol,
                    "timeframe": request.timeframe,
                },
            )
            if run is not None:
                try:
                    backtest_repository = BacktestRepository(session)
                    backtest_repository.mark_failed(run, completed_at=utc_now(), error_text=str(exc))
                    session.commit()
                except Exception:
                    session.rollback()
                    logger.exception("Failed to persist backtest failure state", extra={"run_id": run.id})
            raise
        finally:
            session.close()
