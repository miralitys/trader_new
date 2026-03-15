from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from app.core.config import get_settings
from app.core.logging import get_logger
from app.db.session import SessionLocal
from app.engines.backtest_engine import BacktestEngine, BacktestStopRequestedError
from app.models.enums import BacktestStatus
from app.repositories.backtest_repository import BacktestRepository
from app.repositories.candle_repository import CandleRepository
from app.schemas.backtest import (
    BacktestCandle,
    BacktestDeleteBlockedItem,
    BacktestDeleteResponse,
    BacktestRequest,
    BacktestResponse,
)
from app.services.query_service import QueryService
from app.strategies.registry import get_strategy
from app.utils.time import ensure_utc, utc_now

logger = get_logger(__name__)


class BacktestRunnerService:
    def __init__(self, engine: Optional[BacktestEngine] = None) -> None:
        self.engine = engine or BacktestEngine()
        settings = get_settings()
        self.backtest_stale_after_seconds = settings.backtest_stale_after_seconds
        self.progress_interval_bars = settings.backtest_progress_interval_bars
        self.stop_check_interval_bars = settings.backtest_stop_check_interval_bars

    def run_backtest(self, request: BacktestRequest) -> BacktestResponse:
        self._recover_stale_runs()
        session = SessionLocal()
        run = None
        run_id: int | None = None
        status_finalized = False
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
            run_id = run.id
            session.commit()

            started_at = utc_now()
            backtest_repository.mark_running(run, started_at=started_at)
            session.commit()
            logger.info(
                "Backtest run started",
                extra={
                    "run_id": run.id,
                    "strategy_code": request.strategy_code,
                    "symbol": request.symbol,
                    "timeframe": request.timeframe,
                    "start_at": request.start_at.isoformat(),
                    "end_at": request.end_at.isoformat(),
                },
            )

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
            logger.info(
                "Backtest candles loaded",
                extra={
                    "run_id": run.id,
                    "candles_count": len(candles),
                    "first_candle_at": candles[0].open_time.isoformat(),
                    "last_candle_at": candles[-1].open_time.isoformat(),
                },
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
                progress_interval_bars=self.progress_interval_bars,
                progress_callback=lambda processed, total, candle_time: self._log_progress(
                    run_id=run.id,
                    processed_bars=processed,
                    total_bars=total,
                    candle_time=candle_time,
                ),
                stop_check_interval_bars=self.stop_check_interval_bars,
                should_abort=lambda processed, total, candle_time: self._should_abort_run(
                    run_id=run.id,
                    processed_bars=processed,
                    total_bars=total,
                    candle_time=candle_time,
                ),
            )
            report = report.model_copy(update={"run_id": run.id})

            backtest_repository.save_result(run.id, report)
            logger.info(
                "Backtest result persisted",
                extra={
                    "run_id": run.id,
                    "total_trades": report.metrics.total_trades,
                    "equity_points": len(report.equity_curve),
                },
            )
            backtest_repository.mark_completed(run, completed_at=report.completed_at)
            session.commit()
            status_finalized = True
            logger.info(
                "Backtest status marked completed",
                extra={
                    "run_id": run.id,
                    "completed_at": report.completed_at.isoformat() if report.completed_at is not None else None,
                },
            )

            logger.info(
                "Backtest completed",
                extra={
                    "run_id": run.id,
                    "strategy_code": request.strategy_code,
                    "symbol": request.symbol,
                    "timeframe": request.timeframe,
                    "total_trades": report.metrics.total_trades,
                    "total_return_pct": str(report.metrics.total_return_pct),
                    "duration_ms": int((report.completed_at - started_at).total_seconds() * 1000)
                    if report.completed_at is not None
                    else None,
                },
            )
            return report
        except BacktestStopRequestedError as exc:
            session.rollback()
            logger.info(
                "Backtest stop requested",
                extra={
                    "run_id": run_id,
                    "strategy_code": request.strategy_code,
                    "symbol": request.symbol,
                    "timeframe": request.timeframe,
                },
            )
            if run_id is not None and not status_finalized:
                self._mark_run_failed(run_id=run_id, error_text=str(exc))
                return self._load_backtest_response(run_id)
            raise
        except BaseException as exc:
            session.rollback()
            if isinstance(exc, Exception):
                logger.exception(
                    "Backtest failed",
                    extra={
                        "run_id": run_id,
                        "strategy_code": request.strategy_code,
                        "symbol": request.symbol,
                        "timeframe": request.timeframe,
                    },
                )
            else:
                logger.error(
                    "Backtest aborted by base exception",
                    extra={
                        "run_id": run_id,
                        "strategy_code": request.strategy_code,
                        "symbol": request.symbol,
                        "timeframe": request.timeframe,
                        "exception_type": exc.__class__.__name__,
                    },
                )
            if run_id is not None and not status_finalized:
                self._mark_run_failed(run_id=run_id, error_text=str(exc))
            raise
        finally:
            session.close()

    def stop_backtest(self, run_id: int, reason: str = "manual_stop") -> BacktestResponse:
        session = SessionLocal()
        try:
            backtest_repository = BacktestRepository(session)
            row = backtest_repository.get_run_with_result(run_id)
            if row is None:
                raise KeyError(f"Backtest {run_id} was not found")

            run, _strategy, result = row
            if run.status == BacktestStatus.COMPLETED:
                return self._load_backtest_response(run_id)
            if run.status == BacktestStatus.FAILED:
                return self._load_backtest_response(run_id)

            if result is not None:
                completed_at = run.completed_at or result.created_at or utc_now()
                backtest_repository.mark_completed(run, completed_at=completed_at)
            else:
                backtest_repository.mark_failed(
                    run,
                    completed_at=utc_now(),
                    error_text=f"manual_stop:{reason}",
                )
            session.commit()
            logger.info(
                "Backtest stop requested via API",
                extra={
                    "run_id": run_id,
                    "reason": reason,
                    "status_after_stop": run.status.value,
                },
            )
        finally:
            session.close()

        return self._load_backtest_response(run_id)

    def delete_backtests(self, run_ids: list[int]) -> BacktestDeleteResponse:
        self._recover_stale_runs()
        session = SessionLocal()
        deleted_run_ids: list[int] = []
        blocked_runs: list[BacktestDeleteBlockedItem] = []
        missing_run_ids: list[int] = []
        try:
            backtest_repository = BacktestRepository(session)
            for run_id in run_ids:
                row = backtest_repository.get_run_with_result(run_id)
                if row is None:
                    missing_run_ids.append(run_id)
                    continue

                run, _strategy, result = row
                if run.status in {BacktestStatus.RUNNING, BacktestStatus.QUEUED}:
                    blocked_runs.append(
                        BacktestDeleteBlockedItem(
                            run_id=run_id,
                            reason="active_run_stop_first",
                        )
                    )
                    continue

                backtest_repository.delete_run(run, result=result)
                deleted_run_ids.append(run_id)

            session.commit()
            logger.info(
                "Backtest delete request processed",
                extra={
                    "deleted_run_ids": deleted_run_ids,
                    "blocked_runs": [item.model_dump(mode="json") for item in blocked_runs],
                    "missing_run_ids": missing_run_ids,
                },
            )
            return BacktestDeleteResponse(
                deleted_run_ids=deleted_run_ids,
                blocked_runs=blocked_runs,
                missing_run_ids=missing_run_ids,
            )
        except Exception:
            session.rollback()
            logger.exception(
                "Failed to delete backtests",
                extra={"requested_run_ids": run_ids},
            )
            raise
        finally:
            session.close()

    def _recover_stale_runs(self) -> None:
        session = SessionLocal()
        try:
            backtest_repository = BacktestRepository(session)
            stale_before = utc_now() - timedelta(seconds=self.backtest_stale_after_seconds)
            recovered = backtest_repository.recover_stale_runs(stale_before=stale_before)
            if not recovered:
                return

            session.commit()
            logger.warning(
                "Recovered stale backtest runs",
                extra={
                    "stale_before": stale_before.isoformat(),
                    "recovered_runs": recovered,
                },
            )
        except Exception:
            session.rollback()
            logger.exception("Failed to recover stale backtest runs")
        finally:
            session.close()

    def _mark_run_failed(self, run_id: int, error_text: str) -> None:
        session = SessionLocal()
        try:
            backtest_repository = BacktestRepository(session)
            run = backtest_repository.get_run(run_id)
            if run is None:
                logger.error("Unable to mark failed backtest run because it no longer exists", extra={"run_id": run_id})
                return
            if run.status not in {BacktestStatus.QUEUED, BacktestStatus.RUNNING}:
                return

            completed_at = utc_now()
            backtest_repository.mark_failed(run, completed_at=completed_at, error_text=error_text)
            session.commit()
            logger.info(
                "Backtest status marked failed",
                extra={
                    "run_id": run_id,
                    "completed_at": completed_at.isoformat(),
                    "error_text": error_text,
                },
            )
        except Exception:
            session.rollback()
            logger.exception("Failed to persist backtest failure state", extra={"run_id": run_id})
        finally:
            session.close()

    def _log_progress(
        self,
        run_id: int,
        processed_bars: int,
        total_bars: int,
        candle_time: datetime,
    ) -> None:
        logger.info(
            "Backtest loop progress",
            extra={
                "run_id": run_id,
                "processed_bars": processed_bars,
                "total_bars": total_bars,
                "progress_pct": round((processed_bars / total_bars) * 100, 2) if total_bars else 0,
                "last_candle_at": candle_time.isoformat(),
            },
        )

    def _should_abort_run(
        self,
        run_id: int,
        processed_bars: int,
        total_bars: int,
        candle_time: datetime,
    ) -> bool:
        session = SessionLocal()
        try:
            backtest_repository = BacktestRepository(session)
            run = backtest_repository.get_run(run_id)
            if run is None:
                logger.warning(
                    "Stopping backtest because run record is missing",
                    extra={
                        "run_id": run_id,
                        "processed_bars": processed_bars,
                        "total_bars": total_bars,
                        "last_candle_at": candle_time.isoformat(),
                    },
                )
                return True
            return run.status != BacktestStatus.RUNNING
        finally:
            session.close()

    def _load_backtest_response(self, run_id: int) -> BacktestResponse:
        session = SessionLocal()
        try:
            query_service = QueryService(session)
            return query_service.get_backtest(run_id)
        finally:
            session.close()
