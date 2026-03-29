from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.db.session import session_scope
from app.repositories.pattern_scan_run_repository import PatternScanRunRepository
from app.schemas.research import (
    PatternScanProgressResponse,
    PatternScanRequest,
    PatternScanRunResponse,
    ResearchSummaryResponse,
)
from app.services.pattern_research_service import PatternResearchService
from app.utils.time import utc_now

logger = get_logger(__name__)


class PatternScanRunService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.repository = PatternScanRunRepository(session)

    def create_run(self, request: PatternScanRequest) -> PatternScanRunResponse:
        run = self.repository.create_run(
            exchange=request.exchange_code,
            symbols=request.symbols,
            timeframes=request.timeframes,
            lookback_days=request.lookback_days,
            forward_bars=request.forward_bars,
            fee_pct=str(request.fee_pct),
            slippage_pct=str(request.slippage_pct),
            max_bars_per_series=request.max_bars_per_series,
        )
        self.session.commit()
        return self._to_response(run)

    def list_runs(self, limit: int = 20) -> list[PatternScanRunResponse]:
        return [self._to_response(run) for run in self.repository.list_runs(limit=limit)]

    def get_run(self, run_id: int) -> PatternScanRunResponse | None:
        run = self.repository.get_by_id(run_id)
        if run is None:
            return None
        return self._to_response(run)

    def execute_run(self, run_id: int) -> bool:
        with session_scope() as session:
            repository = PatternScanRunRepository(session)
            run = repository.get_by_id(run_id)
            if run is None:
                return False
            if run.status.value == "completed":
                return False

            repository.mark_running(run, started_at=utc_now())
            repository.update_progress(
                run,
                progress_json={
                    "phase": "running",
                    "processed_series": 0,
                    "total_series": len(run.symbols_json.get("symbols", [])) * len(run.timeframes_json.get("timeframes", [])),
                    "percent_complete": 0.0,
                    "current_symbol": None,
                    "current_timeframe": None,
                },
            )
            logger.info("Pattern scan run started", extra={"run_id": run_id})

        try:
            with session_scope() as session:
                repository = PatternScanRunRepository(session)
                run = repository.get_by_id(run_id)
                if run is None:
                    return False

                service = PatternResearchService(session=session)
                report = service.get_summary(
                    exchange_code=run.exchange,
                    symbols=list(run.symbols_json.get("symbols", [])),
                    timeframes=list(run.timeframes_json.get("timeframes", [])),
                    lookback_days=run.lookback_days,
                    forward_bars=run.forward_bars,
                    fee_pct=Decimal(run.fee_pct),
                    slippage_pct=Decimal(run.slippage_pct),
                    max_bars_per_series=run.max_bars_per_series,
                    progress_callback=lambda symbol, timeframe, processed, total: self._persist_progress(
                        run_id=run_id,
                        symbol=symbol,
                        timeframe=timeframe,
                        processed=processed,
                        total=total,
                    ),
                )
                payload = jsonable_encoder(report)
                repository.mark_completed(
                    run,
                    completed_at=utc_now(),
                    report_summary_json=payload,
                    report_json=payload,
                )
                logger.info("Pattern scan run completed", extra={"run_id": run_id})
                return True
        except Exception as exc:
            with session_scope() as session:
                repository = PatternScanRunRepository(session)
                run = repository.get_by_id(run_id)
                if run is None:
                    return False
                repository.mark_failed(run, completed_at=utc_now(), error_text=str(exc))
                logger.exception("Pattern scan run failed", extra={"run_id": run_id})
            return False

    def process_next_queued_run(self) -> bool:
        with session_scope() as session:
            repository = PatternScanRunRepository(session)
            run = repository.get_next_queued_run()
            run_id = run.id if run is not None else None

        if run_id is None:
            return False

        return self.execute_run(run_id)

    def mark_stale_running_runs(self, *, stale_after_seconds: int) -> int:
        if stale_after_seconds <= 0:
            return 0

        stale_before = utc_now().replace(microsecond=0) - timedelta(seconds=stale_after_seconds)

        with session_scope() as session:
            repository = PatternScanRunRepository(session)
            stale_runs = repository.list_stale_running_runs(stale_before=stale_before)
            for run in stale_runs:
                progress = run.progress_json or {}
                current_symbol = progress.get("current_symbol")
                current_timeframe = progress.get("current_timeframe")
                stuck_on = None
                if current_symbol or current_timeframe:
                    stuck_on = f"{current_symbol or '—'} · {current_timeframe or '—'}"
                repository.mark_failed(
                    run,
                    completed_at=utc_now(),
                    error_text=(
                        "Pattern scan run became stale before completion. "
                        + (f"Last active series: {stuck_on}. " if stuck_on else "")
                        + "The worker likely stopped, the instance restarted, or the current series stalled."
                    ),
                )
            if stale_runs:
                logger.warning(
                    "Marked stale pattern scan runs as failed",
                    extra={"run_ids": [run.id for run in stale_runs], "count": len(stale_runs)},
                )
            return len(stale_runs)

    def _persist_progress(self, *, run_id: int, symbol: str, timeframe: str, processed: int, total: int) -> None:
        with session_scope() as session:
            repository = PatternScanRunRepository(session)
            run = repository.get_by_id(run_id)
            if run is None:
                return

            percent_complete = 0.0
            if total > 0:
                percent_complete = round((processed / total) * 100, 2)

            repository.update_progress(
                run,
                progress_json={
                    "phase": "running",
                    "processed_series": processed,
                    "total_series": total,
                    "percent_complete": percent_complete,
                    "current_symbol": symbol,
                    "current_timeframe": timeframe,
                },
            )

    def _to_response(self, run) -> PatternScanRunResponse:
        progress = PatternScanProgressResponse(**run.progress_json) if run.progress_json else None
        report_summary = ResearchSummaryResponse(**run.report_summary_json) if run.report_summary_json else None
        report = ResearchSummaryResponse(**run.report_json) if run.report_json else None
        return PatternScanRunResponse(
            id=run.id,
            exchange=run.exchange,
            symbols=list(run.symbols_json.get("symbols", [])),
            timeframes=list(run.timeframes_json.get("timeframes", [])),
            lookback_days=run.lookback_days,
            forward_bars=run.forward_bars,
            fee_pct=Decimal(run.fee_pct),
            slippage_pct=Decimal(run.slippage_pct),
            max_bars_per_series=run.max_bars_per_series,
            status=run.status.value,
            started_at=run.started_at,
            completed_at=run.completed_at,
            error_text=run.error_text,
            progress=progress,
            report_summary=report_summary,
            report=report,
            created_at=run.created_at,
            updated_at=run.updated_at,
        )
