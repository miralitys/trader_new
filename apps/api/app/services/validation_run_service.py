from __future__ import annotations

from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.db.session import session_scope
from app.repositories.validation_run_repository import ValidationRunRepository
from app.schemas.api import (
    DataValidationReportResponse,
    DataValidationRequest,
    ValidationRunProgressResponse,
    DataValidationSummaryResponse,
    ValidationRunResponse,
)
from app.services.data_validation_service import DataValidationService, build_validation_report_payload
from app.utils.time import utc_now

logger = get_logger(__name__)


class ValidationRunService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.repository = ValidationRunRepository(session)

    def create_run(self, request: DataValidationRequest) -> ValidationRunResponse:
        run = self.repository.create_run(
            exchange=request.exchange_code,
            symbols=request.symbols,
            timeframes=request.timeframes,
            lookback_days=request.lookback_days,
            sample_limit=request.sample_limit,
            perform_resync=request.perform_resync,
            resync_days=request.resync_days,
        )
        self.session.commit()
        return self._to_response(run)

    def list_runs(self, limit: int = 50) -> list[ValidationRunResponse]:
        return [self._to_response(run) for run in self.repository.list_runs(limit=limit)]

    def get_run(self, run_id: int) -> ValidationRunResponse | None:
        run = self.repository.get_by_id(run_id)
        if run is None:
            return None
        return self._to_response(run)

    def run_in_background(self, run_id: int) -> None:
        self.execute_run(run_id)

    def execute_run(self, run_id: int) -> bool:
        with session_scope() as session:
            repository = ValidationRunRepository(session)
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
            logger.info("Validation run started", extra={"run_id": run_id})

        try:
            with session_scope() as session:
                repository = ValidationRunRepository(session)
                run = repository.get_by_id(run_id)
                if run is None:
                    return False

                validation_service = DataValidationService(session=session)
                try:
                    report = validation_service.validate(
                        exchange_code=run.exchange,
                        symbols=list(run.symbols_json.get("symbols", [])),
                        timeframes=list(run.timeframes_json.get("timeframes", [])),
                        lookback_days=run.lookback_days,
                        perform_resync=run.perform_resync,
                        resync_days=run.resync_days,
                        sample_limit=run.sample_limit,
                        progress_callback=lambda symbol, timeframe, processed, total: self._persist_progress(
                            run_id=run_id,
                            symbol=symbol,
                            timeframe=timeframe,
                            processed=processed,
                            total=total,
                        ),
                    )
                finally:
                    validation_service.close()

                payload = build_validation_report_payload(report)
                response_payload = DataValidationReportResponse(
                    summary=DataValidationSummaryResponse(**payload["summary"]),
                    results=[],
                )
                repository.mark_completed(
                    run,
                    completed_at=utc_now(),
                    report_summary_json=jsonable_encoder(response_payload.summary),
                    report_json=jsonable_encoder(
                        DataValidationReportResponse(
                            summary=DataValidationSummaryResponse(**payload["summary"]),
                            results=payload["results"],
                        )
                    ),
                )
                logger.info("Validation run completed", extra={"run_id": run_id})
                return True
        except Exception as exc:
            with session_scope() as session:
                repository = ValidationRunRepository(session)
                run = repository.get_by_id(run_id)
                if run is None:
                    return False
                repository.mark_failed(run, completed_at=utc_now(), error_text=str(exc))
                logger.exception("Validation run failed", extra={"run_id": run_id})
            return False

    def process_next_queued_run(self) -> bool:
        with session_scope() as session:
            repository = ValidationRunRepository(session)
            run = repository.get_next_queued_run()
            run_id = run.id if run is not None else None

        if run_id is None:
            return False

        return self.execute_run(run_id)

    def mark_stale_running_runs(self, *, stale_after_seconds: int) -> int:
        if stale_after_seconds <= 0:
            return 0

        stale_before = utc_now()
        stale_before = stale_before.replace(microsecond=0)
        from datetime import timedelta

        stale_before = stale_before - timedelta(seconds=stale_after_seconds)

        with session_scope() as session:
            repository = ValidationRunRepository(session)
            stale_runs = repository.list_stale_running_runs(stale_before=stale_before)
            for run in stale_runs:
                repository.mark_failed(
                    run,
                    completed_at=utc_now(),
                    error_text="Validation run became stale before completion. The background process likely stopped or the instance restarted.",
                )
            if stale_runs:
                logger.warning(
                    "Marked stale validation runs as failed",
                    extra={"run_ids": [run.id for run in stale_runs], "count": len(stale_runs)},
                )
            return len(stale_runs)

    def _to_response(self, run) -> ValidationRunResponse:
        progress = None
        if run.progress_json:
            progress = ValidationRunProgressResponse(**run.progress_json)

        report_summary = None
        if run.report_summary_json:
            report_summary = DataValidationSummaryResponse(**run.report_summary_json)

        report = None
        if run.report_json and "summary" in run.report_json:
            report = DataValidationReportResponse(**run.report_json)

        return ValidationRunResponse(
            id=run.id,
            exchange=run.exchange,
            symbols=list(run.symbols_json.get("symbols", [])),
            timeframes=list(run.timeframes_json.get("timeframes", [])),
            lookback_days=run.lookback_days,
            sample_limit=run.sample_limit,
            perform_resync=run.perform_resync,
            resync_days=run.resync_days,
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

    def _persist_progress(self, *, run_id: int, symbol: str, timeframe: str, processed: int, total: int) -> None:
        with session_scope() as session:
            repository = ValidationRunRepository(session)
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
