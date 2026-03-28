from __future__ import annotations

from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from app.db.session import session_scope
from app.repositories.validation_run_repository import ValidationRunRepository
from app.schemas.api import (
    DataValidationReportResponse,
    DataValidationRequest,
    DataValidationSummaryResponse,
    ValidationRunResponse,
)
from app.services.data_validation_service import DataValidationService, build_validation_report_payload
from app.utils.time import utc_now


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
        with session_scope() as session:
            repository = ValidationRunRepository(session)
            run = repository.get_by_id(run_id)
            if run is None:
                return

            repository.mark_running(run, started_at=utc_now())

        try:
            with session_scope() as session:
                repository = ValidationRunRepository(session)
                run = repository.get_by_id(run_id)
                if run is None:
                    return

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
        except Exception as exc:
            with session_scope() as session:
                repository = ValidationRunRepository(session)
                run = repository.get_by_id(run_id)
                if run is None:
                    return
                repository.mark_failed(run, completed_at=utc_now(), error_text=str(exc))

    def _to_response(self, run) -> ValidationRunResponse:
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
            report_summary=report_summary,
            report=report,
            created_at=run.created_at,
            updated_at=run.updated_at,
        )
