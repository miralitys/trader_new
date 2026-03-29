from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import select

from app.models import ValidationRun
from app.models.enums import SyncJobStatus
from app.repositories.base import BaseRepository


class ValidationRunRepository(BaseRepository):
    def create_run(
        self,
        *,
        exchange: str,
        symbols: list[str],
        timeframes: list[str],
        lookback_days: int,
        sample_limit: int,
        perform_resync: bool,
        resync_days: int,
    ) -> ValidationRun:
        run = ValidationRun(
            exchange=exchange,
            symbols_json={"symbols": symbols},
            timeframes_json={"timeframes": timeframes},
            lookback_days=lookback_days,
            sample_limit=sample_limit,
            perform_resync=perform_resync,
            resync_days=resync_days,
            status=SyncJobStatus.QUEUED,
            progress_json={},
            report_summary_json={},
            report_json={},
        )
        self.session.add(run)
        self.session.flush()
        return run

    def get_by_id(self, run_id: int) -> Optional[ValidationRun]:
        return self.session.scalar(select(ValidationRun).where(ValidationRun.id == run_id))

    def list_runs(self, limit: int = 50) -> list[ValidationRun]:
        stmt = select(ValidationRun).order_by(ValidationRun.updated_at.desc(), ValidationRun.id.desc()).limit(limit)
        return list(self.session.scalars(stmt))

    def get_next_queued_run(self) -> Optional[ValidationRun]:
        stmt = (
            select(ValidationRun)
            .where(ValidationRun.status == SyncJobStatus.QUEUED)
            .order_by(ValidationRun.created_at.asc(), ValidationRun.id.asc())
            .limit(1)
        )
        return self.session.scalar(stmt)

    def list_stale_running_runs(self, *, stale_before: datetime) -> list[ValidationRun]:
        stmt = (
            select(ValidationRun)
            .where(
                ValidationRun.status == SyncJobStatus.RUNNING,
                ValidationRun.updated_at < stale_before,
            )
            .order_by(ValidationRun.updated_at.asc(), ValidationRun.id.asc())
        )
        return list(self.session.scalars(stmt))

    def mark_running(self, run: ValidationRun, started_at: datetime) -> ValidationRun:
        run.status = SyncJobStatus.RUNNING
        run.started_at = started_at
        run.completed_at = None
        run.error_text = None
        self.session.add(run)
        self.session.flush()
        return run

    def update_progress(self, run: ValidationRun, *, progress_json: dict) -> ValidationRun:
        run.progress_json = progress_json
        self.session.add(run)
        self.session.flush()
        return run

    def mark_completed(
        self,
        run: ValidationRun,
        *,
        completed_at: datetime,
        report_summary_json: dict,
        report_json: dict,
    ) -> ValidationRun:
        run.status = SyncJobStatus.COMPLETED
        run.completed_at = completed_at
        run.error_text = None
        run.progress_json = {
            "phase": "completed",
            "processed_series": report_summary_json.get("overview", {}).get("total_series", 0),
            "total_series": report_summary_json.get("overview", {}).get("total_series", 0),
            "percent_complete": 100.0,
            "current_symbol": None,
            "current_timeframe": None,
        }
        run.report_summary_json = report_summary_json
        run.report_json = report_json
        self.session.add(run)
        self.session.flush()
        return run

    def mark_failed(self, run: ValidationRun, *, completed_at: datetime, error_text: str) -> ValidationRun:
        run.status = SyncJobStatus.FAILED
        run.completed_at = completed_at
        run.error_text = error_text
        progress = dict(run.progress_json or {})
        progress["phase"] = "failed"
        run.progress_json = progress
        self.session.add(run)
        self.session.flush()
        return run
