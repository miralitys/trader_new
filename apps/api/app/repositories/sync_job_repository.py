from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import select

from app.models import SyncJob
from app.models.enums import SyncJobStatus
from app.repositories.base import BaseRepository


class SyncJobRepository(BaseRepository):
    def create_job(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> SyncJob:
        job = SyncJob(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
            status=SyncJobStatus.QUEUED,
            rows_inserted=0,
        )
        self.session.add(job)
        self.session.flush()
        return job

    def get_by_id(self, job_id: int) -> Optional[SyncJob]:
        return self.session.scalar(select(SyncJob).where(SyncJob.id == job_id))

    def list_jobs(
        self,
        limit: int = 100,
        status: Optional[SyncJobStatus] = None,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
    ) -> list[SyncJob]:
        stmt = select(SyncJob).order_by(SyncJob.updated_at.desc(), SyncJob.id.desc()).limit(limit)
        if status is not None:
            stmt = stmt.where(SyncJob.status == status)
        if symbol:
            stmt = stmt.where(SyncJob.symbol == symbol)
        if timeframe:
            stmt = stmt.where(SyncJob.timeframe == timeframe)
        return list(self.session.scalars(stmt))

    def mark_running(self, job: SyncJob) -> SyncJob:
        job.status = SyncJobStatus.RUNNING
        self.session.add(job)
        self.session.flush()
        return job

    def mark_completed(self, job: SyncJob, rows_inserted: int) -> SyncJob:
        job.status = SyncJobStatus.COMPLETED
        job.rows_inserted = rows_inserted
        job.error_text = None
        self.session.add(job)
        self.session.flush()
        return job

    def mark_failed(self, job: SyncJob, error_text: str, rows_inserted: int) -> SyncJob:
        job.status = SyncJobStatus.FAILED
        job.rows_inserted = rows_inserted
        job.error_text = error_text
        self.session.add(job)
        self.session.flush()
        return job
