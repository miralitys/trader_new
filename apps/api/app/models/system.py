from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import DateTime, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import AppModel, CreatedAtMixin, TimestampMixin
from app.models.enums import AppLogLevel, SyncJobStatus, pg_enum


class SyncJob(AppModel, TimestampMixin):
    __tablename__ = "sync_jobs"

    exchange: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    timeframe: Mapped[str] = mapped_column(Text, nullable=False)
    start_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    end_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[SyncJobStatus] = mapped_column(
        pg_enum(SyncJobStatus, "sync_job_status_enum"),
        nullable=False,
        default=SyncJobStatus.QUEUED,
    )
    rows_inserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class FeatureRun(AppModel, TimestampMixin):
    __tablename__ = "feature_runs"

    exchange: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    timeframe: Mapped[str] = mapped_column(Text, nullable=False)
    lookback_days: Mapped[int] = mapped_column(Integer, nullable=False, default=730)
    start_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    end_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[SyncJobStatus] = mapped_column(
        pg_enum(SyncJobStatus, "sync_job_status_enum"),
        nullable=False,
        default=SyncJobStatus.QUEUED,
    )
    source_candle_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    feature_rows_upserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    computed_start_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    computed_end_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    error_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class ValidationRun(AppModel, TimestampMixin):
    __tablename__ = "validation_runs"

    exchange: Mapped[str] = mapped_column(Text, nullable=False)
    symbols_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
    timeframes_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
    lookback_days: Mapped[int] = mapped_column(Integer, nullable=False, default=730)
    sample_limit: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    perform_resync: Mapped[bool] = mapped_column(nullable=False, default=False)
    resync_days: Mapped[int] = mapped_column(Integer, nullable=False, default=14)
    status: Mapped[SyncJobStatus] = mapped_column(
        pg_enum(SyncJobStatus, "sync_job_status_enum"),
        nullable=False,
        default=SyncJobStatus.QUEUED,
    )
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    error_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    progress_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
    report_summary_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
    report_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)


class AppLog(AppModel, CreatedAtMixin):
    __tablename__ = "app_logs"

    scope: Mapped[str] = mapped_column(Text, nullable=False)
    level: Mapped[AppLogLevel] = mapped_column(
        pg_enum(AppLogLevel, "app_log_level_enum"),
        nullable=False,
        default=AppLogLevel.INFO,
    )
    message: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
