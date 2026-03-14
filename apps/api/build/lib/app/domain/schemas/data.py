from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from app.domain.schemas.common import ORMModel


class DataSyncRequest(BaseModel):
    symbols: list[str]
    timeframes: list[str]
    start: datetime | None = None
    end: datetime | None = None
    full_resync: bool = False


class SyncJobRead(ORMModel):
    id: int
    exchange_id: int
    symbol_id: int
    timeframe_id: int
    status: str
    job_type: str
    requested_start: datetime | None = None
    requested_end: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_message: str | None = None
    metadata_json: dict


class DataCoverageRead(BaseModel):
    symbol: str
    timeframe: str
    first_candle: datetime | None = None
    last_candle: datetime | None = None
    candle_count: int


class DataStatusRead(BaseModel):
    loader_status: str
    queue_depth: int
    last_sync_at: datetime | None = None
    coverage: list[DataCoverageRead]
    jobs: list[SyncJobRead]
