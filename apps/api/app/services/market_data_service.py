from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from app.core.config import Settings, get_settings
from app.core.logging import get_logger
from app.db.session import SessionLocal
from app.integrations.coinbase import CoinbaseClient, CoinbaseTimeframe, normalize_coinbase_candles
from app.repositories.candle_repository import CandleRepository
from app.repositories.sync_job_repository import SyncJobRepository
from app.utils.time import ensure_utc, utc_now

logger = get_logger(__name__)


@dataclass(frozen=True)
class MarketDataSyncResult:
    job_id: int
    exchange: str
    symbol: str
    timeframe: str
    start_at: datetime
    end_at: datetime
    fetched_rows: int
    normalized_rows: int
    inserted_rows: int
    status: str


class MarketDataService:
    def __init__(
        self,
        settings: Optional[Settings] = None,
        client: Optional[CoinbaseClient] = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.client = client or CoinbaseClient(settings=self.settings)

    def close(self) -> None:
        self.client.close()

    def initial_historical_sync(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> MarketDataSyncResult:
        return self._sync_range(
            symbol=symbol,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
            sync_reason="initial",
        )

    def manual_sync(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> MarketDataSyncResult:
        return self._sync_range(
            symbol=symbol,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
            sync_reason="manual",
        )

    def incremental_sync(
        self,
        symbol: str,
        timeframe: str,
        end_at: Optional[datetime] = None,
    ) -> MarketDataSyncResult:
        timeframe_value = CoinbaseTimeframe.from_code(timeframe)
        normalized_end = ensure_utc(end_at or utc_now())

        session = SessionLocal()
        try:
            candle_repository = CandleRepository(session)
            exchange = candle_repository.ensure_exchange("coinbase", name="Coinbase")
            candle_repository.ensure_timeframe(timeframe_value.value)
            symbol_row = candle_repository.ensure_symbol(exchange.id, symbol)
            session.commit()

            last_candle_open_time = candle_repository.get_last_candle_open_time(
                exchange_id=exchange.id,
                symbol_id=symbol_row.id,
                timeframe=timeframe_value.value,
            )
        finally:
            session.close()

        if last_candle_open_time is None:
            start_at = normalized_end - (
                timeframe_value.interval * self.settings.coinbase_max_candles_per_request
            )
        else:
            start_at = last_candle_open_time - (
                timeframe_value.interval * self.settings.market_data_incremental_overlap_candles
            )

        return self._sync_range(
            symbol=symbol,
            timeframe=timeframe_value.value,
            start_at=start_at,
            end_at=normalized_end,
            sync_reason="incremental",
        )

    def _sync_range(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
        sync_reason: str,
    ) -> MarketDataSyncResult:
        timeframe_value = CoinbaseTimeframe.from_code(timeframe)
        normalized_start = ensure_utc(start_at)
        normalized_end = ensure_utc(end_at)

        session = SessionLocal()
        sync_job = None
        total_fetched = 0
        total_normalized = 0
        total_inserted = 0

        try:
            candle_repository = CandleRepository(session)
            sync_job_repository = SyncJobRepository(session)

            exchange = candle_repository.ensure_exchange("coinbase", name="Coinbase")
            candle_repository.ensure_timeframe(timeframe_value.value)
            symbol_row = candle_repository.ensure_symbol(exchange.id, symbol)

            sync_job = sync_job_repository.create_job(
                exchange=exchange.code,
                symbol=symbol,
                timeframe=timeframe_value.value,
                start_at=normalized_start,
                end_at=normalized_end,
            )
            session.commit()

            sync_job_repository.mark_running(sync_job)
            session.commit()

            for raw_chunk in self.client.iter_historical_candles(
                symbol=symbol,
                timeframe=timeframe_value.value,
                start_at=normalized_start,
                end_at=normalized_end,
            ):
                total_fetched += len(raw_chunk)
                normalized_chunk = normalize_coinbase_candles(raw_chunk, timeframe_value)
                total_normalized += len(normalized_chunk)
                inserted_rows = candle_repository.upsert_candles(
                    exchange_id=exchange.id,
                    symbol_id=symbol_row.id,
                    timeframe=timeframe_value.value,
                    candles=normalized_chunk,
                )
                total_inserted += inserted_rows
                session.commit()

            sync_job_repository.mark_completed(sync_job, rows_inserted=total_inserted)
            session.commit()

            logger.info(
                "Market data sync completed",
                extra={
                    "job_id": sync_job.id,
                    "exchange": exchange.code,
                    "symbol": symbol,
                    "timeframe": timeframe_value.value,
                    "sync_reason": sync_reason,
                    "fetched_rows": total_fetched,
                    "normalized_rows": total_normalized,
                    "inserted_rows": total_inserted,
                },
            )

            return MarketDataSyncResult(
                job_id=sync_job.id,
                exchange=exchange.code,
                symbol=symbol,
                timeframe=timeframe_value.value,
                start_at=normalized_start,
                end_at=normalized_end,
                fetched_rows=total_fetched,
                normalized_rows=total_normalized,
                inserted_rows=total_inserted,
                status=sync_job.status.value,
            )
        except Exception as exc:
            session.rollback()
            logger.exception(
                "Market data sync failed",
                extra={
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "sync_reason": sync_reason,
                },
            )

            if sync_job is not None:
                try:
                    sync_job_repository = SyncJobRepository(session)
                    persisted_job = sync_job_repository.get_by_id(sync_job.id) or sync_job
                    sync_job_repository.mark_failed(
                        persisted_job,
                        error_text=str(exc),
                        rows_inserted=total_inserted,
                    )
                    session.commit()
                except Exception:
                    session.rollback()
                    logger.exception("Failed to record sync job failure state", extra={"job_id": sync_job.id})

            raise
        finally:
            session.close()
