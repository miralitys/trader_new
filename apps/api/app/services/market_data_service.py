from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from app.core.config import Settings, get_settings
from app.core.logging import get_logger
from app.integrations.binance_us.client import BinanceUSClientError
from app.integrations.binance_us import BinanceUSClient, BinanceUSTimeframe, normalize_binance_us_candles
from app.db.session import SessionLocal
from app.repositories.candle_repository import CandleCoverageSummary, CandleRepository
from app.repositories.sync_job_repository import SyncJobRepository
from app.utils.exchanges import normalize_exchange_code
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
    coverage: Optional[CandleCoverageSummary] = None


class MarketDataService:
    def __init__(
        self,
        settings: Optional[Settings] = None,
        binance_us_client: Optional[BinanceUSClient] = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.binance_us_client = binance_us_client or BinanceUSClient(settings=self.settings)

    def close(self) -> None:
        self.binance_us_client.close()

    def initial_historical_sync(
        self,
        exchange_code: str,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> MarketDataSyncResult:
        exchange_code = normalize_exchange_code(exchange_code)
        return self._sync_range(
            exchange_code=exchange_code,
            symbol=symbol,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
            sync_reason="initial",
        )

    def manual_sync(
        self,
        exchange_code: str,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> MarketDataSyncResult:
        exchange_code = normalize_exchange_code(exchange_code)
        return self._sync_range(
            exchange_code=exchange_code,
            symbol=symbol,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
            sync_reason="manual",
        )

    def incremental_sync(
        self,
        exchange_code: str,
        symbol: str,
        timeframe: str,
        end_at: Optional[datetime] = None,
    ) -> MarketDataSyncResult:
        exchange_code = normalize_exchange_code(exchange_code)
        timeframe_value = self._timeframe_for_exchange(exchange_code, timeframe)
        normalized_end = ensure_utc(end_at or utc_now())

        session = SessionLocal()
        try:
            candle_repository = CandleRepository(session)
            exchange = candle_repository.ensure_exchange(exchange_code, name=self._exchange_name(exchange_code))
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
                timeframe_value.interval * self._max_candles_per_request(exchange_code)
            )
        else:
            start_at = last_candle_open_time - (
                timeframe_value.interval * self.settings.market_data_incremental_overlap_candles
            )

        return self._sync_range(
            exchange_code=exchange_code,
            symbol=symbol,
            timeframe=timeframe_value.value,
            start_at=start_at,
            end_at=normalized_end,
            sync_reason="incremental",
        )

    def _sync_range(
        self,
        exchange_code: str,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
        sync_reason: str,
    ) -> MarketDataSyncResult:
        exchange_code = normalize_exchange_code(exchange_code)
        timeframe_value = self._timeframe_for_exchange(exchange_code, timeframe)
        normalized_start = ensure_utc(start_at)
        normalized_end = ensure_utc(end_at)

        session = SessionLocal()
        sync_job = None
        total_fetched = 0
        total_normalized = 0
        total_inserted = 0
        coverage: Optional[CandleCoverageSummary] = None

        try:
            candle_repository = CandleRepository(session)
            sync_job_repository = SyncJobRepository(session)

            exchange = candle_repository.ensure_exchange(exchange_code, name=self._exchange_name(exchange_code))
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

            client = self._client_for_exchange(exchange_code)
            for raw_chunk in client.iter_historical_candles(
                symbol=symbol,
                timeframe=timeframe_value.value,
                start_at=normalized_start,
                end_at=normalized_end,
            ):
                total_fetched += len(raw_chunk)
                normalized_chunk = self._normalize_chunk(exchange_code, raw_chunk, timeframe_value.value)
                total_normalized += len(normalized_chunk)
                inserted_rows = candle_repository.upsert_candles(
                    exchange_id=exchange.id,
                    symbol_id=symbol_row.id,
                    timeframe=timeframe_value.value,
                    candles=normalized_chunk,
                )
                total_inserted += inserted_rows
                session.commit()

            coverage = candle_repository.get_candle_coverage(
                exchange_code=exchange.code,
                symbol_code=symbol,
                timeframe=timeframe_value.value,
                start_at=normalized_start,
                end_at=normalized_end,
            )
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
                    "exchange_code": exchange_code,
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
                coverage=coverage,
            )
        except Exception as exc:
            session.rollback()
            logger.exception(
                "Market data sync failed",
                extra={
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "sync_reason": sync_reason,
                    "exchange_code": exchange_code,
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

            if isinstance(exc, BinanceUSClientError):
                raise ValueError(str(exc)) from exc

            raise
        finally:
            session.close()

    def _client_for_exchange(self, exchange_code: str) -> BinanceUSClient:
        if exchange_code == "binance_us":
            return self.binance_us_client
        raise ValueError(f"Unsupported exchange: {exchange_code}")

    def _timeframe_for_exchange(self, exchange_code: str, timeframe: str) -> BinanceUSTimeframe:
        if exchange_code == "binance_us":
            return BinanceUSTimeframe.from_code(timeframe)
        raise ValueError(f"Unsupported exchange: {exchange_code}")

    def _normalize_chunk(
        self,
        exchange_code: str,
        raw_chunk: list[list[object]],
        timeframe: str,
    ):
        if exchange_code == "binance_us":
            return normalize_binance_us_candles(raw_chunk, BinanceUSTimeframe.from_code(timeframe))
        raise ValueError(f"Unsupported exchange: {exchange_code}")

    def _exchange_name(self, exchange_code: str) -> str:
        mapping = {"binance_us": "Binance.US"}
        try:
            return mapping[exchange_code]
        except KeyError as exc:
            raise ValueError(f"Unsupported exchange: {exchange_code}") from exc

    def _max_candles_per_request(self, exchange_code: str) -> int:
        mapping = {
            "binance_us": self.settings.binance_us_max_candles_per_request,
        }
        try:
            return mapping[exchange_code]
        except KeyError as exc:
            raise ValueError(f"Unsupported exchange: {exchange_code}") from exc
