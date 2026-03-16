from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

import httpx

from app.core.config import Settings, get_settings
from app.db.session import session_scope
from app.integrations.binance_archive import BinanceArchiveClient
from app.integrations.binance_common import BinanceResearchTimeframe
from app.integrations.binance_futures import BinanceFuturesClient, BinanceFuturesClientError
from app.integrations.binance_spot import BinanceSpotClient, BinanceSpotClientError
from app.integrations.okx_perp import OkxPerpClient, OkxPerpClientError
from app.repositories.funding_basis_repository import (
    FeeScheduleRow,
    FundingBasisRepository,
    FundingRateRow,
    PerpPriceRow,
    SpotPriceRow,
)
from app.schemas.funding_basis import FundingBasisBatchIngestionResult, FundingBasisIngestionResult
from app.utils.research_symbols import normalize_research_symbol
from app.utils.time import ensure_utc, utc_now


class FundingBasisIngestionService:
    SUPPORTED_PERP_VENUES = {"binance_futures", "okx_swap"}

    def __init__(
        self,
        settings: Optional[Settings] = None,
        spot_client: Optional[BinanceSpotClient] = None,
        futures_client: Optional[BinanceFuturesClient] = None,
        okx_client: Optional[OkxPerpClient] = None,
        archive_client: Optional[BinanceArchiveClient] = None,
        prefer_archive: Optional[bool] = None,
        perp_exchange: str = "binance_futures",
    ) -> None:
        self.settings = settings or get_settings()
        self.spot_exchange = "binance_spot"
        if perp_exchange not in self.SUPPORTED_PERP_VENUES:
            raise ValueError(f"Unsupported perp exchange: {perp_exchange}")
        self.perp_exchange = perp_exchange
        self.spot_client = spot_client or BinanceSpotClient(settings=self.settings)
        self.futures_client = futures_client or BinanceFuturesClient(settings=self.settings)
        self.okx_client = okx_client or OkxPerpClient(settings=self.settings)
        self.archive_client = archive_client or BinanceArchiveClient(settings=self.settings)
        self.prefer_archive = (
            self.settings.funding_basis_archive_prefer_archive if prefer_archive is None else prefer_archive
        )

    def close(self) -> None:
        self.spot_client.close()
        self.futures_client.close()
        self.okx_client.close()
        self.archive_client.close()

    def ensure_default_fee_schedules(self) -> int:
        with session_scope() as session:
            repository = FundingBasisRepository(session)
            return repository.upsert_fee_schedules(
                [
                    FeeScheduleRow(
                        venue=self.spot_exchange,
                        product_type="spot",
                        maker_fee_pct=Decimal("0.001"),
                        taker_fee_pct=Decimal("0.001"),
                        effective_from=datetime(2020, 1, 1, tzinfo=timezone.utc),
                    ),
                    FeeScheduleRow(
                        venue=self.perp_exchange,
                        product_type="perp",
                        maker_fee_pct=Decimal("0.0002"),
                        taker_fee_pct=Decimal("0.0005"),
                        effective_from=datetime(2020, 1, 1, tzinfo=timezone.utc),
                    ),
                ]
            )

    def backfill(
        self,
        symbols: list[str],
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> FundingBasisBatchIngestionResult:
        normalized_symbols = [normalize_research_symbol(symbol) for symbol in symbols]
        self.ensure_default_fee_schedules()
        results = [
            self._ingest_symbol_range(
                symbol=symbol,
                timeframe=timeframe,
                start_at=ensure_utc(start_at),
                end_at=ensure_utc(end_at),
            )
            for symbol in normalized_symbols
        ]
        return FundingBasisBatchIngestionResult(
            mode="history",
            exchange_spot=self.spot_exchange,
            exchange_perp=self.perp_exchange,
            timeframe=timeframe,
            start_at=ensure_utc(start_at),
            end_at=ensure_utc(end_at),
            symbols=normalized_symbols,
            results=results,
        )

    def incremental(
        self,
        symbols: list[str],
        timeframe: str,
        end_at: Optional[datetime] = None,
    ) -> FundingBasisBatchIngestionResult:
        normalized_symbols = [normalize_research_symbol(symbol) for symbol in symbols]
        normalized_end = ensure_utc(end_at or utc_now())
        timeframe_value = BinanceResearchTimeframe.from_code(timeframe)
        self.ensure_default_fee_schedules()
        results: list[FundingBasisIngestionResult] = []

        for symbol in normalized_symbols:
            with session_scope() as session:
                repository = FundingBasisRepository(session)
                last_spot_ts = repository.get_last_spot_ts(self.spot_exchange, symbol)
                last_perp_ts = repository.get_last_perp_ts(self.perp_exchange, symbol)
                last_funding_ts = repository.get_last_funding_time(self.perp_exchange, symbol)

            default_start = normalized_end - timedelta(days=self.settings.funding_basis_default_backfill_days)
            overlap = timeframe_value.interval * self.settings.funding_basis_incremental_overlap_bars
            funding_overlap = timedelta(hours=8 * self.settings.funding_basis_incremental_funding_overlap_intervals)
            start_at = min(
                last_spot_ts - overlap if last_spot_ts is not None else default_start,
                last_perp_ts - overlap if last_perp_ts is not None else default_start,
                last_funding_ts - funding_overlap if last_funding_ts is not None else default_start,
            )
            results.append(
                self._ingest_symbol_range(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_at=start_at,
                    end_at=normalized_end,
                )
            )

        return FundingBasisBatchIngestionResult(
            mode="incremental",
            exchange_spot=self.spot_exchange,
            exchange_perp=self.perp_exchange,
            timeframe=timeframe,
            start_at=None,
            end_at=normalized_end,
            symbols=normalized_symbols,
            results=results,
        )

    def _ingest_symbol_range(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> FundingBasisIngestionResult:
        notes: list[str] = []
        spot_rows, spot_source, spot_notes = self._fetch_spot_rows(
            symbol=symbol,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
        )
        perp_rows, perp_source, perp_notes = self._fetch_perp_rows(
            symbol=symbol,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
        )
        funding_rows, funding_source, funding_notes = self._fetch_funding_rows(
            symbol=symbol,
            start_at=start_at,
            end_at=end_at,
        )
        notes.extend(spot_notes)
        notes.extend(perp_notes)
        notes.extend(funding_notes)

        with session_scope() as session:
            repository = FundingBasisRepository(session)
            return FundingBasisIngestionResult(
                symbol=symbol,
                timeframe=timeframe,
                spot_rows_inserted=repository.upsert_spot_prices(spot_rows),
                perp_rows_inserted=repository.upsert_perp_prices(perp_rows),
                funding_rows_inserted=repository.upsert_funding_rates(funding_rows),
                spot_source=spot_source,
                perp_source=perp_source,
                funding_source=funding_source,
                notes=notes,
            )

    def _fetch_spot_rows(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> tuple[list[SpotPriceRow], str, list[str]]:
        rows: dict[datetime, SpotPriceRow] = {}
        notes: list[str] = []
        source = "rest"
        iterator = None
        if self.prefer_archive:
            iterator = self.archive_client.iter_spot_klines(symbol=symbol, timeframe=timeframe, start_at=start_at, end_at=end_at)
            source = "archive"
        else:
            try:
                iterator = self.spot_client.iter_historical_klines(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_at=start_at,
                    end_at=end_at,
                )
            except (BinanceSpotClientError, httpx.HTTPError) as exc:
                if not self.settings.funding_basis_archive_fallback_enabled:
                    raise
                iterator = self.archive_client.iter_spot_klines(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_at=start_at,
                    end_at=end_at,
                )
                source = "archive"
                notes.append(f"spot_rest_failed_fallback_archive:{type(exc).__name__}")

        if iterator is not None:
            try:
                for chunk in iterator:
                    for row in chunk:
                        ts = datetime.fromtimestamp(int(str(row[0])) / 1000, tz=timezone.utc)
                        close = Decimal(str(row[4]))
                        volume = Decimal(str(row[5]))
                        rows[ts] = SpotPriceRow(
                            exchange=self.spot_exchange,
                            symbol=symbol,
                            ts=ts,
                            bid=None,
                            ask=None,
                            mid=close,
                            close=close,
                            volume=volume,
                        )
            except (BinanceSpotClientError, httpx.HTTPError) as exc:
                if source == "archive" or not self.settings.funding_basis_archive_fallback_enabled:
                    raise
                rows.clear()
                source = "archive"
                notes.append(f"spot_rest_failed_fallback_archive:{type(exc).__name__}")
                for chunk in self.archive_client.iter_spot_klines(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_at=start_at,
                    end_at=end_at,
                ):
                    for row in chunk:
                        ts = datetime.fromtimestamp(int(str(row[0])) / 1000, tz=timezone.utc)
                        close = Decimal(str(row[4]))
                        volume = Decimal(str(row[5]))
                        rows[ts] = SpotPriceRow(
                            exchange=self.spot_exchange,
                            symbol=symbol,
                            ts=ts,
                            bid=None,
                            ask=None,
                            mid=close,
                            close=close,
                            volume=volume,
                        )

        return [rows[key] for key in sorted(rows)], source, notes

    def _fetch_perp_rows(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> tuple[list[PerpPriceRow], str, list[str]]:
        if self.perp_exchange == "okx_swap":
            return self._fetch_okx_perp_rows(symbol=symbol, timeframe=timeframe, start_at=start_at, end_at=end_at)

        mark_rows: dict[datetime, Decimal] = {}
        index_rows: dict[datetime, Decimal] = {}
        volume_rows: dict[datetime, Decimal] = {}
        oi_rows: dict[datetime, Decimal] = {}
        notes: list[str] = []
        source = "rest"

        def load_from_rest() -> None:
            for chunk in self.futures_client.iter_mark_price_klines(
                symbol=symbol,
                timeframe=timeframe,
                start_at=start_at,
                end_at=end_at,
            ):
                for row in chunk:
                    ts = datetime.fromtimestamp(int(str(row[0])) / 1000, tz=timezone.utc)
                    mark_rows[ts] = Decimal(str(row[4]))

            for chunk in self.futures_client.iter_index_price_klines(
                symbol=symbol,
                timeframe=timeframe,
                start_at=start_at,
                end_at=end_at,
            ):
                for row in chunk:
                    ts = datetime.fromtimestamp(int(str(row[0])) / 1000, tz=timezone.utc)
                    index_rows[ts] = Decimal(str(row[4]))

            for chunk in self.futures_client.iter_trade_klines(
                symbol=symbol,
                timeframe=timeframe,
                start_at=start_at,
                end_at=end_at,
            ):
                for row in chunk:
                    ts = datetime.fromtimestamp(int(str(row[0])) / 1000, tz=timezone.utc)
                    volume_rows[ts] = Decimal(str(row[5]))

            for chunk in self.futures_client.iter_open_interest_hist(
                symbol=symbol,
                timeframe=timeframe,
                start_at=start_at,
                end_at=end_at,
            ):
                for row in chunk:
                    ts = datetime.fromtimestamp(int(str(row["timestamp"])) / 1000, tz=timezone.utc)
                    oi_rows[ts] = Decimal(str(row["sumOpenInterest"]))

        def load_from_archive() -> None:
            for chunk in self.archive_client.iter_mark_price_klines(
                symbol=symbol,
                timeframe=timeframe,
                start_at=start_at,
                end_at=end_at,
            ):
                for row in chunk:
                    ts = datetime.fromtimestamp(int(str(row[0])) / 1000, tz=timezone.utc)
                    mark_rows[ts] = Decimal(str(row[4]))

            for chunk in self.archive_client.iter_index_price_klines(
                symbol=symbol,
                timeframe=timeframe,
                start_at=start_at,
                end_at=end_at,
            ):
                for row in chunk:
                    ts = datetime.fromtimestamp(int(str(row[0])) / 1000, tz=timezone.utc)
                    index_rows[ts] = Decimal(str(row[4]))

            for chunk in self.archive_client.iter_trade_klines(
                symbol=symbol,
                timeframe=timeframe,
                start_at=start_at,
                end_at=end_at,
            ):
                for row in chunk:
                    ts = datetime.fromtimestamp(int(str(row[0])) / 1000, tz=timezone.utc)
                    volume_rows[ts] = Decimal(str(row[5]))

            notes.append("perp_open_interest_unavailable_in_archive")

        if self.prefer_archive:
            source = "archive"
            load_from_archive()
        else:
            try:
                load_from_rest()
            except (BinanceFuturesClientError, httpx.HTTPError) as exc:
                if not self.settings.funding_basis_archive_fallback_enabled:
                    raise
                mark_rows.clear()
                index_rows.clear()
                volume_rows.clear()
                oi_rows.clear()
                source = "archive"
                notes.append(f"perp_rest_failed_fallback_archive:{type(exc).__name__}")
                load_from_archive()

        merged: list[PerpPriceRow] = []
        for ts in sorted(mark_rows):
            mark_price = mark_rows[ts]
            index_price = index_rows.get(ts, mark_price)
            merged.append(
                PerpPriceRow(
                    exchange=self.perp_exchange,
                    symbol=symbol,
                    ts=ts,
                    mark_price=mark_price,
                    index_price=index_price,
                    bid=None,
                    ask=None,
                    mid=mark_price,
                    open_interest=oi_rows.get(ts),
                    volume=volume_rows.get(ts),
                )
            )
        return merged, source, notes

    def _fetch_funding_rows(
        self,
        symbol: str,
        start_at: datetime,
        end_at: datetime,
    ) -> tuple[list[FundingRateRow], str, list[str]]:
        if self.perp_exchange == "okx_swap":
            return self._fetch_okx_funding_rows(symbol=symbol, start_at=start_at, end_at=end_at)

        rows: dict[datetime, FundingRateRow] = {}
        notes: list[str] = []
        source = "rest"

        def load_from_rest() -> None:
            for chunk in self.futures_client.iter_funding_rates(symbol=symbol, start_at=start_at, end_at=end_at):
                for row in chunk:
                    funding_time = datetime.fromtimestamp(int(str(row["fundingTime"])) / 1000, tz=timezone.utc)
                    funding_rate = Decimal(str(row["fundingRate"]))
                    rows[funding_time] = FundingRateRow(
                        exchange=self.perp_exchange,
                        symbol=symbol,
                        funding_time=funding_time,
                        funding_rate=funding_rate,
                        realized_funding_rate=funding_rate,
                    )

        def load_from_archive() -> None:
            for chunk in self.archive_client.iter_funding_rates(symbol=symbol, start_at=start_at, end_at=end_at):
                for row in chunk:
                    funding_time = datetime.fromtimestamp(int(str(row["fundingTime"])) / 1000, tz=timezone.utc)
                    funding_rate = Decimal(str(row["fundingRate"]))
                    rows[funding_time] = FundingRateRow(
                        exchange=self.perp_exchange,
                        symbol=symbol,
                        funding_time=funding_time,
                        funding_rate=funding_rate,
                        realized_funding_rate=funding_rate,
                    )

        if self.prefer_archive:
            source = "archive"
            load_from_archive()
        else:
            try:
                load_from_rest()
            except (BinanceFuturesClientError, httpx.HTTPError) as exc:
                if not self.settings.funding_basis_archive_fallback_enabled:
                    raise
                rows.clear()
                source = "archive"
                notes.append(f"funding_rest_failed_fallback_archive:{type(exc).__name__}")
                load_from_archive()

        if source == "archive" and ensure_utc(end_at) > ensure_utc(utc_now()).replace(day=1, hour=0, minute=0, second=0, microsecond=0):
            notes.append("funding_archive_current_month_may_be_incomplete")

        return [rows[key] for key in sorted(rows)], source, notes

    def _fetch_okx_perp_rows(
        self,
        *,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> tuple[list[PerpPriceRow], str, list[str]]:
        mark_rows: dict[datetime, Decimal] = {}
        index_rows: dict[datetime, Decimal] = {}
        volume_rows: dict[datetime, Decimal] = {}

        for chunk in self.okx_client.iter_mark_price_klines(
            symbol=symbol,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
        ):
            for row in chunk:
                ts = datetime.fromtimestamp(int(str(row[0])) / 1000, tz=timezone.utc)
                mark_rows[ts] = Decimal(str(row[4]))

        for chunk in self.okx_client.iter_index_price_klines(
            symbol=symbol,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
        ):
            for row in chunk:
                ts = datetime.fromtimestamp(int(str(row[0])) / 1000, tz=timezone.utc)
                index_rows[ts] = Decimal(str(row[4]))

        for chunk in self.okx_client.iter_trade_klines(
            symbol=symbol,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
        ):
            for row in chunk:
                ts = datetime.fromtimestamp(int(str(row[0])) / 1000, tz=timezone.utc)
                volume_rows[ts] = Decimal(str(row[5]))

        merged: list[PerpPriceRow] = []
        for ts in sorted(mark_rows):
            mark_price = mark_rows[ts]
            index_price = index_rows.get(ts, mark_price)
            merged.append(
                PerpPriceRow(
                    exchange=self.perp_exchange,
                    symbol=symbol,
                    ts=ts,
                    mark_price=mark_price,
                    index_price=index_price,
                    bid=None,
                    ask=None,
                    mid=mark_price,
                    open_interest=None,
                    volume=volume_rows.get(ts),
                )
            )

        return merged, "rest", ["perp_open_interest_not_supported_by_okx_history"]

    def _fetch_okx_funding_rows(
        self,
        *,
        symbol: str,
        start_at: datetime,
        end_at: datetime,
    ) -> tuple[list[FundingRateRow], str, list[str]]:
        rows: dict[datetime, FundingRateRow] = {}

        for chunk in self.okx_client.iter_funding_rates(
            symbol=symbol,
            start_at=start_at,
            end_at=end_at,
        ):
            for row in chunk:
                funding_time = datetime.fromtimestamp(int(str(row["fundingTime"])) / 1000, tz=timezone.utc)
                funding_rate = Decimal(str(row["fundingRate"]))
                realized_rate = Decimal(str(row.get("realizedRate", row["fundingRate"])))
                rows[funding_time] = FundingRateRow(
                    exchange=self.perp_exchange,
                    symbol=symbol,
                    funding_time=funding_time,
                    funding_rate=funding_rate,
                    realized_funding_rate=realized_rate,
                )

        return [rows[key] for key in sorted(rows)], "rest", []
