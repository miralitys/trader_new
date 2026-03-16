from __future__ import annotations

import csv
import io
import zipfile
from datetime import date, datetime, timedelta
from typing import Iterator, Optional

import httpx

from app.core.config import Settings, get_settings
from app.integrations.binance_common import BinanceResearchTimeframe
from app.utils.research_symbols import to_binance_symbol
from app.utils.time import ensure_utc, utc_now


class BinanceArchiveClientError(Exception):
    pass


class BinanceArchiveClient:
    provider_name = "binance_archive"

    def __init__(
        self,
        settings: Optional[Settings] = None,
        client: Optional[httpx.Client] = None,
    ) -> None:
        app_settings = settings or get_settings()
        self.base_url = app_settings.funding_basis_archive_base_url.rstrip("/")
        self.timeout_seconds = app_settings.funding_basis_timeout_seconds
        self._client = client or httpx.Client(
            base_url=self.base_url,
            timeout=self.timeout_seconds,
            follow_redirects=True,
            headers={"User-Agent": "trader-research/0.1.0"},
        )

    def close(self) -> None:
        self._client.close()

    def iter_spot_klines(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> Iterator[list[list[object]]]:
        binance_symbol = to_binance_symbol(symbol)
        timeframe_value = BinanceResearchTimeframe.from_code(timeframe)
        monthly_template = (
            "spot/monthly/klines/{symbol}/{timeframe}/{symbol}-{timeframe}-{period}.zip"
        )
        daily_template = "spot/daily/klines/{symbol}/{timeframe}/{symbol}-{timeframe}-{period}.zip"
        yield from self._iter_kline_dataset(
            symbol=binance_symbol,
            timeframe=timeframe_value,
            start_at=start_at,
            end_at=end_at,
            monthly_template=monthly_template,
            daily_template=daily_template,
        )

    def iter_mark_price_klines(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> Iterator[list[list[object]]]:
        yield from self._iter_futures_kline_dataset(
            dataset="markPriceKlines",
            symbol=symbol,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
        )

    def iter_index_price_klines(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> Iterator[list[list[object]]]:
        yield from self._iter_futures_kline_dataset(
            dataset="indexPriceKlines",
            symbol=symbol,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
        )

    def iter_trade_klines(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> Iterator[list[list[object]]]:
        yield from self._iter_futures_kline_dataset(
            dataset="klines",
            symbol=symbol,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
        )

    def iter_funding_rates(
        self,
        symbol: str,
        start_at: datetime,
        end_at: datetime,
    ) -> Iterator[list[dict[str, object]]]:
        normalized_start = ensure_utc(start_at)
        normalized_end = ensure_utc(end_at)
        binance_symbol = to_binance_symbol(symbol)
        for month_start in self._iter_month_starts(normalized_start, normalized_end):
            rows = self._download_csv_rows(
                f"futures/um/monthly/fundingRate/{binance_symbol}/{binance_symbol}-fundingRate-{month_start:%Y-%m}.zip"
            )
            if rows is None:
                continue
            parsed = self._parse_funding_rows(rows=rows, start_at=normalized_start, end_at=normalized_end)
            if parsed:
                yield parsed

    def _iter_futures_kline_dataset(
        self,
        dataset: str,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> Iterator[list[list[object]]]:
        binance_symbol = to_binance_symbol(symbol)
        timeframe_value = BinanceResearchTimeframe.from_code(timeframe)
        monthly_template = (
            f"futures/um/monthly/{dataset}" + "/{symbol}/{timeframe}/{symbol}-{timeframe}-{period}.zip"
        )
        daily_template = (
            f"futures/um/daily/{dataset}" + "/{symbol}/{timeframe}/{symbol}-{timeframe}-{period}.zip"
        )
        yield from self._iter_kline_dataset(
            symbol=binance_symbol,
            timeframe=timeframe_value,
            start_at=start_at,
            end_at=end_at,
            monthly_template=monthly_template,
            daily_template=daily_template,
        )

    def _iter_kline_dataset(
        self,
        symbol: str,
        timeframe: BinanceResearchTimeframe,
        start_at: datetime,
        end_at: datetime,
        monthly_template: str,
        daily_template: str,
    ) -> Iterator[list[list[object]]]:
        normalized_start = ensure_utc(start_at)
        normalized_end = ensure_utc(end_at)
        today_utc = utc_now().date()

        for month_start in self._iter_month_starts(normalized_start, normalized_end):
            monthly_path = monthly_template.format(
                symbol=symbol,
                timeframe=timeframe.value,
                period=month_start.strftime("%Y-%m"),
            )
            monthly_rows = self._download_csv_rows(monthly_path)
            if monthly_rows is not None:
                parsed = self._parse_kline_rows(
                    rows=monthly_rows,
                    start_at=normalized_start,
                    end_at=normalized_end,
                )
                if parsed:
                    yield parsed
                continue

            first_day = max(month_start.date(), normalized_start.date())
            last_day = min(self._next_month_start(month_start).date() - timedelta(days=1), normalized_end.date())
            if last_day >= today_utc:
                last_day = today_utc - timedelta(days=1)

            cursor = first_day
            while cursor <= last_day:
                daily_path = daily_template.format(
                    symbol=symbol,
                    timeframe=timeframe.value,
                    period=cursor.strftime("%Y-%m-%d"),
                )
                daily_rows = self._download_csv_rows(daily_path)
                if daily_rows is not None:
                    parsed = self._parse_kline_rows(
                        rows=daily_rows,
                        start_at=normalized_start,
                        end_at=normalized_end,
                    )
                    if parsed:
                        yield parsed
                cursor += timedelta(days=1)

    def _download_csv_rows(self, path: str) -> Optional[list[list[str]]]:
        response = self._client.get(path)
        if response.status_code == 404:
            return None
        if response.status_code >= 400:
            raise BinanceArchiveClientError(
                f"Binance archive request failed with status {response.status_code}: {response.text}"
            )
        try:
            archive = zipfile.ZipFile(io.BytesIO(response.content))
        except zipfile.BadZipFile as exc:
            raise BinanceArchiveClientError(f"Invalid Binance archive zip for {path}") from exc
        members = archive.namelist()
        if not members:
            return None
        with archive.open(members[0]) as handle:
            wrapper = io.TextIOWrapper(handle, encoding="utf-8")
            return list(csv.reader(wrapper))

    def _parse_kline_rows(
        self,
        rows: list[list[str]],
        start_at: datetime,
        end_at: datetime,
    ) -> list[list[object]]:
        parsed: list[list[object]] = []
        for row in rows:
            if not row or not row[0]:
                continue
            if not self._looks_like_epoch(row[0]):
                continue
            open_time_ms = self._normalize_epoch_ms(row[0])
            ts = datetime.fromtimestamp(open_time_ms / 1000, tz=start_at.tzinfo)
            if ts < start_at or ts > end_at:
                continue
            normalized_row: list[object] = list(row)
            normalized_row[0] = open_time_ms
            parsed.append(normalized_row)
        return parsed

    def _parse_funding_rows(
        self,
        rows: list[list[str]],
        start_at: datetime,
        end_at: datetime,
    ) -> list[dict[str, object]]:
        if not rows:
            return []

        header = rows[0]
        data_rows = rows[1:] if header and not self._looks_like_epoch(header[0]) else rows
        if header and not self._looks_like_epoch(header[0]):
            header_map = {name: idx for idx, name in enumerate(header)}
            ts_idx = next(
                (header_map[key] for key in ("calc_time", "fundingTime", "funding_time") if key in header_map),
                None,
            )
            rate_idx = next(
                (header_map[key] for key in ("last_funding_rate", "fundingRate", "funding_rate") if key in header_map),
                None,
            )
        else:
            ts_idx = 0
            rate_idx = 2 if rows and len(rows[0]) > 2 else 1

        if ts_idx is None or rate_idx is None:
            raise BinanceArchiveClientError("Funding archive schema is missing timestamp or rate columns")

        parsed: list[dict[str, object]] = []
        for row in data_rows:
            if not row or ts_idx >= len(row) or rate_idx >= len(row):
                continue
            if not self._looks_like_epoch(row[ts_idx]):
                continue
            funding_time_ms = self._normalize_epoch_ms(row[ts_idx])
            ts = datetime.fromtimestamp(funding_time_ms / 1000, tz=start_at.tzinfo)
            if ts < start_at or ts > end_at:
                continue
            parsed.append(
                {
                    "fundingTime": funding_time_ms,
                    "fundingRate": row[rate_idx],
                }
            )
        return parsed

    def _iter_month_starts(self, start_at: datetime, end_at: datetime) -> Iterator[datetime]:
        cursor = datetime(start_at.year, start_at.month, 1, tzinfo=start_at.tzinfo)
        normalized_end = ensure_utc(end_at)
        while cursor <= normalized_end:
            yield cursor
            cursor = self._next_month_start(cursor)

    @staticmethod
    def _next_month_start(value: datetime) -> datetime:
        if value.month == 12:
            return value.replace(year=value.year + 1, month=1, day=1)
        return value.replace(month=value.month + 1, day=1)

    @staticmethod
    def _looks_like_epoch(value: str) -> bool:
        stripped = value.strip()
        return stripped.isdigit()

    @staticmethod
    def _normalize_epoch_ms(value: str) -> int:
        normalized = int(value)
        while normalized > 9_999_999_999_999:
            normalized //= 1000
        return normalized
