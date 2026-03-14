from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Iterable, Iterator, Optional, Sequence

from app.utils.time import ensure_utc


class CoinbaseTimeframe(str, Enum):
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"
    ONE_HOUR = "1h"

    @classmethod
    def from_code(cls, value: str) -> "CoinbaseTimeframe":
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"Unsupported Coinbase timeframe: {value}")

    @property
    def granularity_seconds(self) -> int:
        mapping = {
            CoinbaseTimeframe.FIVE_MINUTES: 300,
            CoinbaseTimeframe.FIFTEEN_MINUTES: 900,
            CoinbaseTimeframe.ONE_HOUR: 3600,
        }
        return mapping[self]

    @property
    def interval(self) -> timedelta:
        return timedelta(seconds=self.granularity_seconds)

    @property
    def display_name(self) -> str:
        mapping = {
            CoinbaseTimeframe.FIVE_MINUTES: "5 Minutes",
            CoinbaseTimeframe.FIFTEEN_MINUTES: "15 Minutes",
            CoinbaseTimeframe.ONE_HOUR: "1 Hour",
        }
        return mapping[self]

    def iter_request_windows(
        self,
        start_at: datetime,
        end_at: datetime,
        max_candles_per_request: int = 300,
    ) -> Iterator[tuple[datetime, datetime]]:
        normalized_start = ensure_utc(start_at)
        normalized_end = ensure_utc(end_at)
        if normalized_end <= normalized_start:
            raise ValueError("end_at must be greater than start_at")

        effective_candle_count = max(max_candles_per_request - 1, 1)
        chunk_span = self.interval * effective_candle_count
        cursor = normalized_start

        while cursor < normalized_end:
            chunk_end = min(cursor + chunk_span, normalized_end)
            yield cursor, chunk_end
            if chunk_end >= normalized_end:
                break
            cursor = chunk_end


@dataclass(frozen=True)
class NormalizedCandle:
    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal


def normalize_coinbase_candles(
    raw_rows: Iterable[Sequence[object]],
    timeframe: CoinbaseTimeframe,
) -> list[NormalizedCandle]:
    deduped: dict[datetime, NormalizedCandle] = {}

    for row in raw_rows:
        candle = _normalize_coinbase_row(row=row, timeframe=timeframe)
        if candle is not None:
            deduped[candle.open_time] = candle

    return [deduped[key] for key in sorted(deduped)]


def _normalize_coinbase_row(
    row: Sequence[object],
    timeframe: CoinbaseTimeframe,
) -> Optional[NormalizedCandle]:
    if len(row) != 6:
        return None

    try:
        timestamp = int(row[0])
        low = _to_decimal(row[1])
        high = _to_decimal(row[2])
        open_price = _to_decimal(row[3])
        close_price = _to_decimal(row[4])
        volume = _to_decimal(row[5])
    except (TypeError, ValueError, InvalidOperation):
        return None

    if timestamp < 0:
        return None
    if timestamp % timeframe.granularity_seconds != 0:
        return None
    if min(low, high, open_price, close_price) <= 0:
        return None
    if volume < 0:
        return None
    if high < low:
        return None
    if high < max(open_price, close_price):
        return None
    if low > min(open_price, close_price):
        return None

    open_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return NormalizedCandle(
        open_time=open_time,
        open=open_price,
        high=high,
        low=low,
        close=close_price,
        volume=volume,
    )


def _to_decimal(value: object) -> Decimal:
    return Decimal(str(value))
