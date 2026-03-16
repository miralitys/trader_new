from __future__ import annotations

from datetime import datetime, timedelta
from enum import Enum
from typing import Iterator

from app.utils.time import ensure_utc


class BinanceResearchTimeframe(str, Enum):
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"
    ONE_HOUR = "1h"

    @classmethod
    def from_code(cls, value: str) -> "BinanceResearchTimeframe":
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"Unsupported research timeframe: {value}")

    @property
    def interval(self) -> timedelta:
        mapping = {
            self.FIVE_MINUTES: timedelta(minutes=5),
            self.FIFTEEN_MINUTES: timedelta(minutes=15),
            self.ONE_HOUR: timedelta(hours=1),
        }
        return mapping[self]

    @property
    def granularity_seconds(self) -> int:
        return int(self.interval.total_seconds())

    def iter_request_windows(
        self,
        start_at: datetime,
        end_at: datetime,
        max_rows_per_request: int,
    ) -> Iterator[tuple[datetime, datetime]]:
        normalized_start = ensure_utc(start_at)
        normalized_end = ensure_utc(end_at)
        if normalized_end <= normalized_start:
            raise ValueError("end_at must be greater than start_at")

        effective_rows = max(max_rows_per_request - 1, 1)
        chunk_span = self.interval * effective_rows
        cursor = normalized_start
        while cursor < normalized_end:
            chunk_end = min(cursor + chunk_span, normalized_end)
            yield cursor, chunk_end
            if chunk_end >= normalized_end:
                break
            cursor = chunk_end
