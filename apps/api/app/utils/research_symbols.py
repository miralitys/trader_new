from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Iterable, Optional, Sequence, TypeVar

from app.utils.time import ensure_utc

KNOWN_QUOTES = ("USDT", "USDC", "BUSD", "USD")

T = TypeVar("T")


def normalize_research_symbol(value: str) -> str:
    normalized = value.strip().upper()
    if not normalized:
        raise ValueError("Research symbol must not be empty")

    normalized = normalized.replace("/", "").replace("_", "").replace(" ", "")
    normalized = normalized.replace("PERPETUAL", "").replace("PERP", "").replace("SWAP", "")
    normalized = normalized.strip("-")

    if "-" in normalized:
        parts = [part for part in normalized.split("-") if part]
        if len(parts) < 2:
            raise ValueError(f"Unable to normalize research symbol: {value}")
        base, quote = parts[0], parts[1]
        if not base or not quote:
            raise ValueError(f"Unable to normalize research symbol: {value}")
        return f"{base}-{quote}"

    for quote in KNOWN_QUOTES:
        if normalized.endswith(quote) and len(normalized) > len(quote):
            base = normalized[: -len(quote)]
            return f"{base}-{quote}"

    raise ValueError(f"Unable to normalize research symbol: {value}")


def to_binance_symbol(value: str) -> str:
    return normalize_research_symbol(value).replace("-", "")


def to_okx_swap_inst_id(value: str) -> str:
    return f"{normalize_research_symbol(value)}-SWAP"


def to_okx_index_inst_id(value: str) -> str:
    return normalize_research_symbol(value)


@dataclass(frozen=True)
class SnapshotAlignmentResult:
    matched: bool
    distance_seconds: Optional[int]


def select_nearest_snapshot(
    target_time: datetime,
    snapshots: Sequence[T],
    *,
    max_alignment_seconds: int,
    timestamp_getter: Callable[[T], datetime],
) -> tuple[Optional[T], SnapshotAlignmentResult]:
    if not snapshots:
        return None, SnapshotAlignmentResult(matched=False, distance_seconds=None)

    target = ensure_utc(target_time)
    timestamps = [ensure_utc(timestamp_getter(item)) for item in snapshots]
    cursor = bisect_left(timestamps, target)
    candidates: list[tuple[int, T]] = []

    if cursor < len(snapshots):
        ts = timestamps[cursor]
        candidates.append((abs(int((ts - target).total_seconds())), snapshots[cursor]))
    if cursor > 0:
        ts = timestamps[cursor - 1]
        candidates.append((abs(int((ts - target).total_seconds())), snapshots[cursor - 1]))

    if not candidates:
        return None, SnapshotAlignmentResult(matched=False, distance_seconds=None)

    distance_seconds, selected = sorted(
        candidates,
        key=lambda item: (
            item[0],
            ensure_utc(timestamp_getter(item[1])),
        ),
    )[0]
    if distance_seconds > max_alignment_seconds:
        return None, SnapshotAlignmentResult(matched=False, distance_seconds=distance_seconds)
    return selected, SnapshotAlignmentResult(matched=True, distance_seconds=distance_seconds)
