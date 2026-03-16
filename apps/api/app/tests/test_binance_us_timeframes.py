from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.integrations.binance_us import BinanceUSTimeframe


def test_binance_us_timeframe_mapping() -> None:
    assert BinanceUSTimeframe.from_code("5m").granularity_seconds == 300
    assert BinanceUSTimeframe.from_code("15m").granularity_seconds == 900
    assert BinanceUSTimeframe.from_code("1h").granularity_seconds == 3600
    assert BinanceUSTimeframe.from_code("4h").granularity_seconds == 14400


def test_binance_us_timeframe_windows_chunk_large_ranges() -> None:
    start_at = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_at = start_at + timedelta(seconds=300 * 650)

    windows = list(BinanceUSTimeframe.from_code("5m").iter_request_windows(start_at, end_at))

    assert len(windows) == 1
    assert windows[0][0] == start_at
    assert windows[-1][1] == end_at


def test_binance_us_timeframe_rejects_unsupported_values() -> None:
    with pytest.raises(ValueError):
        BinanceUSTimeframe.from_code("1d")
