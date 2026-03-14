from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.integrations.coinbase import CoinbaseTimeframe


def test_coinbase_timeframe_mapping() -> None:
    assert CoinbaseTimeframe.from_code("5m").granularity_seconds == 300
    assert CoinbaseTimeframe.from_code("15m").granularity_seconds == 900
    assert CoinbaseTimeframe.from_code("1h").granularity_seconds == 3600


def test_coinbase_timeframe_windows_chunk_large_ranges() -> None:
    start_at = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_at = start_at + timedelta(seconds=300 * 650)

    windows = list(CoinbaseTimeframe.from_code("5m").iter_request_windows(start_at, end_at))

    assert len(windows) == 3
    assert windows[0][0] == start_at
    assert windows[-1][1] == end_at


def test_coinbase_timeframe_rejects_unsupported_values() -> None:
    with pytest.raises(ValueError):
        CoinbaseTimeframe.from_code("4h")
