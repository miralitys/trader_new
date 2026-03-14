from __future__ import annotations

from decimal import Decimal
from datetime import datetime, timezone

from app.integrations.coinbase import CoinbaseTimeframe, normalize_coinbase_candles


def test_normalize_coinbase_candles_sorts_and_dedupes() -> None:
    raw_rows = [
        [1710461100, "99", "105", "100", "103", "12.5"],
        [1710460800, "98", "102", "99", "101", "10.0"],
        [1710460800, "98", "102", "99", "101", "10.0"],
    ]

    candles = normalize_coinbase_candles(raw_rows, CoinbaseTimeframe.from_code("5m"))

    assert len(candles) == 2
    assert candles[0].open_time < candles[1].open_time
    assert candles[0].open_time == datetime.fromtimestamp(1710460800, tz=timezone.utc)
    assert candles[1].close == Decimal("103")


def test_normalize_coinbase_candles_filters_invalid_rows() -> None:
    raw_rows = [
        [1710461400, "100", "104", "101", "103", "12.0"],
        [1710461700, "100", "99", "101", "103", "12.0"],
        [1710462001, "100", "104", "101", "103", "12.0"],
        [1710462300, "0", "104", "101", "103", "12.0"],
    ]

    candles = normalize_coinbase_candles(raw_rows, CoinbaseTimeframe.from_code("5m"))

    assert len(candles) == 1
    assert candles[0].open == Decimal("101")
