from __future__ import annotations

from decimal import Decimal
from datetime import datetime, timezone

from app.integrations.binance_us import BinanceUSTimeframe, normalize_binance_us_candles


def test_normalize_binance_us_candles_sorts_and_dedupes() -> None:
    raw_rows = [
        [1710461100000, "100", "105", "99", "103", "12.5"],
        [1710460800000, "99", "102", "98", "101", "10.0"],
        [1710460800000, "99", "102", "98", "101.5", "10.0"],
    ]

    candles = normalize_binance_us_candles(raw_rows, BinanceUSTimeframe.from_code("5m"))

    assert len(candles) == 2
    assert candles[0].open_time < candles[1].open_time
    assert candles[0].open_time == datetime.fromtimestamp(1710460800, tz=timezone.utc)
    assert candles[0].close == Decimal("101.5")
    assert candles[1].close == Decimal("103")


def test_normalize_binance_us_candles_filters_invalid_rows() -> None:
    raw_rows = [
        [1710461400000, "101", "104", "100", "103", "12.0"],
        [1710461700000, "101", "99", "100", "103", "12.0"],
        [1710462001000, "101", "104", "100", "103", "12.0"],
        [1710462300000, "0", "104", "100", "103", "12.0"],
    ]

    candles = normalize_binance_us_candles(raw_rows, BinanceUSTimeframe.from_code("5m"))

    assert len(candles) == 1
    assert candles[0].open == Decimal("101")
