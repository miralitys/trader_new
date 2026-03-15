from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from app.integrations.binance_us import (
    BinanceUSTimeframe,
    normalize_binance_us_candles,
    normalize_binance_us_symbol,
)


def test_normalize_binance_us_symbol_converts_internal_pair_to_provider_pair() -> None:
    assert normalize_binance_us_symbol("btc-usdt") == "BTCUSDT"


def test_normalize_binance_us_candles_sorts_and_dedupes() -> None:
    raw_rows = [
        [1704067500000, "101.0", "102.0", "100.0", "101.5", "9.0"],
        [1704067200000, "100.0", "101.0", "99.5", "100.5", "12.0"],
        [1704067200000, "100.0", "101.2", "99.5", "100.6", "13.0"],
    ]

    candles = normalize_binance_us_candles(raw_rows, BinanceUSTimeframe.from_code("5m"))

    assert [candle.open_time for candle in candles] == [
        datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 0, 5, tzinfo=timezone.utc),
    ]
    assert candles[0].close == Decimal("100.6")


def test_normalize_binance_us_candles_filters_invalid_rows() -> None:
    raw_rows = [
        ["bad-row"],
        [1704067201000, "100", "101", "99", "100", "1"],
        [1704067200000, "100", "99", "101", "100", "1"],
        [1704067200000, "100", "101", "99", "100", "-1"],
        [1704067200000, "100", "101", "99", "100", "1"],
    ]

    candles = normalize_binance_us_candles(raw_rows, BinanceUSTimeframe.from_code("5m"))

    assert len(candles) == 1
    assert candles[0].open_time == datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
