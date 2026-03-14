from __future__ import annotations

from datetime import datetime, timezone

from app.exchanges.coinbase import CoinbaseAdapter


def test_coinbase_normalization_sorts_and_maps_fields() -> None:
    adapter = CoinbaseAdapter()
    payload = [
        [1735689900, 98, 102, 99, 101, 42],
        [1735689600, 95, 100, 96, 99, 30],
    ]
    candles = adapter._normalize_response(payload, 300)

    assert len(candles) == 2
    assert candles[0].open_time < candles[1].open_time
    assert candles[0].open == 96
    assert candles[1].close == 101
