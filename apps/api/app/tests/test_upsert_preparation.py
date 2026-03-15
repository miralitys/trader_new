from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from app.integrations.binance_us import NormalizedCandle
from app.repositories.candle_repository import prepare_candle_upsert_rows


def test_prepare_candle_upsert_rows_is_sorted_and_deduped() -> None:
    duplicate_time = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    later_time = datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc)

    candles = [
        NormalizedCandle(
            open_time=later_time,
            open=Decimal("102"),
            high=Decimal("105"),
            low=Decimal("101"),
            close=Decimal("104"),
            volume=Decimal("10"),
        ),
        NormalizedCandle(
            open_time=duplicate_time,
            open=Decimal("100"),
            high=Decimal("103"),
            low=Decimal("99"),
            close=Decimal("101"),
            volume=Decimal("9"),
        ),
        NormalizedCandle(
            open_time=duplicate_time,
            open=Decimal("100"),
            high=Decimal("103"),
            low=Decimal("99"),
            close=Decimal("102"),
            volume=Decimal("9.5"),
        ),
    ]

    rows = prepare_candle_upsert_rows(
        exchange_id=1,
        symbol_id=2,
        timeframe="5m",
        candles=candles,
    )

    assert len(rows) == 2
    assert rows[0]["open_time"] == duplicate_time
    assert rows[1]["open_time"] == later_time
    assert rows[0]["close"] == Decimal("102")
    assert rows[0]["exchange_id"] == 1
    assert rows[0]["symbol_id"] == 2
