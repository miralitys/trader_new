from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

from app.services.feature_layer_service import FeatureLayerService


def _candle(at: datetime, open_: str, high: str, low: str, close: str, volume: str):
    return SimpleNamespace(
        open_time=at,
        open=Decimal(open_),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal(volume),
    )


def test_compute_feature_rows_handles_sparse_early_windows_without_none_multiplication() -> None:
    service = FeatureLayerService.__new__(FeatureLayerService)
    start = datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc)

    source_candles = [
        _candle(start + timedelta(hours=4 * index), "100", "101", "99", str(100 + index), "10")
        for index in range(6)
    ]

    rows = FeatureLayerService._compute_feature_rows(service, source_candles=source_candles, start_at=start)

    assert len(rows) == 6
    assert rows[0].realized_vol_20 is None
    assert rows[1].realized_vol_20 is None
    assert rows[2].realized_vol_20 is not None
