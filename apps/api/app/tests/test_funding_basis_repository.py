from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from app.repositories.funding_basis_repository import (
    FundingBasisRepository,
    FundingRateRow,
    SpotPriceRow,
)


class _FakeResult:
    def __init__(self, count: int) -> None:
        self.count = count

    def scalars(self):
        return range(self.count)


class _FakeSession:
    def __init__(self) -> None:
        self.chunk_sizes: list[int] = []

    def execute(self, stmt):
        rows = stmt._multi_values[0]
        self.chunk_sizes.append(len(rows))
        return _FakeResult(len(rows))


def test_upsert_spot_prices_chunks_large_payload() -> None:
    session = _FakeSession()
    repository = FundingBasisRepository(session)
    ts = datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc)

    inserted = repository.upsert_spot_prices(
        [
            SpotPriceRow(
                exchange="binance_spot",
                symbol="BTC-USDT",
                ts=ts,
                bid=None,
                ask=None,
                mid=Decimal("50000"),
                close=Decimal("50000"),
                volume=Decimal("10"),
            )
            for _ in range(2501)
        ]
    )

    assert inserted == 2501
    assert session.chunk_sizes == [1000, 1000, 501]


def test_upsert_funding_rates_chunks_large_payload() -> None:
    session = _FakeSession()
    repository = FundingBasisRepository(session)
    funding_time = datetime(2026, 3, 15, 8, 0, tzinfo=timezone.utc)

    inserted = repository.upsert_funding_rates(
        [
            FundingRateRow(
                exchange="okx_swap",
                symbol="ACT-USDT",
                funding_time=funding_time,
                funding_rate=Decimal("0.0005"),
                realized_funding_rate=None,
            )
            for _ in range(4101)
        ]
    )

    assert inserted == 4101
    assert session.chunk_sizes == [2000, 2000, 101]
