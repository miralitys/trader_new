from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from app.repositories.candle_repository import CandleCoverageSummary, estimate_expected_candle_count
from app.services.query_service import QueryService


def test_estimate_expected_candle_count_for_aligned_range() -> None:
    start_at = datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc)
    end_at = datetime(2026, 3, 1, 1, 0, tzinfo=timezone.utc)

    assert estimate_expected_candle_count("5m", start_at, end_at) == 13


def test_estimate_expected_candle_count_for_unaligned_range() -> None:
    start_at = datetime(2026, 3, 1, 0, 2, tzinfo=timezone.utc)
    end_at = datetime(2026, 3, 1, 1, 1, tzinfo=timezone.utc)

    assert estimate_expected_candle_count("5m", start_at, end_at) == 12


def test_estimate_expected_candle_count_returns_zero_when_no_bucket_fits() -> None:
    start_at = datetime(2026, 3, 1, 0, 1, tzinfo=timezone.utc)
    end_at = datetime(2026, 3, 1, 0, 4, tzinfo=timezone.utc)

    assert estimate_expected_candle_count("5m", start_at, end_at) == 0


def test_query_service_list_sync_jobs_includes_actual_coverage() -> None:
    job = SimpleNamespace(
        id=11,
        exchange="binance_us",
        symbol="BTC-USDT",
        timeframe="5m",
        start_at=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc),
        end_at=datetime(2026, 3, 1, 1, 0, tzinfo=timezone.utc),
        status=SimpleNamespace(value="completed"),
        rows_inserted=42,
        error_text=None,
        created_at=datetime(2026, 3, 1, 1, 5, tzinfo=timezone.utc),
        updated_at=datetime(2026, 3, 1, 1, 10, tzinfo=timezone.utc),
    )

    class FakeSyncJobRepository:
        def list_jobs(self, **_: object) -> list[SimpleNamespace]:
            return [job]

    class FakeCandleRepository:
        def get_candle_coverage(self, **kwargs: object) -> CandleCoverageSummary:
            assert kwargs["exchange_code"] == "binance_us"
            assert kwargs["symbol_code"] == "BTC-USDT"
            assert kwargs["timeframe"] == "5m"
            return CandleCoverageSummary(
                exchange_code="binance_us",
                symbol_code="BTC-USDT",
                timeframe="5m",
                requested_start_at=job.start_at,
                requested_end_at=job.end_at,
                actual_start_at=job.start_at,
                actual_end_at=job.end_at,
                candle_count=13,
                expected_candle_count=13,
                missing_candle_count=0,
                completion_pct=Decimal("100.00"),
            )

    service = QueryService(session=None)  # type: ignore[arg-type]
    service.sync_job_repository = FakeSyncJobRepository()
    service.candle_repository = FakeCandleRepository()

    jobs = service.list_sync_jobs(limit=10)

    assert len(jobs) == 1
    assert jobs[0].coverage is not None
    assert jobs[0].coverage.candle_count == 13
    assert jobs[0].coverage.loaded_start_at == job.start_at
    assert jobs[0].coverage.completion_pct == Decimal("100.00")
