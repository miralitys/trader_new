from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from app.repositories.candle_repository import CandleCoverageSummary
from app.services.data_validation_service import (
    ApiTruthfulnessSummary,
    DataValidationService,
    DuplicateSummary,
    GapSummary,
    ResyncSummary,
    StoredRangeSummary,
    TimestampAlignmentSummary,
    derive_recent_window,
)


def test_derive_recent_window_uses_last_complete_grid_bucket() -> None:
    first_candle = datetime(2025, 3, 15, 4, 30, tzinfo=timezone.utc)
    last_candle = datetime(2026, 3, 15, 4, 25, tzinfo=timezone.utc)

    start_at, end_at = derive_recent_window(first_candle, last_candle, "5m", lookback_days=90)

    assert start_at == datetime(2025, 12, 15, 4, 30, tzinfo=timezone.utc)
    assert end_at == last_candle


def test_derive_recent_window_caps_to_first_available_candle() -> None:
    first_candle = datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc)
    last_candle = datetime(2026, 3, 15, 0, 0, tzinfo=timezone.utc)

    start_at, end_at = derive_recent_window(first_candle, last_candle, "1h", lookback_days=90)

    assert start_at == first_candle
    assert end_at == last_candle


def test_build_issues_marks_clean_dataset_as_warning_only_when_resync_skipped() -> None:
    service = DataValidationService.__new__(DataValidationService)

    issues = service._build_issues(  # type: ignore[attr-defined]
        stored_range=StoredRangeSummary(
            first_candle=datetime(2025, 3, 15, 4, 30, tzinfo=timezone.utc),
            last_candle=datetime(2026, 3, 15, 4, 25, tzinfo=timezone.utc),
            candle_count=105120,
            expected_candle_count=105120,
            completion_pct=Decimal("100.00"),
        ),
        validation_window=CandleCoverageSummary(
            exchange_code="binance_us",
            symbol_code="BTC-USDT",
            timeframe="5m",
            requested_start_at=datetime(2025, 12, 15, 4, 30, tzinfo=timezone.utc),
            requested_end_at=datetime(2026, 3, 15, 4, 25, tzinfo=timezone.utc),
            actual_start_at=datetime(2025, 12, 15, 4, 30, tzinfo=timezone.utc),
            actual_end_at=datetime(2026, 3, 15, 4, 25, tzinfo=timezone.utc),
            candle_count=25920,
            expected_candle_count=25920,
            missing_candle_count=0,
            completion_pct=Decimal("100.00"),
        ),
        duplicates=DuplicateSummary(duplicate_count=0, duplicate_bucket_count=0, sample_duplicates=[]),
        alignment=TimestampAlignmentSummary(invalid_timestamp_count=0, sample_invalid_timestamps=[]),
        gaps=GapSummary(missing_candle_count=0, sample_missing_timestamps=[]),
        api_truthfulness=ApiTruthfulnessSummary(
            coverage_endpoint_matches_db=True,
            status_endpoint_matches_db=True,
            latest_sync_job_id=35,
            latest_sync_job_status="completed",
            notes=[],
        ),
        resync=None,
    )

    assert len(issues) == 1
    assert issues[0].code == "resync_not_performed"
    assert issues[0].severity == "medium"


def test_build_issues_marks_resync_instability_as_critical() -> None:
    service = DataValidationService.__new__(DataValidationService)

    issues = service._build_issues(  # type: ignore[attr-defined]
        stored_range=StoredRangeSummary(
            first_candle=datetime(2025, 3, 15, 4, 30, tzinfo=timezone.utc),
            last_candle=datetime(2026, 3, 15, 4, 25, tzinfo=timezone.utc),
            candle_count=105120,
            expected_candle_count=105120,
            completion_pct=Decimal("100.00"),
        ),
        validation_window=CandleCoverageSummary(
            exchange_code="binance_us",
            symbol_code="BTC-USDT",
            timeframe="5m",
            requested_start_at=datetime(2025, 12, 15, 4, 30, tzinfo=timezone.utc),
            requested_end_at=datetime(2026, 3, 15, 4, 25, tzinfo=timezone.utc),
            actual_start_at=datetime(2025, 12, 15, 4, 30, tzinfo=timezone.utc),
            actual_end_at=datetime(2026, 3, 15, 4, 25, tzinfo=timezone.utc),
            candle_count=25920,
            expected_candle_count=25920,
            missing_candle_count=0,
            completion_pct=Decimal("100.00"),
        ),
        duplicates=DuplicateSummary(duplicate_count=0, duplicate_bucket_count=0, sample_duplicates=[]),
        alignment=TimestampAlignmentSummary(invalid_timestamp_count=0, sample_invalid_timestamps=[]),
        gaps=GapSummary(missing_candle_count=0, sample_missing_timestamps=[]),
        api_truthfulness=ApiTruthfulnessSummary(
            coverage_endpoint_matches_db=True,
            status_endpoint_matches_db=True,
            latest_sync_job_id=35,
            latest_sync_job_status="completed",
            notes=[],
        ),
        resync=ResyncSummary(
            requested_start_at=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc),
            requested_end_at=datetime(2026, 3, 15, 4, 25, tzinfo=timezone.utc),
            before_count=4032,
            after_count=4033,
            before_loaded_start_at=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc),
            before_loaded_end_at=datetime(2026, 3, 15, 4, 25, tzinfo=timezone.utc),
            after_loaded_start_at=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc),
            after_loaded_end_at=datetime(2026, 3, 15, 4, 25, tzinfo=timezone.utc),
            rows_inserted=1,
            duplicate_count_after=0,
            stable=False,
        ),
    )

    assert any(issue.code == "resync_instability" and issue.severity == "critical" for issue in issues)
