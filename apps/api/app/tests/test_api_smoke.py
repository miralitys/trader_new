from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from fastapi.testclient import TestClient

from app.api.dependencies import (
    get_data_validation_service,
    get_feature_layer_service,
    get_pattern_research_service,
    get_query_service,
)
from app.main import app
from app.schemas.api import CandleCoverageResponse, FeatureCoverageResponse, FeatureRunResponse
from app.schemas.research import PatternSummaryResponse, ResearchCoverageResponse, ResearchSummaryResponse


class FakeQueryService:
    def get_candle_coverage(
        self,
        exchange_code: str,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> CandleCoverageResponse:
        return CandleCoverageResponse(
            exchange_code=exchange_code,
            symbol=symbol,
            timeframe=timeframe,
            requested_start_at=start_at,
            requested_end_at=end_at,
            loaded_start_at=start_at,
            loaded_end_at=end_at,
            candle_count=61,
            expected_candle_count=61,
            missing_candle_count=0,
            completion_pct=Decimal("100"),
        )


class FakePatternResearchService:
    def get_summary(self, **_: object) -> ResearchSummaryResponse:
        return ResearchSummaryResponse(
            generated_at=datetime(2026, 3, 27, 18, 0, tzinfo=timezone.utc),
            exchange_code="binance_us",
            lookback_days=730,
            forward_bars=12,
            fee_pct=Decimal("0.001"),
            slippage_pct=Decimal("0.0005"),
            max_bars_per_series=5000,
            coverage=[
                ResearchCoverageResponse(
                    symbol="BTC-USDT",
                    timeframe="1m",
                    candle_count=5000,
                    loaded_start_at=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc),
                    loaded_end_at=datetime(2026, 3, 27, 17, 59, tzinfo=timezone.utc),
                    completion_pct=Decimal("100"),
                    ready_for_pattern_scan=True,
                )
            ],
            patterns=[
                PatternSummaryResponse(
                    pattern_code="range_breakout",
                    pattern_name="Range Breakout",
                    symbol="BTC-USDT",
                    timeframe="5m",
                    sample_size=24,
                    win_rate_pct=Decimal("54.1"),
                    avg_forward_return_pct=Decimal("0.42"),
                    median_forward_return_pct=Decimal("0.19"),
                    avg_net_return_pct=Decimal("0.12"),
                    best_forward_return_pct=Decimal("2.34"),
                    worst_forward_return_pct=Decimal("-1.15"),
                    verdict="candidate",
                )
            ],
            notes=["Loaded history only."],
        )


class FakeDataValidationService:
    def validate(self, **_: object):
        from app.services.data_validation_service import (
            ApiTruthfulnessSummary,
            DataValidationReport,
            DataValidationResult,
            DuplicateSummary,
            GapSummary,
            StoredRangeSummary,
            TimestampAlignmentSummary,
        )

        return DataValidationReport(
            generated_at=datetime(2026, 3, 27, 18, 0, tzinfo=timezone.utc),
            exchange_code="binance_us",
            lookback_days=730,
            resync_days=14,
            perform_resync=False,
            verdict="PASS WITH WARNINGS",
            results=[
                DataValidationResult(
                    exchange_code="binance_us",
                    symbol="BTC-USDT",
                    timeframe="1m",
                    stored_range=StoredRangeSummary(
                        first_candle=datetime(2024, 3, 27, 0, 0, tzinfo=timezone.utc),
                        last_candle=datetime(2026, 3, 27, 0, 0, tzinfo=timezone.utc),
                        candle_count=100,
                        expected_candle_count=100,
                        completion_pct=Decimal("100"),
                    ),
                    validation_window=CandleCoverageResponse(
                        exchange_code="binance_us",
                        symbol="BTC-USDT",
                        timeframe="1m",
                        requested_start_at=datetime(2024, 3, 27, 0, 0, tzinfo=timezone.utc),
                        requested_end_at=datetime(2026, 3, 27, 0, 0, tzinfo=timezone.utc),
                        loaded_start_at=datetime(2024, 3, 27, 0, 0, tzinfo=timezone.utc),
                        loaded_end_at=datetime(2026, 3, 27, 0, 0, tzinfo=timezone.utc),
                        candle_count=100,
                        expected_candle_count=100,
                        missing_candle_count=0,
                        completion_pct=Decimal("100"),
                    ),
                    duplicates=DuplicateSummary(duplicate_count=0, duplicate_bucket_count=0, sample_duplicates=[]),
                    timestamp_alignment=TimestampAlignmentSummary(invalid_timestamp_count=0, sample_invalid_timestamps=[]),
                    gaps=GapSummary(missing_candle_count=2, sample_missing_timestamps=[]),
                    api_truthfulness=ApiTruthfulnessSummary(
                        coverage_endpoint_matches_db=True,
                        status_endpoint_matches_db=True,
                        latest_sync_job_id=1,
                        latest_sync_job_status="completed",
                        notes=[],
                    ),
                    resync=None,
                    issues=[],
                    verdict="PASS WITH WARNINGS",
                )
            ],
        )


class FakeFeatureLayerService:
    def run(self, **_: object) -> FeatureRunResponse:
        return FeatureRunResponse(
            id=77,
            exchange="binance_us",
            symbol="BTC-USDT",
            timeframe="15m",
            lookback_days=180,
            start_at=datetime(2025, 9, 28, 0, 0, tzinfo=timezone.utc),
            end_at=datetime(2026, 3, 27, 0, 0, tzinfo=timezone.utc),
            status="completed",
            source_candle_count=1234,
            feature_rows_upserted=1180,
            computed_start_at=datetime(2025, 9, 28, 0, 15, tzinfo=timezone.utc),
            computed_end_at=datetime(2026, 3, 27, 0, 0, tzinfo=timezone.utc),
            error_text=None,
            created_at=datetime(2026, 3, 27, 18, 0, tzinfo=timezone.utc),
            updated_at=datetime(2026, 3, 27, 18, 5, tzinfo=timezone.utc),
        )

    def list_runs(self, **_: object) -> list[FeatureRunResponse]:
        return [self.run()]

    def get_symbol_timeframe_coverages(self, **_: object) -> list[FeatureCoverageResponse]:
        return [
            FeatureCoverageResponse(
                exchange_code="binance_us",
                symbol="BTC-USDT",
                timeframe="15m",
                feature_count=1180,
                loaded_start_at=datetime(2025, 9, 28, 0, 15, tzinfo=timezone.utc),
                loaded_end_at=datetime(2026, 3, 27, 0, 0, tzinfo=timezone.utc),
            )
        ]


def test_research_summary_endpoint_returns_pattern_payload(client: TestClient) -> None:
    app.dependency_overrides[get_pattern_research_service] = lambda: FakePatternResearchService()

    response = client.get("/api/research/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["coverage"][0]["timeframe"] == "1m"
    assert payload["patterns"][0]["pattern_code"] == "range_breakout"
    assert payload["patterns"][0]["verdict"] == "candidate"


def test_candle_coverage_endpoint_returns_aggregate_payload(client: TestClient) -> None:
    app.dependency_overrides[get_query_service] = lambda: FakeQueryService()

    response = client.get(
        "/api/candles/coverage",
        params={
            "exchange_code": "binance_us",
            "symbol": "BTC-USDT",
            "timeframe": "1m",
            "start_at": "2026-03-14T00:00:00Z",
            "end_at": "2026-03-14T01:00:00Z",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "BTC-USDT"
    assert payload["timeframe"] == "1m"
    assert payload["candle_count"] == 61
    assert payload["completion_pct"] == "100"


def test_data_validation_endpoint_returns_summary_payload(client: TestClient) -> None:
    app.dependency_overrides[get_data_validation_service] = lambda: FakeDataValidationService()

    response = client.post(
        "/api/data/validation-report",
        json={
            "exchange_code": "binance_us",
            "symbols": ["BTC-USDT"],
            "timeframes": ["1m"],
            "lookback_days": 730,
            "sample_limit": 5,
            "perform_resync": False,
            "resync_days": 14,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["overview"]["total_series"] == 1
    assert payload["summary"]["one_minute_laggards"][0]["symbol"] == "BTC-USDT"
    assert payload["results"][0]["timeframe"] == "1m"


def test_feature_run_endpoint_returns_feature_payload(client: TestClient) -> None:
    app.dependency_overrides[get_feature_layer_service] = lambda: FakeFeatureLayerService()

    response = client.post(
        "/api/features/run",
        json={
            "exchange_code": "binance_us",
            "symbol": "BTC-USDT",
            "timeframe": "15m",
            "lookback_days": 180,
        },
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["symbol"] == "BTC-USDT"
    assert payload["feature_rows_upserted"] == 1180
    assert payload["status"] == "completed"


def test_feature_coverage_endpoint_returns_rows(client: TestClient) -> None:
    app.dependency_overrides[get_feature_layer_service] = lambda: FakeFeatureLayerService()

    response = client.get("/api/features/coverage", params={"exchange_code": "binance_us"})

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["symbol"] == "BTC-USDT"
    assert payload[0]["feature_count"] == 1180
