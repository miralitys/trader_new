from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from fastapi.testclient import TestClient

from app.api.dependencies import get_backtest_runner_service, get_query_service, get_strategy_service
from app.main import app
from app.schemas.api import (
    BacktestListItemResponse,
    CandleCoverageResponse,
    DashboardDataSyncStatus,
    DashboardRunStatus,
    DashboardSummaryResponse,
    StrategySummaryResponse,
)
from app.schemas.backtest import BacktestMetrics, BacktestRequest, BacktestResponse


class FakeStrategyService:
    def list_strategies(self) -> list[StrategySummaryResponse]:
        return [
            StrategySummaryResponse(
                code="breakout_retest",
                name="BreakoutRetest",
                description="Smoke test strategy",
                has_saved_config=True,
            )
        ]


class FakeBacktestRunnerService:
    def run_backtest(self, request: BacktestRequest) -> BacktestResponse:
        return BacktestResponse(
            run_id=42,
            strategy_code=request.strategy_code,
            symbol=request.symbol,
            timeframe=request.timeframe,
            exchange_code=request.exchange_code,
            status="completed",
            initial_capital=request.initial_capital,
            final_equity=Decimal("10500"),
            started_at=datetime(2026, 3, 14, 12, 0, tzinfo=timezone.utc),
            completed_at=datetime(2026, 3, 14, 12, 5, tzinfo=timezone.utc),
            params=request.model_dump(mode="json"),
            metrics=BacktestMetrics(
                total_return_pct=Decimal("5"),
                max_drawdown_pct=Decimal("1.5"),
                win_rate_pct=Decimal("50"),
                profit_factor=Decimal("1.2"),
                expectancy=Decimal("100"),
                avg_winner=Decimal("200"),
                avg_loser=Decimal("-50"),
                total_trades=2,
            ),
            equity_curve=[],
            trades=[],
        )

    def stop_backtest(self, run_id: int, reason: str = "manual_stop") -> BacktestResponse:
        return BacktestResponse(
            run_id=run_id,
            strategy_code="breakout_retest",
            symbol="BTC-USDT",
            timeframe="5m",
            exchange_code="binance_us",
            status="failed",
            initial_capital=Decimal("10000"),
            final_equity=Decimal("10000"),
            started_at=datetime(2026, 3, 14, 12, 0, tzinfo=timezone.utc),
            completed_at=datetime(2026, 3, 14, 12, 1, tzinfo=timezone.utc),
            params={"reason": reason},
            metrics=BacktestMetrics(),
            equity_curve=[],
            trades=[],
            error_text=f"manual_stop:{reason}",
        )


class FakeQueryService:
    def get_dashboard_summary(self) -> DashboardSummaryResponse:
        return DashboardSummaryResponse(
            strategies=[
                StrategySummaryResponse(
                    code="trend_retrace_70",
                    name="TrendRetrace70",
                    description="Dashboard strategy",
                )
            ],
            run_status=DashboardRunStatus(active_paper_runs=1, recent_backtests=1),
            key_performance_metrics=[],
            open_positions_count=2,
            recent_trades=[],
            recent_backtests=[
                BacktestListItemResponse(
                    id=7,
                    strategy_code="trend_retrace_70",
                    strategy_name="TrendRetrace70",
                    status="completed",
                    symbol="BTC-USDT",
                    timeframe="5m",
                    started_at=datetime(2026, 3, 14, 12, 0, tzinfo=timezone.utc),
                    completed_at=datetime(2026, 3, 14, 12, 15, tzinfo=timezone.utc),
                    initial_capital=Decimal("10000"),
                    final_equity=Decimal("10300"),
                    total_return_pct=Decimal("3"),
                    max_drawdown_pct=Decimal("1"),
                    win_rate_pct=Decimal("60"),
                    total_trades=5,
                )
            ],
            data_sync_status=DashboardDataSyncStatus(),
        )

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
            candle_count=13,
            expected_candle_count=13,
            missing_candle_count=0,
            completion_pct=Decimal("100"),
        )


def test_strategies_endpoint_returns_registered_strategies(client: TestClient) -> None:
    app.dependency_overrides[get_strategy_service] = lambda: FakeStrategyService()

    response = client.get("/api/strategies")

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["code"] == "breakout_retest"
    assert payload[0]["has_saved_config"] is True


def test_backtest_run_endpoint_returns_report(client: TestClient) -> None:
    app.dependency_overrides[get_backtest_runner_service] = lambda: FakeBacktestRunnerService()

    response = client.post(
        "/api/backtests/run",
        json={
            "strategy_code": "breakout_retest",
            "symbol": "BTC-USDT",
            "timeframe": "5m",
            "start_at": "2026-03-14T00:00:00Z",
            "end_at": "2026-03-14T01:00:00Z",
            "exchange_code": "binance_us",
            "initial_capital": "10000",
            "fee": "0.001",
            "slippage": "0.0005",
            "position_size_pct": "1",
            "strategy_config_override": {},
        },
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["run_id"] == 42
    assert payload["metrics"]["total_trades"] == 2
    assert payload["final_equity"] == "10500"


def test_backtest_stop_endpoint_returns_failed_report(client: TestClient) -> None:
    app.dependency_overrides[get_backtest_runner_service] = lambda: FakeBacktestRunnerService()

    response = client.post("/api/backtests/9/stop", json={"reason": "manual_stop"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["run_id"] == 9
    assert payload["status"] == "failed"
    assert payload["error_text"] == "manual_stop:manual_stop"


def test_dashboard_summary_endpoint_returns_aggregate_payload(client: TestClient) -> None:
    app.dependency_overrides[get_query_service] = lambda: FakeQueryService()

    response = client.get("/api/dashboard/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["run_status"]["active_paper_runs"] == 1
    assert payload["open_positions_count"] == 2
    assert payload["recent_backtests"][0]["strategy_code"] == "trend_retrace_70"


def test_candle_coverage_endpoint_returns_aggregate_payload(client: TestClient) -> None:
    app.dependency_overrides[get_query_service] = lambda: FakeQueryService()

    response = client.get(
        "/api/candles/coverage",
        params={
            "exchange_code": "binance_us",
            "symbol": "BTC-USDT",
            "timeframe": "5m",
            "start_at": "2026-03-14T00:00:00Z",
            "end_at": "2026-03-14T01:00:00Z",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "BTC-USDT"
    assert payload["candle_count"] == 13
    assert payload["completion_pct"] == "100"
