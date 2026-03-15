from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.models.enums import BacktestStatus
from app.engines.backtest_engine import BacktestStopRequestedError
from app.schemas.backtest import BacktestCandle, BacktestMetrics, BacktestRequest, BacktestResponse
from app.services.backtest_runner_service import BacktestRunnerService
from app.services.query_service import QueryService
from app.strategies.base import BaseStrategy, BaseStrategyConfig


class FakeSession:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


class SessionFactory:
    def __init__(self) -> None:
        self.sessions: list[FakeSession] = []

    def __call__(self) -> FakeSession:
        session = FakeSession()
        self.sessions.append(session)
        return session


class FakeStrategy(BaseStrategy):
    key = "fake_strategy"
    name = "FakeStrategy"
    description = "Synthetic strategy for runner tests."
    config_model = BaseStrategyConfig


class ArchivedFakeStrategy(FakeStrategy):
    key = "archived_fake_strategy"
    name = "ArchivedFakeStrategy"
    status = "archived"


class PausedFakeStrategy(FakeStrategy):
    key = "paused_fake_strategy"
    name = "PausedFakeStrategy"
    status = "paused"


class FakeBacktestRepository:
    current_run: SimpleNamespace | None = None
    save_result_exception: Exception | None = None
    recovered_runs: list[dict[str, object]] = []
    recover_calls = 0

    def __init__(self, session: FakeSession) -> None:
        self.session = session

    @classmethod
    def reset(cls) -> None:
        cls.current_run = None
        cls.save_result_exception = None
        cls.recovered_runs = []
        cls.recover_calls = 0

    def ensure_strategy(self, code: str, name: str, description: str) -> SimpleNamespace:
        return SimpleNamespace(id=1, code=code, name=name, description=description)

    def create_run(self, strategy_id: int, params_json: dict[str, object]) -> SimpleNamespace:
        run = SimpleNamespace(
            id=8,
            strategy_id=strategy_id,
            params_json=params_json,
            status=BacktestStatus.QUEUED,
            started_at=None,
            completed_at=None,
            error_text=None,
            created_at=datetime(2026, 3, 14, 12, 0, tzinfo=timezone.utc),
        )
        FakeBacktestRepository.current_run = run
        return run

    def get_run(self, run_id: int) -> SimpleNamespace | None:
        if FakeBacktestRepository.current_run is None or FakeBacktestRepository.current_run.id != run_id:
            return None
        return FakeBacktestRepository.current_run

    def get_run_with_result(
        self,
        run_id: int,
    ) -> tuple[SimpleNamespace, SimpleNamespace, None] | None:
        run = self.get_run(run_id)
        if run is None:
            return None
        return run, SimpleNamespace(code="fake_strategy", name="FakeStrategy"), None

    def mark_running(self, run: SimpleNamespace, started_at: datetime) -> SimpleNamespace:
        run.status = BacktestStatus.RUNNING
        run.started_at = started_at
        run.error_text = None
        return run

    def mark_completed(self, run: SimpleNamespace, completed_at: datetime) -> SimpleNamespace:
        run.status = BacktestStatus.COMPLETED
        run.completed_at = completed_at
        run.error_text = None
        return run

    def mark_failed(self, run: SimpleNamespace, completed_at: datetime, error_text: str) -> SimpleNamespace:
        run.status = BacktestStatus.FAILED
        run.completed_at = completed_at
        run.error_text = error_text
        return run

    def save_result(self, backtest_run_id: int, report: BacktestResponse) -> None:
        if FakeBacktestRepository.save_result_exception is not None:
            raise FakeBacktestRepository.save_result_exception

    def recover_stale_runs(self, stale_before: datetime) -> list[dict[str, object]]:
        FakeBacktestRepository.recover_calls += 1
        return list(FakeBacktestRepository.recovered_runs)


class FakeCandleRepository:
    def __init__(self, session: FakeSession) -> None:
        self.session = session

    def list_candles(
        self,
        exchange_code: str,
        symbol_code: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> list[BacktestCandle]:
        return [
            BacktestCandle(
                open_time=start_at,
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100"),
                volume=Decimal("1"),
            )
        ]


class RaisingEngine:
    def run(self, *args: object, **kwargs: object) -> BacktestResponse:
        raise RuntimeError("engine exploded")


class StaticReportEngine:
    def run(self, request: BacktestRequest, *args: object, **kwargs: object) -> BacktestResponse:
        started_at = kwargs["started_at"]
        completed_at = kwargs["completed_at"]()
        return BacktestResponse(
            run_id=None,
            strategy_code=request.strategy_code,
            symbol=request.symbol,
            timeframe=request.timeframe,
            exchange_code=request.exchange_code,
            status="completed",
            initial_capital=request.initial_capital,
            final_equity=request.initial_capital,
            started_at=started_at,
            completed_at=completed_at,
            params=request.model_dump(mode="json"),
            metrics=BacktestMetrics(),
            equity_curve=[],
            trades=[],
        )


class StopRequestedEngine:
    def run(self, *args: object, **kwargs: object) -> BacktestResponse:
        raise BacktestStopRequestedError("manual_stop_requested")


def _request() -> BacktestRequest:
    return BacktestRequest(
        strategy_code="fake_strategy",
        symbol="BTC-USDT",
        timeframe="5m",
        start_at=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc),
        end_at=datetime(2026, 3, 2, 0, 0, tzinfo=timezone.utc),
        initial_capital=Decimal("1000"),
        fee=Decimal("0"),
        slippage=Decimal("0"),
        position_size_pct=Decimal("1"),
        strategy_config_override={},
    )


def test_backtest_runner_marks_failed_when_engine_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    session_factory = SessionFactory()
    FakeBacktestRepository.reset()

    monkeypatch.setattr("app.services.backtest_runner_service.SessionLocal", session_factory)
    monkeypatch.setattr("app.services.backtest_runner_service.BacktestRepository", FakeBacktestRepository)
    monkeypatch.setattr("app.services.backtest_runner_service.CandleRepository", FakeCandleRepository)
    monkeypatch.setattr("app.services.backtest_runner_service.get_strategy", lambda _code: FakeStrategy())

    service = BacktestRunnerService(engine=RaisingEngine())

    with pytest.raises(RuntimeError, match="engine exploded"):
        service.run_backtest(_request())

    assert FakeBacktestRepository.current_run is not None
    assert FakeBacktestRepository.current_run.status == BacktestStatus.FAILED
    assert FakeBacktestRepository.current_run.error_text == "engine exploded"
    assert len(session_factory.sessions) == 3


@pytest.mark.parametrize(
    ("strategy_factory", "expected_status"),
    [
        (ArchivedFakeStrategy, "archived"),
        (PausedFakeStrategy, "paused"),
    ],
)
def test_backtest_runner_rejects_unavailable_strategy(
    monkeypatch: pytest.MonkeyPatch,
    strategy_factory: type[FakeStrategy],
    expected_status: str,
) -> None:
    session_factory = SessionFactory()
    FakeBacktestRepository.reset()

    monkeypatch.setattr("app.services.backtest_runner_service.SessionLocal", session_factory)
    monkeypatch.setattr("app.services.backtest_runner_service.BacktestRepository", FakeBacktestRepository)
    monkeypatch.setattr("app.services.backtest_runner_service.CandleRepository", FakeCandleRepository)
    monkeypatch.setattr(
        "app.services.backtest_runner_service.get_strategy",
        lambda _code: strategy_factory(),
    )

    service = BacktestRunnerService(engine=StaticReportEngine())

    with pytest.raises(ValueError, match=expected_status):
        service.run_backtest(_request())

    assert FakeBacktestRepository.current_run is None


def test_backtest_runner_marks_failed_when_result_persistence_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    session_factory = SessionFactory()
    FakeBacktestRepository.reset()
    FakeBacktestRepository.save_result_exception = RuntimeError("persist failed")

    monkeypatch.setattr("app.services.backtest_runner_service.SessionLocal", session_factory)
    monkeypatch.setattr("app.services.backtest_runner_service.BacktestRepository", FakeBacktestRepository)
    monkeypatch.setattr("app.services.backtest_runner_service.CandleRepository", FakeCandleRepository)
    monkeypatch.setattr("app.services.backtest_runner_service.get_strategy", lambda _code: FakeStrategy())

    service = BacktestRunnerService(engine=StaticReportEngine())

    with pytest.raises(RuntimeError, match="persist failed"):
        service.run_backtest(_request())

    assert FakeBacktestRepository.current_run is not None
    assert FakeBacktestRepository.current_run.status == BacktestStatus.FAILED
    assert FakeBacktestRepository.current_run.error_text == "persist failed"


def test_query_service_returns_null_completed_at_for_running_backtest() -> None:
    session = FakeSession()
    query_service = QueryService(session)  # type: ignore[arg-type]

    running_run = SimpleNamespace(
        id=8,
        status=BacktestStatus.RUNNING,
        started_at=datetime(2026, 3, 14, 12, 0, tzinfo=timezone.utc),
        completed_at=None,
        error_text=None,
        params_json={
            "symbol": "BTC-USDT",
            "timeframe": "5m",
            "exchange_code": "binance_us",
            "initial_capital": "1000",
        },
        created_at=datetime(2026, 3, 14, 11, 59, tzinfo=timezone.utc),
    )
    strategy = SimpleNamespace(code="breakout_retest", name="BreakoutRetest")

    class FakeQueryBacktestRepository:
        def recover_stale_runs(self, stale_before: datetime) -> list[dict[str, object]]:
            return []

        def get_run_with_result(self, run_id: int) -> tuple[SimpleNamespace, SimpleNamespace, None]:
            assert run_id == 8
            return running_run, strategy, None

    query_service.backtest_repository = FakeQueryBacktestRepository()  # type: ignore[assignment]

    response = query_service.get_backtest(8)

    assert response.status == "running"
    assert response.completed_at is None


def test_query_service_does_not_trigger_stale_recovery_when_listing_backtests() -> None:
    session = FakeSession()
    query_service = QueryService(session)  # type: ignore[arg-type]

    run = SimpleNamespace(
        id=11,
        status=BacktestStatus.RUNNING,
        started_at=datetime(2026, 3, 14, 12, 0, tzinfo=timezone.utc),
        completed_at=None,
        error_text=None,
        params_json={
            "symbol": "BTC-USDT",
            "timeframe": "5m",
            "exchange_code": "binance_us",
            "initial_capital": "1000",
        },
        created_at=datetime(2026, 3, 14, 11, 59, tzinfo=timezone.utc),
    )
    strategy = SimpleNamespace(code="breakout_continuation", name="BreakoutContinuation")

    class FakeQueryBacktestRepository:
        def recover_stale_runs(self, stale_before: datetime) -> list[dict[str, object]]:
            raise AssertionError("query path should not attempt stale recovery")

        def list_runs(
            self,
            limit: int = 100,
            status: BacktestStatus | None = None,
            strategy_code: str | None = None,
        ) -> list[tuple[SimpleNamespace, SimpleNamespace, None]]:
            assert limit == 100
            assert status is None
            assert strategy_code is None
            return [(run, strategy, None)]

    query_service.backtest_repository = FakeQueryBacktestRepository()  # type: ignore[assignment]

    rows = query_service.list_backtests()

    assert len(rows) == 1
    assert rows[0].id == 11
    assert rows[0].status == "running"


def test_query_service_returns_backtest_diagnostics_from_summary() -> None:
    session = FakeSession()
    query_service = QueryService(session)  # type: ignore[arg-type]

    completed_run = SimpleNamespace(
        id=10,
        status=BacktestStatus.COMPLETED,
        started_at=datetime(2026, 3, 14, 12, 0, tzinfo=timezone.utc),
        completed_at=datetime(2026, 3, 14, 12, 5, tzinfo=timezone.utc),
        error_text=None,
        params_json={
            "symbol": "BTC-USDT",
            "timeframe": "5m",
            "exchange_code": "binance_us",
            "initial_capital": "1000",
        },
        created_at=datetime(2026, 3, 14, 11, 59, tzinfo=timezone.utc),
    )
    strategy = SimpleNamespace(code="breakout_retest", name="BreakoutRetest")
    result = SimpleNamespace(
        total_return_pct=Decimal("0"),
        max_drawdown_pct=Decimal("0"),
        win_rate_pct=Decimal("0"),
        profit_factor=Decimal("0"),
        expectancy=Decimal("0"),
        total_trades=0,
        avg_winner=Decimal("0"),
        avg_loser=Decimal("0"),
        equity_curve_json=[],
        summary_json={
            "symbol": "BTC-USDT",
            "timeframe": "5m",
            "exchange_code": "binance_us",
            "initial_capital": "1000",
            "final_equity": "1000",
            "metrics": {"gross_expectancy": "0", "net_expectancy": "0"},
            "trades": [],
            "diagnostics": {
                "entry_hold_reasons": {"regime_blocked": 7},
                "entry_hold_reason_details": {"ema200_slope_below_threshold": 7},
                "entry_hold_total": 7,
            },
        },
    )

    class FakeQueryBacktestRepository:
        def recover_stale_runs(self, stale_before: datetime) -> list[dict[str, object]]:
            return []

        def get_run_with_result(
            self, run_id: int
        ) -> tuple[SimpleNamespace, SimpleNamespace, SimpleNamespace]:
            assert run_id == 10
            return completed_run, strategy, result

    query_service.backtest_repository = FakeQueryBacktestRepository()  # type: ignore[assignment]

    response = query_service.get_backtest(10)

    assert response.status == "completed"
    assert response.diagnostics == {
        "entry_hold_reasons": {"regime_blocked": 7},
        "entry_hold_reason_details": {"ema200_slope_below_threshold": 7},
        "entry_hold_total": 7,
    }


def test_backtest_runner_recovers_stale_runs_before_starting_new_one(monkeypatch: pytest.MonkeyPatch) -> None:
    session_factory = SessionFactory()
    FakeBacktestRepository.reset()
    FakeBacktestRepository.recovered_runs = [{"run_id": 5, "status": "failed", "reason": "stale_run_missing_result"}]

    monkeypatch.setattr("app.services.backtest_runner_service.SessionLocal", session_factory)
    monkeypatch.setattr("app.services.backtest_runner_service.BacktestRepository", FakeBacktestRepository)
    monkeypatch.setattr("app.services.backtest_runner_service.CandleRepository", FakeCandleRepository)
    monkeypatch.setattr("app.services.backtest_runner_service.get_strategy", lambda _code: FakeStrategy())

    service = BacktestRunnerService(engine=StaticReportEngine())
    report = service.run_backtest(_request())

    assert report.status == "completed"
    assert FakeBacktestRepository.recover_calls >= 1
    assert session_factory.sessions[0].commits == 1


def test_backtest_runner_returns_failed_response_when_stop_is_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    session_factory = SessionFactory()
    FakeBacktestRepository.reset()

    monkeypatch.setattr("app.services.backtest_runner_service.SessionLocal", session_factory)
    monkeypatch.setattr("app.services.backtest_runner_service.BacktestRepository", FakeBacktestRepository)
    monkeypatch.setattr("app.services.backtest_runner_service.CandleRepository", FakeCandleRepository)
    monkeypatch.setattr("app.services.backtest_runner_service.get_strategy", lambda _code: FakeStrategy())

    service = BacktestRunnerService(engine=StopRequestedEngine())
    monkeypatch.setattr(
        service,
        "_load_backtest_response",
        lambda run_id: BacktestResponse(
            run_id=run_id,
            strategy_code="fake_strategy",
            symbol="BTC-USDT",
            timeframe="5m",
            exchange_code="binance_us",
            status="failed",
            initial_capital=Decimal("1000"),
            final_equity=Decimal("1000"),
            started_at=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc),
            completed_at=datetime(2026, 3, 1, 0, 1, tzinfo=timezone.utc),
            params={},
            metrics=BacktestMetrics(),
            equity_curve=[],
            trades=[],
            error_text="manual_stop_requested",
        ),
    )

    report = service.run_backtest(_request())

    assert report.status == "failed"
    assert report.error_text == "manual_stop_requested"
    assert FakeBacktestRepository.current_run is not None
    assert FakeBacktestRepository.current_run.status == BacktestStatus.FAILED


def test_backtest_runner_stop_backtest_marks_running_run_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    session_factory = SessionFactory()
    FakeBacktestRepository.reset()
    FakeBacktestRepository.current_run = SimpleNamespace(
        id=9,
        strategy_id=1,
        params_json={"symbol": "BTC-USDT", "timeframe": "5m"},
        status=BacktestStatus.RUNNING,
        started_at=datetime(2026, 3, 14, 15, 0, tzinfo=timezone.utc),
        completed_at=None,
        error_text=None,
        created_at=datetime(2026, 3, 14, 15, 0, tzinfo=timezone.utc),
    )

    monkeypatch.setattr("app.services.backtest_runner_service.SessionLocal", session_factory)
    monkeypatch.setattr("app.services.backtest_runner_service.BacktestRepository", FakeBacktestRepository)

    service = BacktestRunnerService(engine=StaticReportEngine())
    monkeypatch.setattr(
        service,
        "_load_backtest_response",
        lambda run_id: BacktestResponse(
            run_id=run_id,
            strategy_code="fake_strategy",
            symbol="BTC-USDT",
            timeframe="5m",
            exchange_code="binance_us",
            status="failed",
            initial_capital=Decimal("1000"),
            final_equity=Decimal("1000"),
            started_at=datetime(2026, 3, 14, 15, 0, tzinfo=timezone.utc),
            completed_at=datetime(2026, 3, 14, 15, 1, tzinfo=timezone.utc),
            params={},
            metrics=BacktestMetrics(),
            equity_curve=[],
            trades=[],
            error_text="manual_stop:manual_stop",
        ),
    )

    report = service.stop_backtest(9, "manual_stop")

    assert report.status == "failed"
    assert FakeBacktestRepository.current_run.status == BacktestStatus.FAILED
    assert FakeBacktestRepository.current_run.error_text == "manual_stop:manual_stop"
