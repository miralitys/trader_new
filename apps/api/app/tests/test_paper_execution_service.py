from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.models.enums import StrategyRunStatus
from app.repositories.paper_account_repository import PaperAccountRepository
from app.schemas.paper import PaperRunResponse, PaperRunStartRequest
from app.services.paper_execution_service import PaperExecutionService


class DummySession:
    def __init__(self) -> None:
        self.added: list[object] = []
        self.flushed = False
        self.closed = False
        self.commits = 0

    def add(self, obj: object) -> None:
        self.added.append(obj)

    def flush(self) -> None:
        self.flushed = True

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        self.closed = True


def test_ensure_account_resets_existing_account_when_requested() -> None:
    session = DummySession()
    repository = PaperAccountRepository(session)  # type: ignore[arg-type]
    account = SimpleNamespace(balance=Decimal("2500"), currency="EUR")
    repository.get_by_strategy_id = lambda strategy_id: account  # type: ignore[method-assign]

    result = repository.ensure_account(
        strategy_id=1,
        balance=Decimal("10000"),
        currency="USD",
        reset_existing=True,
    )

    assert result is account
    assert account.balance == Decimal("10000")
    assert account.currency == "USD"
    assert session.flushed is True


@dataclass
class FakeStrategy:
    key: str
    name: str
    description: str


def test_start_run_requests_account_reset(monkeypatch: pytest.MonkeyPatch) -> None:
    session = DummySession()
    ensure_account_calls: list[dict[str, object]] = []

    class FakeStrategyRunRepository:
        def __init__(self, current_session: DummySession) -> None:
            assert current_session is session

        def ensure_strategy(self, code: str, name: str, description: str):
            return SimpleNamespace(id=7, code=code, name=name, description=description)

        def get_active_paper_run_for_strategy(self, strategy_id: int):
            assert strategy_id == 7
            return None

        def create_paper_run(
            self,
            strategy_id: int,
            symbols: list[str],
            timeframes: list[str],
            metadata_json: dict[str, object],
        ):
            return SimpleNamespace(
                id=11,
                strategy_id=strategy_id,
                status=StrategyRunStatus.CREATED,
                symbols_json=symbols,
                timeframes_json=timeframes,
                metadata_json=metadata_json,
                last_processed_candle_at=None,
            )

        def mark_running(self, run, started_at):
            run.status = StrategyRunStatus.RUNNING
            run.started_at = started_at
            return run

    class FakePaperAccountRepository:
        def __init__(self, current_session: DummySession) -> None:
            assert current_session is session

        def ensure_account(
            self,
            strategy_id: int,
            balance: Decimal,
            currency: str = "USD",
            reset_existing: bool = False,
        ):
            ensure_account_calls.append(
                {
                    "strategy_id": strategy_id,
                    "balance": balance,
                    "currency": currency,
                    "reset_existing": reset_existing,
                }
            )
            return SimpleNamespace(balance=balance, currency=currency)

    monkeypatch.setattr("app.services.paper_execution_service.SessionLocal", lambda: session)
    monkeypatch.setattr(
        "app.services.paper_execution_service.StrategyRunRepository",
        FakeStrategyRunRepository,
    )
    monkeypatch.setattr(
        "app.services.paper_execution_service.PaperAccountRepository",
        FakePaperAccountRepository,
    )
    monkeypatch.setattr(
        "app.services.paper_execution_service.get_strategy",
        lambda code: FakeStrategy(code, "TestStrategy", "Test strategy"),
    )

    service = PaperExecutionService()
    response = service.start_run(
        PaperRunStartRequest(
            strategy_code="breakout_retest",
            symbols=["BTC-USD"],
            timeframes=["5m"],
            initial_balance=Decimal("15000"),
            currency="USD",
        )
    )

    assert response.run_id == 11
    assert ensure_account_calls == [
        {
            "strategy_id": 7,
            "balance": Decimal("15000"),
            "currency": "USD",
            "reset_existing": True,
        }
    ]
    assert session.commits == 1
    assert session.closed is True


def test_process_active_runs_continues_after_single_run_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = DummySession()

    class FakeStrategyRunRepository:
        def __init__(self, current_session: DummySession) -> None:
            assert current_session is session

        def list_active_paper_runs(self):
            return [SimpleNamespace(id=1), SimpleNamespace(id=2)]

    def fake_process_run(self, run_id: int, max_candles_per_stream: int = 100) -> PaperRunResponse:
        assert max_candles_per_stream == 25
        if run_id == 1:
            raise ValueError("synthetic failure")
        return PaperRunResponse(
            run_id=run_id,
            strategy_code="breakout_retest",
            status="running",
            symbols=["BTC-USD"],
            timeframes=["5m"],
            exchange_code="coinbase",
            account_balance=Decimal("10000"),
            currency="USD",
        )

    monkeypatch.setattr("app.services.paper_execution_service.SessionLocal", lambda: session)
    monkeypatch.setattr(
        "app.services.paper_execution_service.StrategyRunRepository",
        FakeStrategyRunRepository,
    )
    monkeypatch.setattr(PaperExecutionService, "process_run", fake_process_run)

    service = PaperExecutionService()
    results = service.process_active_runs(max_candles_per_stream=25)

    assert [result.run_id for result in results] == [2]
    assert session.closed is True
