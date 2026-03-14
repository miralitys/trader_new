from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.engines.paper_engine import PaperCandleResult, PaperRuntimeState
from app.schemas.backtest import BacktestCandle
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

    def rollback(self) -> None:
        return None

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

    def default_config(self) -> dict[str, object]:
        return {}

    def parse_config(self, payload: dict[str, object]):
        return SimpleNamespace(model_dump=lambda: payload)


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


def test_process_run_loads_warmup_history_for_paper_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = DummySession()
    watermark_time = datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc)
    captured_history_lengths: list[int] = []

    class FakeStrategy:
        key = "mean_reversion_hard_stop"
        name = "MeanReversionHardStop"
        description = "Strategy with warmup requirement."

        def default_config(self) -> dict[str, object]:
            return {}

        def parse_config(self, payload: dict[str, object]):
            return SimpleNamespace(model_dump=lambda: {})

        def required_history_bars(self, timeframe: str, strategy_config) -> int:
            assert timeframe == "5m"
            return 3

    class FakeEngine:
        def process_candle(
            self,
            strategy,
            symbol: str,
            timeframe: str,
            candle: BacktestCandle,
            history: list[BacktestCandle],
            state: PaperRuntimeState,
            fee_rate: Decimal,
            slippage_rate: Decimal,
            strategy_config_override: dict[str, object],
            runtime_metadata: dict[str, object],
        ) -> PaperCandleResult:
            captured_history_lengths.append(len(history))
            return PaperCandleResult(
                state=state,
                signal_event=None,
                orders=[],
                trade_event=None,
            )

    class FakeCandleRepository:
        def __init__(self, current_session: DummySession) -> None:
            assert current_session is session

        def list_recent_candles(
            self,
            exchange_code: str,
            symbol_code: str,
            timeframe: str,
            end_at: datetime,
            limit: int,
        ):
            assert exchange_code == "coinbase"
            assert symbol_code == "BTC-USD"
            assert timeframe == "5m"
            assert end_at == watermark_time
            assert limit == 3
            return [
                BacktestCandle(
                    open_time=datetime(2026, 1, 1, 0, 45, tzinfo=timezone.utc),
                    open=Decimal("100"),
                    high=Decimal("101"),
                    low=Decimal("99"),
                    close=Decimal("100"),
                    volume=Decimal("1"),
                ),
                BacktestCandle(
                    open_time=datetime(2026, 1, 1, 0, 50, tzinfo=timezone.utc),
                    open=Decimal("101"),
                    high=Decimal("102"),
                    low=Decimal("100"),
                    close=Decimal("101"),
                    volume=Decimal("1"),
                ),
                BacktestCandle(
                    open_time=datetime(2026, 1, 1, 0, 55, tzinfo=timezone.utc),
                    open=Decimal("102"),
                    high=Decimal("103"),
                    low=Decimal("101"),
                    close=Decimal("102"),
                    volume=Decimal("1"),
                ),
            ]

        def list_candles_after(
            self,
            exchange_code: str,
            symbol_code: str,
            timeframe: str,
            after_time: datetime,
            limit: int,
        ):
            assert after_time == watermark_time
            assert limit == 1
            return [
                BacktestCandle(
                    open_time=datetime(2026, 1, 1, 1, 5, tzinfo=timezone.utc),
                    open=Decimal("103"),
                    high=Decimal("104"),
                    low=Decimal("102"),
                    close=Decimal("103"),
                    volume=Decimal("1"),
                )
            ]

    class FakeStrategyRunRepository:
        def __init__(self, current_session: DummySession) -> None:
            assert current_session is session

        def get_by_id(self, run_id: int):
            return SimpleNamespace(
                id=run_id,
                strategy_id=7,
                status=SimpleNamespace(value="running"),
                symbols_json=["BTC-USD"],
                timeframes_json=["5m"],
                metadata_json={
                        "exchange_code": "coinbase",
                        "fee": "0",
                        "slippage": "0",
                        "strategy_config_override": {},
                        "last_processed_by_stream": {"BTC-USD|5m": watermark_time.isoformat()},
                        "open_positions_runtime": {},
                    },
                last_processed_candle_at=None,
            )

        def get_strategy_by_id(self, strategy_id: int):
            return SimpleNamespace(id=strategy_id, code="mean_reversion_hard_stop")

        def update_last_processed(self, run, candle_time, stream_key: str) -> None:
            run.last_processed_candle_at = candle_time

        def store_open_position_runtime(self, run, symbol: str, runtime_payload: dict[str, object]) -> None:
            return None

        def clear_open_position_runtime(self, run, symbol: str) -> None:
            return None

        def mark_failed(self, run, stopped_at, error_text: str) -> None:
            run.status = SimpleNamespace(value="failed")

    class FakeSignalRepository:
        def __init__(self, current_session: DummySession) -> None:
            assert current_session is session

        def create_signal(self, *args, **kwargs):
            return SimpleNamespace(id=1)

    class FakeOrderRepository:
        def __init__(self, current_session: DummySession) -> None:
            assert current_session is session

        def create_filled_order(self, *args, **kwargs) -> None:
            return None

    class FakePositionRepository:
        def __init__(self, current_session: DummySession) -> None:
            assert current_session is session

        def list_open_positions(self, run_id: int):
            return []

        def open_position(self, *args, **kwargs):
            return SimpleNamespace()

        def close_position(self, *args, **kwargs) -> None:
            return None

    class FakeTradeRepository:
        def __init__(self, current_session: DummySession) -> None:
            assert current_session is session

        def create_trade(self, *args, **kwargs) -> None:
            return None

    class FakePaperAccountRepository:
        def __init__(self, current_session: DummySession) -> None:
            assert current_session is session

        def ensure_account(self, strategy_id: int, balance: Decimal):
            return SimpleNamespace(balance=Decimal("10000"), currency="USD")

        def update_balance(self, account, balance: Decimal) -> None:
            account.balance = balance

    monkeypatch.setattr("app.services.paper_execution_service.SessionLocal", lambda: session)
    monkeypatch.setattr("app.services.paper_execution_service.CandleRepository", FakeCandleRepository)
    monkeypatch.setattr("app.services.paper_execution_service.StrategyRunRepository", FakeStrategyRunRepository)
    monkeypatch.setattr("app.services.paper_execution_service.SignalRepository", FakeSignalRepository)
    monkeypatch.setattr("app.services.paper_execution_service.OrderRepository", FakeOrderRepository)
    monkeypatch.setattr("app.services.paper_execution_service.PositionRepository", FakePositionRepository)
    monkeypatch.setattr("app.services.paper_execution_service.TradeRepository", FakeTradeRepository)
    monkeypatch.setattr("app.services.paper_execution_service.PaperAccountRepository", FakePaperAccountRepository)
    monkeypatch.setattr("app.services.paper_execution_service.get_strategy", lambda code: FakeStrategy())

    service = PaperExecutionService(engine=FakeEngine())
    response = service.process_run(run_id=42, max_candles_per_stream=1)

    assert response.processed_candles == 1
    assert captured_history_lengths == [4]
