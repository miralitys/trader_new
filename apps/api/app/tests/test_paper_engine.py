from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from app.engines.paper_engine import PaperEngine, PaperRuntimeState
from app.schemas.backtest import BacktestCandle
from app.strategies.base import BaseStrategy, BaseStrategyConfig, StrategyContext, StrategySignal


class HoldPaperStrategy(BaseStrategy):
    key = "hold_paper_strategy"
    name = "HoldPaperStrategy"
    description = "No-op paper strategy."
    config_model = BaseStrategyConfig

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        return StrategySignal(action="hold", reason="hold")


class EnterThenExitPaperStrategy(BaseStrategy):
    key = "enter_then_exit_paper_strategy"
    name = "EnterThenExitPaperStrategy"
    description = "Open on first candle and close on second."
    config_model = BaseStrategyConfig

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        history = context.metadata["history"]
        has_position = context.metadata["has_position"]
        if len(history) == 1 and not has_position:
            return StrategySignal(action="enter", reason="entry")
        if len(history) == 2 and has_position:
            return StrategySignal(action="exit", reason="exit")
        return StrategySignal(action="hold", reason="hold")


class DynamicStopPaperStrategy(BaseStrategy):
    key = "dynamic_stop_paper_strategy"
    name = "DynamicStopPaperStrategy"
    description = "Opens once with a metadata-defined stop."
    config_model = BaseStrategyConfig

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        history = context.metadata["history"]
        has_position = context.metadata["has_position"]
        if len(history) == 1 and not has_position:
            return StrategySignal(
                action="enter",
                reason="entry",
                metadata={"stop_price": "95", "take_profit_price": "108"},
            )
        return StrategySignal(action="hold", reason="hold")


def _candle(ts: datetime, price: str) -> BacktestCandle:
    decimal_price = Decimal(price)
    return BacktestCandle(
        open_time=ts,
        open=decimal_price,
        high=decimal_price,
        low=decimal_price,
        close=decimal_price,
        volume=Decimal("1"),
    )


def test_paper_engine_opens_position() -> None:
    engine = PaperEngine()
    state = PaperRuntimeState(cash=Decimal("1000"), position=None)
    candle = _candle(datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc), "100")

    result = engine.process_candle(
        strategy=EnterThenExitPaperStrategy(),
        symbol="BTC-USD",
        timeframe="5m",
        candle=candle,
        history=[candle],
        state=state,
        fee_rate=Decimal("0"),
        slippage_rate=Decimal("0"),
        strategy_config_override={"position_size_pct": 1, "stop_loss_pct": 10, "take_profit_pct": 10},
    )

    assert result.state.position is not None
    assert result.state.position.qty == Decimal("10")
    assert result.state.cash == Decimal("0")
    assert len(result.orders) == 1
    assert result.trade_event is None


def test_paper_engine_closes_position() -> None:
    engine = PaperEngine()
    candles = [
        _candle(datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc), "100"),
        _candle(datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc), "110"),
    ]

    final_state, results = engine.process_candle_batch(
        strategy=EnterThenExitPaperStrategy(),
        symbol="BTC-USD",
        timeframe="5m",
        candles=candles,
        state=PaperRuntimeState(cash=Decimal("1000"), position=None),
        fee_rate=Decimal("0"),
        slippage_rate=Decimal("0"),
        strategy_config_override={"position_size_pct": 1, "stop_loss_pct": 10, "take_profit_pct": 10},
    )

    assert final_state.position is None
    assert final_state.cash == Decimal("1100")
    assert results[-1].trade_event is not None
    assert results[-1].trade_event.pnl == Decimal("100")


def test_paper_engine_handles_no_signal_case() -> None:
    engine = PaperEngine()
    candle = _candle(datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc), "100")

    result = engine.process_candle(
        strategy=HoldPaperStrategy(),
        symbol="BTC-USD",
        timeframe="5m",
        candle=candle,
        history=[candle],
        state=PaperRuntimeState(cash=Decimal("1000"), position=None),
        fee_rate=Decimal("0"),
        slippage_rate=Decimal("0"),
        strategy_config_override={},
    )

    assert result.state.cash == Decimal("1000")
    assert result.state.position is None
    assert result.orders == []
    assert result.trade_event is None
    assert result.signal_event is None


def test_paper_engine_updates_account_balance_with_fees_and_profit() -> None:
    engine = PaperEngine()
    candles = [
        _candle(datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc), "100"),
        _candle(datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc), "120"),
    ]

    final_state, results = engine.process_candle_batch(
        strategy=EnterThenExitPaperStrategy(),
        symbol="BTC-USD",
        timeframe="5m",
        candles=candles,
        state=PaperRuntimeState(cash=Decimal("1000"), position=None),
        fee_rate=Decimal("0.01"),
        slippage_rate=Decimal("0"),
        strategy_config_override={"position_size_pct": 1, "stop_loss_pct": 10, "take_profit_pct": 10},
    )

    assert final_state.position is None
    assert final_state.cash > Decimal("1000")
    assert results[-1].trade_event is not None
    assert results[-1].trade_event.fees > Decimal("0")


def test_paper_engine_honors_dynamic_stop_from_signal_metadata() -> None:
    engine = PaperEngine()
    candles = [
        _candle(datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc), "100"),
        BacktestCandle(
            open_time=datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc),
            open=Decimal("99"),
            high=Decimal("101"),
            low=Decimal("94"),
            close=Decimal("96"),
            volume=Decimal("1"),
        ),
    ]

    final_state, results = engine.process_candle_batch(
        strategy=DynamicStopPaperStrategy(),
        symbol="BTC-USD",
        timeframe="5m",
        candles=candles,
        state=PaperRuntimeState(cash=Decimal("1000"), position=None),
        fee_rate=Decimal("0"),
        slippage_rate=Decimal("0"),
        strategy_config_override={"position_size_pct": 1, "stop_loss_pct": 10, "take_profit_pct": 10},
    )

    assert final_state.position is None
    assert final_state.cash == Decimal("950")
    assert results[-1].trade_event is not None
    assert results[-1].trade_event.exit_price == Decimal("95")
