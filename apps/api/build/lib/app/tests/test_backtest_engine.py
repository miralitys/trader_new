from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.domain.enums import SignalAction
from app.engines.backtest_engine import BacktestEngine
from app.strategies.base import BaseStrategy
from app.strategies.types import BaseStrategyConfig, CandleInput, SignalDecision, StrategyContext


class TestConfig(BaseStrategyConfig):
    warmup_candles: int = 2


class TestStrategy(BaseStrategy):
    key = "test"
    name = "Test"
    description = "test"
    config_model = TestConfig

    def generate_signal(self, candles: list[CandleInput], context: StrategyContext) -> SignalDecision:
        if not context.has_open_position and len(candles) == 2:
            return SignalDecision(action=SignalAction.ENTER, reason="enter")
        if context.has_open_position and len(candles) == 4:
            return SignalDecision(action=SignalAction.EXIT, reason="exit")
        return SignalDecision(action=SignalAction.HOLD, reason="hold")


def test_backtest_engine_generates_trade() -> None:
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    candles = [
        CandleInput(start + timedelta(minutes=5 * index), start + timedelta(minutes=5 * (index + 1)), 100 + index, 101 + index, 99 + index, 100 + index, 10)
        for index in range(5)
    ]
    result = BacktestEngine().run(
        strategy=TestStrategy(),
        config=TestConfig(position_size_pct=0.5, fee_bps=0, slippage_bps=0).model_dump(),
        candles=candles,
        symbol="BTC-USD",
        timeframe="5m",
        initial_capital=1000,
    )

    assert result["summary"]["total_trades"] == 1
    assert len(result["trades"]) == 1
    assert result["summary"]["ending_equity"] > 1000
