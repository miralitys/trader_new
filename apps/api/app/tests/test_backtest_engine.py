from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from app.engines.backtest_engine import BacktestEngine, BacktestStopRequestedError
from app.schemas.backtest import BacktestCandle, BacktestRequest
from app.strategies.base import BaseStrategy, BaseStrategyConfig, StrategyContext, StrategySignal


class HoldStrategy(BaseStrategy):
    key = "hold_strategy"
    name = "HoldStrategy"
    description = "No-trades strategy for tests."
    config_model = BaseStrategyConfig

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        return StrategySignal(action="hold", reason="hold")


class SingleTradeProfitStrategy(BaseStrategy):
    key = "single_trade_profit_strategy"
    name = "SingleTradeProfitStrategy"
    description = "Enter once and exit later for a profit."
    config_model = BaseStrategyConfig

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        index = context.metadata["bar_index"]
        has_position = context.metadata["has_position"]
        if index == 0 and not has_position:
            return StrategySignal(action="enter", reason="initial_entry")
        if index == 2 and has_position:
            return StrategySignal(action="exit", reason="planned_exit")
        return StrategySignal(action="hold", reason="hold")


class TwoTradeMixedStrategy(BaseStrategy):
    key = "two_trade_mixed_strategy"
    name = "TwoTradeMixedStrategy"
    description = "Produces one winning and one losing trade."
    config_model = BaseStrategyConfig

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        index = context.metadata["bar_index"]
        has_position = context.metadata["has_position"]
        if index in {0, 2} and not has_position:
            return StrategySignal(action="enter", reason=f"entry_{index}")
        if index in {1, 3} and has_position:
            return StrategySignal(action="exit", reason=f"exit_{index}")
        return StrategySignal(action="hold", reason="hold")


class DynamicStopStrategy(BaseStrategy):
    key = "dynamic_stop_strategy"
    name = "DynamicStopStrategy"
    description = "Uses signal metadata to place a structure stop."
    config_model = BaseStrategyConfig

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        index = context.metadata["bar_index"]
        has_position = context.metadata["has_position"]
        if index == 0 and not has_position:
            return StrategySignal(
                action="enter",
                reason="dynamic_entry",
                metadata={"stop_price": "95", "take_profit_price": "108"},
            )
        return StrategySignal(action="hold", reason="hold")


class WindowTrackingStrategy(BaseStrategy):
    key = "window_tracking_strategy"
    name = "WindowTrackingStrategy"
    description = "Captures runtime history window sizes."
    config_model = BaseStrategyConfig

    def __init__(self) -> None:
        self.max_history_seen = 0
        self.max_one_hour_history_seen = 0

    def required_history_bars(self, timeframe: str, config: BaseStrategyConfig | None = None) -> int:
        return 5

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        history = context.metadata["history"]
        one_hour_history = context.metadata["one_hour_history"]
        self.max_history_seen = max(self.max_history_seen, len(history))
        self.max_one_hour_history_seen = max(self.max_one_hour_history_seen, len(one_hour_history))
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


def _request(strategy_code: str) -> BacktestRequest:
    return BacktestRequest(
        strategy_code=strategy_code,
        symbol="BTC-USD",
        timeframe="5m",
        start_at=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
        end_at=datetime(2026, 1, 1, 0, 20, tzinfo=timezone.utc),
        initial_capital=Decimal("1000"),
        fee=Decimal("0"),
        slippage=Decimal("0"),
        position_size_pct=Decimal("1"),
        strategy_config_override={
            "stop_loss_pct": 10,
            "take_profit_pct": 10,
        },
    )


def test_backtest_engine_handles_profitable_sequence() -> None:
    engine = BacktestEngine()
    candles = [
        _candle(datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc), "100"),
        _candle(datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc), "110"),
        _candle(datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc), "120"),
    ]

    report = engine.run(
        request=_request("single_trade_profit_strategy"),
        strategy=SingleTradeProfitStrategy(),
        candles=candles,
    )

    assert report.metrics.total_trades == 1
    assert report.trades[0].pnl == Decimal("200")
    assert report.final_equity == Decimal("1200")
    assert report.metrics.total_return_pct == Decimal("20")


def test_backtest_engine_handles_no_trades_case() -> None:
    engine = BacktestEngine()
    candles = [
        _candle(datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc), "100"),
        _candle(datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc), "101"),
        _candle(datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc), "102"),
    ]

    report = engine.run(
        request=_request("hold_strategy"),
        strategy=HoldStrategy(),
        candles=candles,
    )

    assert report.metrics.total_trades == 0
    assert report.final_equity == Decimal("1000")
    assert report.metrics.total_return_pct == Decimal("0")
    assert report.trades == []


def test_backtest_engine_calculates_metrics_for_mixed_results() -> None:
    engine = BacktestEngine()
    candles = [
        _candle(datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc), "100"),
        _candle(datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc), "110"),
        _candle(datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc), "100"),
        _candle(datetime(2026, 1, 1, 0, 15, tzinfo=timezone.utc), "90"),
    ]

    report = engine.run(
        request=_request("two_trade_mixed_strategy"),
        strategy=TwoTradeMixedStrategy(),
        candles=candles,
    )

    assert report.metrics.total_trades == 2
    assert report.metrics.win_rate_pct == Decimal("50")
    assert report.metrics.avg_winner == Decimal("100")
    assert report.metrics.avg_loser == Decimal("-110")
    assert report.metrics.expectancy == Decimal("-5")
    assert report.metrics.total_return_pct == Decimal("-1")


def test_backtest_engine_honors_dynamic_stop_from_signal_metadata() -> None:
    engine = BacktestEngine()
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

    report = engine.run(
        request=_request("dynamic_stop_strategy"),
        strategy=DynamicStopStrategy(),
        candles=candles,
    )

    assert report.metrics.total_trades == 1
    assert report.trades[0].exit_reason == "stop_loss"
    assert report.trades[0].exit_price == Decimal("95")


def test_backtest_engine_limits_history_window_for_strategies_with_declared_requirements() -> None:
    engine = BacktestEngine()
    strategy = WindowTrackingStrategy()
    candles = [
        _candle(datetime(2026, 1, 1, 0, index * 5, tzinfo=timezone.utc), str(100 + index))
        for index in range(12)
    ]

    report = engine.run(
        request=_request(strategy.key),
        strategy=strategy,
        candles=candles,
    )

    assert report.metrics.total_trades == 0
    assert strategy.max_history_seen <= 5
    assert strategy.max_one_hour_history_seen <= 1


def test_backtest_engine_emits_progress_markers() -> None:
    engine = BacktestEngine()
    candles = [
        _candle(datetime(2026, 1, 1, 0, index * 5, tzinfo=timezone.utc), str(100 + index))
        for index in range(5)
    ]
    progress: list[tuple[int, int]] = []

    engine.run(
        request=_request("hold_strategy"),
        strategy=HoldStrategy(),
        candles=candles,
        progress_interval_bars=2,
        progress_callback=lambda processed, total, _candle_time: progress.append((processed, total)),
    )

    assert progress == [(2, 5), (4, 5), (5, 5)]


def test_backtest_engine_stops_when_abort_callback_requests_it() -> None:
    engine = BacktestEngine()
    candles = [
        _candle(datetime(2026, 1, 1, 0, index * 5, tzinfo=timezone.utc), str(100 + index))
        for index in range(5)
    ]

    with pytest.raises(BacktestStopRequestedError, match="manual_stop_requested"):
        engine.run(
            request=_request("hold_strategy"),
            strategy=HoldStrategy(),
            candles=candles,
            stop_check_interval_bars=2,
            should_abort=lambda processed, _total, _candle_time: processed >= 2,
        )
