from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.engines.backtest_engine import BacktestEngine, BacktestStopRequestedError
from app.schemas.backtest import BacktestCandle, BacktestRequest
from app.strategies.breakout_retest import BreakoutRetestStrategy
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


class DiagnosticHoldStrategy(BaseStrategy):
    key = "diagnostic_hold_strategy"
    name = "DiagnosticHoldStrategy"
    description = "Emits hold reasons for diagnostics aggregation tests."
    config_model = BaseStrategyConfig

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        index = context.metadata["bar_index"]
        if index == 0:
            return StrategySignal(action="hold", reason="insufficient_history")
        if index == 1:
            return StrategySignal(
                action="hold",
                reason="regime_blocked",
                metadata={"skip_reason_detail": "close_below_ema200_1h"},
            )
        if index == 2:
            return StrategySignal(
                action="hold",
                reason="safety_guard_failed",
                metadata={"skip_reason_detail": "flush_not_deep_enough"},
            )
        if index == 3:
            return StrategySignal(
                action="hold",
                reason="safety_guard_failed",
                metadata={"skip_reason_detail": "late_rebound_entry"},
            )
        if index == 4:
            return StrategySignal(
                action="hold",
                reason="safety_guard_failed",
                metadata={"skip_reason_detail": "flush_low_too_old"},
            )
        if index == 5:
            return StrategySignal(
                action="hold",
                reason="safety_guard_failed",
                metadata={"skip_reason_detail": "context_not_reset"},
            )
        if index == 6:
            return StrategySignal(
                action="hold",
                reason="safety_guard_failed",
                metadata={"skip_reason_detail": "no_reclaim_close"},
            )
        if index == 7:
            return StrategySignal(
                action="hold",
                reason="safety_guard_failed",
                metadata={"skip_reason_detail": "entry_candle_not_green"},
            )
        if index == 8:
            return StrategySignal(
                action="hold",
                reason="safety_guard_failed",
                metadata={"skip_reason_detail": "entry_bar_not_strong_enough"},
            )
        if index == 9:
            return StrategySignal(
                action="hold",
                reason="safety_guard_failed",
                metadata={"skip_reason_detail": "entry_bar_overextended"},
            )
        if index == 10:
            return StrategySignal(action="hold", reason="insufficient_tp_vs_cost")
        if index == 11:
            return StrategySignal(action="hold", reason="max_stop_exceeded")
        if index == 12:
            return StrategySignal(action="hold", reason="range_not_tight_enough")
        if index == 13:
            return StrategySignal(action="hold", reason="breakout_not_confirmed")
        if index == 14:
            return StrategySignal(action="hold", reason="breakout_bar_not_green")
        if index == 15:
            return StrategySignal(action="hold", reason="breakout_bar_too_weak")
        if index == 16:
            return StrategySignal(action="hold", reason="breakout_bar_too_extended")
        return StrategySignal(action="hold", reason="invalid_target")


class TelemetryStrategy(BaseStrategy):
    key = "telemetry_strategy"
    name = "TelemetryStrategy"
    description = "Produces deterministic debug telemetry for the backtest pipeline."
    config_model = BaseStrategyConfig
    debug_counter_keys = (
        "breakout_candidate_count",
        "retest_candidate_count",
        "confirmation_pass_count",
        "entry_signal_count",
    )

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        index = context.metadata["bar_index"]
        has_position = context.metadata["has_position"]
        if index == 0:
            return StrategySignal(
                action="hold",
                reason="insufficient_history",
                metadata={
                    "debug_reject_reason": "insufficient_lookback",
                    "debug_reject_detail": "warmup_not_ready",
                },
            )
        if index == 1:
            return StrategySignal(
                action="hold",
                reason="breakout_not_confirmed",
                metadata={
                    "debug_setup_detected": True,
                    "debug_reject_reason": "no_entry_confirmation",
                    "debug_reject_detail": "retest_confirmation_missing",
                    "debug_strategy_counters_delta": {
                        "breakout_candidate_count": 1,
                        "retest_candidate_count": 1,
                    },
                },
            )
        if index == 2 and not has_position:
            return StrategySignal(
                action="enter",
                reason="telemetry_entry",
                metadata={
                    "debug_setup_detected": True,
                    "debug_strategy_counters_delta": {
                        "breakout_candidate_count": 1,
                        "retest_candidate_count": 1,
                        "confirmation_pass_count": 1,
                        "entry_signal_count": 1,
                    },
                },
            )
        if index == 3 and has_position:
            return StrategySignal(action="exit", reason="telemetry_exit")
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
        symbol="BTC-USDT",
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


def test_backtest_engine_aggregates_entry_hold_reason_diagnostics() -> None:
    engine = BacktestEngine()
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    candles = [
        _candle(start + timedelta(minutes=index * 5), str(100 + index))
        for index in range(18)
    ]

    report = engine.run(
        request=_request("diagnostic_hold_strategy"),
        strategy=DiagnosticHoldStrategy(),
        candles=candles,
    )

    assert report.diagnostics["entry_hold_total"] == 18
    assert report.diagnostics["entry_hold_reasons"] == {
        "insufficient_history": 1,
        "regime_blocked": 1,
        "breakout_not_valid": 0,
        "retest_not_reached": 0,
        "retest_failed": 0,
        "range_not_tight_enough": 1,
        "breakout_not_confirmed": 1,
        "breakout_bar_not_green": 1,
        "breakout_bar_too_weak": 1,
        "breakout_bar_too_extended": 1,
        "flush_not_deep_enough": 1,
        "late_rebound_entry": 1,
        "flush_low_too_old": 1,
        "context_not_reset": 1,
        "reclaim_not_confirmed": 1,
        "entry_bar_not_green": 1,
        "entry_bar_too_weak": 1,
        "entry_bar_too_strong": 1,
        "insufficient_tp_vs_cost": 1,
        "max_stop_exceeded": 1,
        "any_other_hold_reason": 1,
    }
    assert report.diagnostics["entry_hold_reason_details"] == {
        "close_below_ema200_1h": 1,
        "flush_not_deep_enough": 1,
        "late_rebound_entry": 1,
        "flush_low_too_old": 1,
        "context_not_reset": 1,
        "reclaim_not_confirmed": 1,
        "entry_bar_not_green": 1,
        "entry_bar_too_weak": 1,
        "entry_bar_too_strong": 1,
    }
    assert report.diagnostics["regime_blocked_details"] == {"close_below_ema200_1h": 1}
    assert report.diagnostics["entry_hold_other_reasons"] == {"invalid_target": 1}


def test_backtest_engine_records_pipeline_debug_counters() -> None:
    engine = BacktestEngine()
    candles = [
        _candle(datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc), "100"),
        _candle(datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc), "101"),
        _candle(datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc), "102"),
        _candle(datetime(2026, 1, 1, 0, 15, tzinfo=timezone.utc), "103"),
    ]

    report = engine.run(
        request=_request("telemetry_strategy"),
        strategy=TelemetryStrategy(),
        candles=candles,
    )

    pipeline = report.diagnostics["pipeline_counters"]
    reject_reasons = report.diagnostics["reject_reasons"]
    strategy_counters = report.diagnostics["strategy_specific_counters"]

    assert pipeline["total_candles_processed"] == 4
    assert pipeline["total_iterations"] == 4
    assert pipeline["total_setups_detected"] == 2
    assert pipeline["total_signals_generated"] == 2
    assert pipeline["total_signals_rejected"] == 2
    assert pipeline["total_orders_created"] == 1
    assert pipeline["total_orders_filled"] == 1
    assert pipeline["total_positions_opened"] == 1
    assert pipeline["total_positions_closed"] == 1
    assert pipeline["total_trades_closed"] == 1

    assert reject_reasons["insufficient_lookback"] == 1
    assert reject_reasons["no_entry_confirmation"] == 1
    assert strategy_counters["breakout_candidate_count"] == 2
    assert strategy_counters["retest_candidate_count"] == 2
    assert strategy_counters["confirmation_pass_count"] == 1
    assert strategy_counters["entry_signal_count"] == 1


def test_breakout_retest_debug_diagnostics_show_retest_not_reached_path() -> None:
    engine = BacktestEngine()
    candles = [
        BacktestCandle(
            open_time=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=5 * index),
            open=Decimal(value[0]),
            high=Decimal(value[1]),
            low=Decimal(value[2]),
            close=Decimal(value[3]),
            volume=Decimal("1"),
        )
        for index, value in enumerate(
            [
                ("100.0", "100.8", "99.7", "100.4"),
                ("100.4", "100.9", "100.0", "100.6"),
                ("100.6", "101.0", "100.2", "100.7"),
                ("100.7", "101.1", "100.3", "100.8"),
                ("100.8", "101.3", "100.6", "101.2"),
                ("101.55", "101.7", "101.52", "101.62"),
            ]
        )
    ]

    report = engine.run(
        request=_request(
            "breakout_retest",
        ).model_copy(
            update={
                "strategy_config_override": {
                    "breakout_lookback": 4,
                    "atr_period": 3,
                    "regime_filter_enabled": False,
                    "require_cost_edge": False,
                    "max_bars_in_trade": 3,
                    "retest_tolerance_pct": 0.004,
                    "breakout_min_body_pct": 0.001,
                    "breakout_max_body_pct": 0.03,
                    "max_breakout_extension_pct": 0.01,
                    "confirmation_min_body_pct": 0.0005,
                    "max_stop_pct": 0.05,
                    "stop_buffer_atr_mult": 0.0,
                }
            }
        ),
        strategy=BreakoutRetestStrategy(),
        candles=candles,
    )

    pipeline = report.diagnostics["pipeline_counters"]
    reject_reasons = report.diagnostics["reject_reasons"]
    reject_reason_details = report.diagnostics["reject_reason_details"]
    strategy_counters = report.diagnostics["strategy_specific_counters"]

    assert report.metrics.total_trades == 0
    assert pipeline["total_candles_processed"] == len(candles)
    assert pipeline["total_signals_generated"] == 0
    assert pipeline["total_orders_filled"] == 0
    assert reject_reasons["insufficient_lookback"] > 0
    assert reject_reasons["no_entry_confirmation"] > 0
    assert reject_reason_details["retest_not_reached"] > 0
    assert strategy_counters["breakout_candidate_count"] >= 1
    assert strategy_counters["retest_candidate_count"] == 0
    assert strategy_counters["confirmation_pass_count"] == 0
    assert strategy_counters["entry_signal_count"] == 0
