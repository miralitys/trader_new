from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal, Mapping, Sequence

from pydantic import Field

from app.schemas.backtest import BacktestCandle
from app.strategies.base import BaseStrategyConfig, StrategyContext, StrategySignal
from app.strategies.pullback_in_trend import ONE, StopMode, TargetMode, ZERO
from app.strategies.pullback_in_trend_v2 import PullbackInTrendV2Strategy

DerivedContextTimeframe = Literal["4h", "1d"]
PullbackReferenceMode72h = Literal["ema20", "retrace_band", "ema20_or_retrace_band"]
TriggerMode72h = Literal["reclaim_and_break_prev_high", "close_above_ema20", "break_prev_high"]


class TrendReclaim72hConfig(BaseStrategyConfig):
    signal_timeframe: str = "1h"
    context_from_1h_enabled: bool = True
    derived_context_timeframes: list[DerivedContextTimeframe] = Field(
        default_factory=lambda: ["4h", "1d"]
    )
    required_preroll_days: int = Field(default=365, ge=0)

    regime_filter_enabled: bool = True
    regime_ema_period_4h: int = Field(default=200, ge=2)
    require_close_above_ema200_4h: bool = False
    require_non_negative_ema200_slope_4h: bool = False
    min_ema200_slope_4h: float = Field(default=0.0)
    require_atr_band_4h: bool = True
    min_atr_pct_4h: float = Field(default=0.003, ge=0)
    max_atr_pct_4h: float = Field(default=0.04, gt=0)
    require_not_overextended: bool = True
    trend_extension_ema_period_4h: int = Field(default=20, ge=2)
    max_distance_above_ema20_4h: float = Field(default=0.06, ge=0)
    use_daily_trend_confirmation: bool = False
    require_close_above_ema200_1d: bool = False
    daily_ema_period: int = Field(default=200, ge=2)
    filter_expanding_downside_volatility: bool = False
    downside_volatility_lookback: int = Field(default=6, ge=2)
    downside_volatility_expansion_ratio: float = Field(default=1.5, gt=0)

    impulse_lookback_bars: int = Field(default=12, ge=4)
    impulse_min_return_pct: float = Field(default=0.015, ge=0)
    impulse_max_bars: int = Field(default=6, ge=2)
    impulse_min_body_pct: float = Field(default=0.003, ge=0)
    impulse_max_upper_wick_ratio: float = Field(default=1.2, gt=0)

    pullback_lookback_bars: int = Field(default=8, ge=2)
    pullback_ema_period: int = Field(default=20, ge=2)
    pullback_reference_mode: PullbackReferenceMode72h = "ema20_or_retrace_band"
    pullback_touch_tolerance_pct: float = Field(default=0.0015, ge=0)
    min_impulse_retrace_ratio: float = Field(default=0.25, ge=0, le=1)
    max_impulse_retrace_ratio: float = Field(default=0.5, gt=0, le=1)
    max_pullback_pct: float = Field(default=0.06, gt=0)
    require_pullback_above_structure_low: bool = True
    require_pullback_volatility_contraction: bool = True

    trigger_mode: TriggerMode72h = "reclaim_and_break_prev_high"
    require_trigger_green: bool = True
    trigger_min_body_pct: float = Field(default=0.002, ge=0)
    require_close_near_high: bool = True
    close_near_high_threshold: float = Field(default=0.65, ge=0, le=1)

    atr_period: int = Field(default=14, ge=2)
    stop_mode: StopMode = "pullback_low"
    stop_buffer_atr_mult: float = Field(default=0.15, ge=0)
    max_stop_pct: float = Field(default=0.05, gt=0)

    target_mode: TargetMode = "stop_multiple"
    target_r_multiple: float = Field(default=1.8, gt=0)
    fixed_target_pct: float = Field(default=0.04, gt=0)

    max_bars_in_trade: int = Field(default=72, ge=1)
    fast_failure_bars: int = Field(default=8, ge=1)
    fast_failure_min_progress_r: float = Field(default=0.2, ge=0)
    pullback_failure_buffer_pct: float = Field(default=0.0, ge=0)

    require_cost_edge: bool = True
    min_tp_cost_multiple: float = Field(default=1.5, gt=0)


class TrendReclaim72hStrategy(PullbackInTrendV2Strategy):
    key = "trend_reclaim_72h"
    name = "TrendReclaim72h"
    description = "Long-only swing continuation strategy on 1H reclaim setups with derived 4H/1D trend context."
    status = "implemented"
    config_model = TrendReclaim72hConfig
    debug_counter_keys = (
        "context_pass_count",
        "impulse_candidate_count",
        "impulse_quality_pass_count",
        "pullback_candidate_count",
        "pullback_shape_pass_count",
        "trigger_confirmed_count",
        "entry_signal_count",
        "fast_failure_exit_count",
    )

    def required_preroll_days(
        self,
        timeframe: str,
        strategy_config: TrendReclaim72hConfig | None = None,
    ) -> int:
        config = strategy_config or self.config_model()
        return int(config.required_preroll_days)

    def required_history_bars(
        self,
        timeframe: str,
        strategy_config: TrendReclaim72hConfig | None = None,
    ) -> int:
        config = strategy_config or self.config_model()
        bars_per_day = max(1, self._bars_per_hour(timeframe) * 24)
        setup_bars = max(
            config.impulse_lookback_bars + config.pullback_lookback_bars + 2,
            config.pullback_ema_period + config.pullback_lookback_bars + 2,
            config.atr_period + 2,
            config.max_bars_in_trade + 2,
            config.fast_failure_bars + 2,
        )
        preroll_bars = config.required_preroll_days * bars_per_day
        derived_context_bars = config.regime_ema_period_4h * 4
        if config.require_atr_band_4h:
            derived_context_bars = max(derived_context_bars, (config.atr_period + 2) * 4)
        if config.require_not_overextended:
            derived_context_bars = max(
                derived_context_bars,
                config.trend_extension_ema_period_4h * 4,
            )
        if config.use_daily_trend_confirmation:
            derived_context_bars = max(derived_context_bars, config.daily_ema_period * 24)
        return max(setup_bars, preroll_bars, derived_context_bars)

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        config = self._config_from_context(context)
        raw_history = context.metadata.get("history", [])
        history = raw_history if isinstance(raw_history, Sequence) else list(raw_history)

        if not config.enabled:
            return self._hold("disabled")

        if context.timeframe != config.signal_timeframe:
            return self._hold(
                "any_other_hold_reason",
                detail="unsupported_signal_timeframe",
                debug_reject_reason="invalid_config",
                debug_reject_detail="unsupported_signal_timeframe",
            )

        required_history = self.required_history_bars(context.timeframe, config)
        if len(history) < required_history:
            return self._hold(
                "insufficient_history",
                debug_reject_reason="insufficient_lookback",
                debug_reject_detail="trend_reclaim_72h_not_ready",
                history_bars=len(history),
                required_history_bars=required_history,
            )

        if context.metadata.get("has_position"):
            return self._exit_signal(context=context, history=history, config=config)

        return self._entry_signal(context=context, history=history, config=config)

    def _entry_signal(
        self,
        context: StrategyContext,
        history: Sequence[BacktestCandle],
        config: TrendReclaim72hConfig,
    ) -> StrategySignal:
        trigger_bar = history[-1]
        pullback_window = history[-(config.pullback_lookback_bars + 1) : -1]
        impulse_window = history[
            -(config.impulse_lookback_bars + config.pullback_lookback_bars + 1) : -(config.pullback_lookback_bars + 1)
        ]
        closes = [self._decimal(bar.close) for bar in history]
        ema_series = self._ema_series(closes, config.pullback_ema_period)
        current_ema = ema_series[-1]
        previous_ema = ema_series[-2]
        pullback_ema_values = ema_series[-(config.pullback_lookback_bars + 1) : -1]
        context_counters = {"context_pass_count": 1}

        regime_block_reason = self._regime_block_reason(
            history=history,
            timeframe=context.timeframe,
            config=config,
            metadata=context.metadata,
        )
        if regime_block_reason is not None:
            return self._hold(
                "regime_blocked",
                detail=regime_block_reason,
                debug_strategy_counters_delta=context_counters,
            )

        impulse = self._detect_impulse(impulse_window=impulse_window, config=config)
        if impulse["reason"] is not None:
            return self._hold(
                str(impulse["reason"]),
                detail=self._string_or_none(impulse.get("detail")),
                debug_strategy_counters_delta=context_counters,
                **self._signal_metadata(impulse),
            )

        impulse_counters = {
            "context_pass_count": 1,
            "impulse_candidate_count": 1,
            "impulse_quality_pass_count": 1,
        }

        pullback = self._detect_swing_pullback(
            pullback_window=pullback_window,
            pullback_ema_values=pullback_ema_values,
            impulse=impulse,
            config=config,
        )
        if pullback["reason"] is not None:
            return self._hold(
                str(pullback["reason"]),
                detail=self._string_or_none(pullback.get("detail")),
                debug_setup_detected=True,
                debug_strategy_counters_delta=impulse_counters,
                debug_reject_reason="no_entry_confirmation",
                debug_reject_detail=self._string_or_none(pullback.get("detail")),
                **self._signal_metadata({**impulse, **pullback}),
            )

        pullback_counters = {
            "context_pass_count": 1,
            "impulse_candidate_count": 1,
            "impulse_quality_pass_count": 1,
            "pullback_candidate_count": 1,
            "pullback_shape_pass_count": 1,
        }

        previous_bar = history[-2]
        previous_close = self._decimal(previous_bar.close)
        previous_high = self._decimal(previous_bar.high)
        trigger_open = self._decimal(trigger_bar.open)
        trigger_high = self._decimal(trigger_bar.high)
        trigger_low = self._decimal(trigger_bar.low)
        trigger_close = self._decimal(trigger_bar.close)
        trigger_body_pct = self._ratio(trigger_close - trigger_open, trigger_open)
        trigger_via_ema = previous_close <= previous_ema and trigger_close > current_ema
        trigger_via_prev_high = trigger_close > previous_high
        trigger_confirmed = (
            trigger_via_ema and trigger_via_prev_high
            if config.trigger_mode == "reclaim_and_break_prev_high"
            else trigger_via_ema
            if config.trigger_mode == "close_above_ema20"
            else trigger_via_prev_high
        )
        if not trigger_confirmed:
            return self._hold(
                "trigger_not_confirmed",
                detail="trigger_conditions_not_met",
                debug_setup_detected=True,
                debug_strategy_counters_delta=pullback_counters,
                debug_reject_reason="no_entry_confirmation",
                debug_reject_detail="trigger_conditions_not_met",
                current_ema=current_ema,
                previous_high=previous_high,
                trigger_close=trigger_close,
            )

        if config.require_trigger_green and trigger_close <= trigger_open:
            return self._hold(
                "trigger_bar_too_weak",
                detail="trigger_bar_not_green",
                debug_setup_detected=True,
                debug_strategy_counters_delta=pullback_counters,
                debug_reject_reason="no_entry_confirmation",
                debug_reject_detail="trigger_bar_not_green",
                trigger_body_pct=trigger_body_pct,
            )

        if trigger_body_pct < self._decimal(config.trigger_min_body_pct):
            return self._hold(
                "trigger_bar_too_weak",
                detail="trigger_bar_too_weak",
                debug_setup_detected=True,
                debug_strategy_counters_delta=pullback_counters,
                debug_reject_reason="no_entry_confirmation",
                debug_reject_detail="trigger_bar_too_weak",
                trigger_body_pct=trigger_body_pct,
            )

        if config.require_close_near_high:
            close_location = self._close_location(high=trigger_high, low=trigger_low, close=trigger_close)
            if close_location < self._decimal(config.close_near_high_threshold):
                return self._hold(
                    "trigger_close_not_strong_enough",
                    detail="trigger_close_not_strong_enough",
                    debug_setup_detected=True,
                    debug_strategy_counters_delta=pullback_counters,
                    debug_reject_reason="no_entry_confirmation",
                    debug_reject_detail="trigger_close_not_strong_enough",
                    close_location=close_location,
                )

        atr_value = self._atr(history, config.atr_period)
        if atr_value is None or atr_value <= ZERO:
            return self._hold("insufficient_history")

        entry_price = trigger_close
        trigger_counters = {
            "context_pass_count": 1,
            "impulse_candidate_count": 1,
            "impulse_quality_pass_count": 1,
            "pullback_candidate_count": 1,
            "pullback_shape_pass_count": 1,
            "trigger_confirmed_count": 1,
        }
        stop_price = self._stop_price(
            pullback_low=self._decimal(pullback["pullback_low"]),
            trigger_low=trigger_low,
            atr_value=atr_value,
            entry_price=entry_price,
            config=config,
        )
        if stop_price is None:
            return self._hold(
                "max_stop_exceeded",
                detail="invalid_stop_structure",
                debug_setup_detected=True,
                debug_strategy_counters_delta=trigger_counters,
            )

        stop_distance = entry_price - stop_price
        stop_distance_pct = self._ratio(stop_distance, entry_price)
        if stop_distance <= ZERO or stop_distance_pct > self._decimal(config.max_stop_pct):
            return self._hold(
                "max_stop_exceeded",
                detail="max_stop_exceeded",
                debug_setup_detected=True,
                debug_strategy_counters_delta=trigger_counters,
                stop_distance_pct=stop_distance_pct,
            )

        take_profit_price = self._take_profit_price(entry_price=entry_price, stop_price=stop_price, config=config)
        if take_profit_price <= entry_price:
            return self._hold(
                "any_other_hold_reason",
                detail="invalid_target",
                debug_setup_detected=True,
                debug_strategy_counters_delta=trigger_counters,
            )

        if config.require_cost_edge:
            fee_rate = self._decimal(context.metadata.get("fee_rate", ZERO))
            slippage_rate = self._decimal(context.metadata.get("slippage_rate", ZERO))
            take_profit_pct = self._ratio(take_profit_price - entry_price, entry_price)
            round_trip_cost_pct = (fee_rate + slippage_rate) * Decimal("2")
            minimum_edge = round_trip_cost_pct * self._decimal(config.min_tp_cost_multiple)
            if take_profit_pct <= minimum_edge:
                return self._hold(
                    "insufficient_tp_vs_cost",
                    detail="insufficient_tp_vs_cost",
                    debug_setup_detected=True,
                    debug_strategy_counters_delta=trigger_counters,
                    round_trip_cost_pct=round_trip_cost_pct,
                    take_profit_pct=take_profit_pct,
                )

        return StrategySignal(
            action="enter",
            reason="trend_reclaim_72h_entry",
            confidence=0.65,
            metadata={
                "atr": str(atr_value),
                "current_ema": str(current_ema),
                "entry_price": str(entry_price),
                "impulse_high": str(impulse["impulse_high"]),
                "impulse_low": str(impulse["impulse_low"]),
                "impulse_return_pct": float(self._decimal(impulse["impulse_return_pct"])),
                "pullback_low": str(pullback["pullback_low"]),
                "pullback_depth_pct": float(self._decimal(pullback["pullback_depth_pct"])),
                "retracement_ratio": float(self._decimal(pullback["retracement_ratio"])),
                "stop_mode": config.stop_mode,
                "stop_price": str(stop_price),
                "target_mode": config.target_mode,
                "take_profit_price": str(take_profit_price),
                "trigger_mode": config.trigger_mode,
                "debug_setup_detected": True,
                "debug_strategy_counters_delta": {
                    **trigger_counters,
                    "entry_signal_count": 1,
                },
            },
        )

    def _exit_signal(
        self,
        context: StrategyContext,
        history: Sequence[BacktestCandle],
        config: TrendReclaim72hConfig,
    ) -> StrategySignal:
        position = context.metadata.get("position") or {}
        entry_metadata = dict(position.get("entry_metadata") or {})
        entry_price = self._decimal(position.get("entry_price"))
        stop_price = self._decimal(entry_metadata.get("stop_price"))
        current_close = self._decimal(history[-1].close)
        bars_held = self._bars_held(history=history, entry_time=position.get("entry_time"))
        ema_series = self._ema_series([self._decimal(bar.close) for bar in history], config.pullback_ema_period)
        current_ema = ema_series[-1]

        if entry_price > ZERO and stop_price > ZERO and entry_price > stop_price and bars_held >= config.fast_failure_bars:
            progress_r = (current_close - entry_price) / (entry_price - stop_price)
            if progress_r < self._decimal(config.fast_failure_min_progress_r):
                return StrategySignal(
                    action="exit",
                    reason="fast_failure",
                    confidence=0.45,
                    metadata={
                        "bars_held": bars_held,
                        "progress_r": float(progress_r),
                        "debug_strategy_counters_delta": {"fast_failure_exit_count": 1},
                    },
                )

        trend_failure_level = current_ema * (ONE - self._decimal(config.pullback_failure_buffer_pct))
        if current_close < trend_failure_level:
            return StrategySignal(
                action="exit",
                reason="trend_failure",
                confidence=0.45,
                metadata={"trend_failure_level": str(trend_failure_level)},
            )

        if bars_held >= config.max_bars_in_trade:
            return StrategySignal(
                action="exit",
                reason="time_stop",
                confidence=0.4,
                metadata={"bars_held": bars_held},
            )

        return StrategySignal(action="hold", reason="position_open")

    def _regime_block_reason(
        self,
        history: Sequence[BacktestCandle],
        timeframe: str,
        config: TrendReclaim72hConfig,
        metadata: Mapping[str, object] | None = None,
    ) -> str | None:
        if not config.regime_filter_enabled:
            return None

        one_hour_history = self._ensure_one_hour_history(history=history, timeframe=timeframe)
        four_hour_history = self._aggregate_from_one_hour(one_hour_history, 4)
        four_hour_closes = [self._decimal(bar.close) for bar in four_hour_history]
        minimum_four_hour_bars = config.regime_ema_period_4h
        if config.require_atr_band_4h:
            minimum_four_hour_bars = max(minimum_four_hour_bars, config.atr_period + 2)
        if config.require_not_overextended:
            minimum_four_hour_bars = max(minimum_four_hour_bars, config.trend_extension_ema_period_4h)
        if len(four_hour_closes) < minimum_four_hour_bars:
            return "insufficient_history"

        ema200_4h = self._ema_series(four_hour_closes, config.regime_ema_period_4h)
        current_close_4h = four_hour_closes[-1]
        current_ema200_4h = ema200_4h[-1]
        previous_ema200_4h = ema200_4h[-2] if len(ema200_4h) > 1 else current_ema200_4h
        if config.require_close_above_ema200_4h and current_close_4h <= current_ema200_4h:
            return "close_below_ema200_4h"

        ema_slope = self._ratio(current_ema200_4h - previous_ema200_4h, previous_ema200_4h)
        if (
            config.require_non_negative_ema200_slope_4h
            and ema_slope < self._decimal(config.min_ema200_slope_4h)
        ):
            return "ema200_slope_below_threshold_4h"

        if config.require_atr_band_4h:
            atr_4h = self._atr(four_hour_history, config.atr_period)
            if atr_4h is None or current_close_4h <= ZERO:
                return "insufficient_history"
            atr_pct_4h = atr_4h / current_close_4h
            if atr_pct_4h < self._decimal(config.min_atr_pct_4h):
                return "atr_pct_below_min_4h"
            if atr_pct_4h > self._decimal(config.max_atr_pct_4h):
                return "atr_pct_above_max_4h"

        if config.require_not_overextended:
            ema20_4h = self._ema_series(four_hour_closes, config.trend_extension_ema_period_4h)[-1]
            if current_close_4h > ema20_4h * (ONE + self._decimal(config.max_distance_above_ema20_4h)):
                return "trend_too_extended"

        if config.filter_expanding_downside_volatility and self._expanding_downside_volatility(
            closes=four_hour_closes,
            lookback=config.downside_volatility_lookback,
            ratio_threshold=self._decimal(config.downside_volatility_expansion_ratio),
        ):
            return "downside_volatility_blocked"

        if config.use_daily_trend_confirmation and config.require_close_above_ema200_1d:
            daily_history = self._aggregate_from_one_hour(one_hour_history, 24)
            daily_closes = [self._decimal(bar.close) for bar in daily_history]
            if len(daily_closes) < config.daily_ema_period:
                return "insufficient_history"
            ema200_1d = self._ema_series(daily_closes, config.daily_ema_period)[-1]
            if daily_closes[-1] <= ema200_1d:
                return "close_below_ema200_1d"

        return None

    def _detect_swing_pullback(
        self,
        pullback_window: Sequence[BacktestCandle],
        pullback_ema_values: Sequence[Decimal],
        impulse: Mapping[str, object],
        config: TrendReclaim72hConfig,
    ) -> dict[str, object]:
        impulse_low = self._decimal(impulse.get("impulse_low"))
        impulse_high = self._decimal(impulse.get("impulse_high"))
        impulse_range = self._decimal(impulse.get("impulse_range"))
        pullback_low = min(self._decimal(bar.low) for bar in pullback_window)
        pullback_depth_pct = self._ratio(impulse_high - pullback_low, impulse_high)
        retracement_ratio = self._ratio(impulse_high - pullback_low, impulse_range)
        ema_touched = any(
            self._decimal(bar.low) <= ema_value * (ONE + self._decimal(config.pullback_touch_tolerance_pct))
            for bar, ema_value in zip(pullback_window, pullback_ema_values)
        )
        retracement_touched = (
            retracement_ratio >= self._decimal(config.min_impulse_retrace_ratio)
            and retracement_ratio <= self._decimal(config.max_impulse_retrace_ratio)
        )
        reference_detected = (
            ema_touched
            if config.pullback_reference_mode == "ema20"
            else retracement_touched
            if config.pullback_reference_mode == "retrace_band"
            else ema_touched or retracement_touched
        )
        if not reference_detected:
            return {
                "reason": "pullback_not_detected",
                "detail": "pullback_reference_not_reached",
                "pullback_low": pullback_low,
                "pullback_depth_pct": pullback_depth_pct,
                "retracement_ratio": retracement_ratio,
            }

        if config.require_pullback_above_structure_low and pullback_low <= impulse_low:
            return {
                "reason": "pullback_broke_structure",
                "detail": "pullback_broke_structure",
                "pullback_low": pullback_low,
                "impulse_low": impulse_low,
            }

        if (
            retracement_ratio > self._decimal(config.max_impulse_retrace_ratio)
            or pullback_depth_pct > self._decimal(config.max_pullback_pct)
        ):
            return {
                "reason": "pullback_too_deep",
                "detail": "pullback_too_deep",
                "pullback_low": pullback_low,
                "pullback_depth_pct": pullback_depth_pct,
                "retracement_ratio": retracement_ratio,
            }

        if config.require_pullback_volatility_contraction:
            impulse_segment = impulse.get("impulse_segment")
            impulse_avg_range = self._average_range(impulse_segment if isinstance(impulse_segment, Sequence) else tuple())
            pullback_avg_range = self._average_range(pullback_window)
            if pullback_avg_range >= impulse_avg_range:
                return {
                    "reason": "pullback_shape_invalid",
                    "detail": "pullback_volatility_not_contracting",
                    "pullback_avg_range": pullback_avg_range,
                    "impulse_avg_range": impulse_avg_range,
                }

        return {
            "reason": None,
            "detail": None,
            "pullback_low": pullback_low,
            "pullback_depth_pct": pullback_depth_pct,
            "retracement_ratio": retracement_ratio,
        }

    def _ensure_one_hour_history(
        self,
        history: Sequence[BacktestCandle],
        timeframe: str,
    ) -> list[BacktestCandle]:
        if timeframe == "1h":
            return list(history)
        return self._aggregate_to_one_hour(history, timeframe)

    def _aggregate_from_one_hour(
        self,
        one_hour_history: Sequence[BacktestCandle],
        bucket_hours: int,
    ) -> list[BacktestCandle]:
        aggregated: list[BacktestCandle] = []
        for candle in one_hour_history:
            if bucket_hours == 24:
                bucket_time = candle.open_time.replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                bucket_start_hour = (candle.open_time.hour // bucket_hours) * bucket_hours
                bucket_time = candle.open_time.replace(
                    hour=bucket_start_hour,
                    minute=0,
                    second=0,
                    microsecond=0,
                )
            if not aggregated or aggregated[-1].open_time != bucket_time:
                aggregated.append(
                    BacktestCandle(
                        open_time=bucket_time,
                        open=self._decimal(candle.open),
                        high=self._decimal(candle.high),
                        low=self._decimal(candle.low),
                        close=self._decimal(candle.close),
                        volume=self._decimal(candle.volume),
                    )
                )
                continue

            previous = aggregated[-1]
            aggregated[-1] = BacktestCandle(
                open_time=previous.open_time,
                open=previous.open,
                high=max(previous.high, self._decimal(candle.high)),
                low=min(previous.low, self._decimal(candle.low)),
                close=self._decimal(candle.close),
                volume=previous.volume + self._decimal(candle.volume),
            )
        return aggregated

    def _config_from_context(self, context: StrategyContext) -> TrendReclaim72hConfig:
        config = context.metadata.get("config")
        if isinstance(config, TrendReclaim72hConfig):
            return config
        if isinstance(config, BaseStrategyConfig):
            return self.parse_config(config.model_dump())  # type: ignore[arg-type]
        if isinstance(config, dict):
            return self.parse_config(config)
        return self.parse_config({})
