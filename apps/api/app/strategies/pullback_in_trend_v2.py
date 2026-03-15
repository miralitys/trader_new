from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Iterable, Literal, Mapping, Sequence

from pydantic import Field

from app.schemas.backtest import BacktestCandle
from app.strategies.base import BaseStrategyConfig, StrategyContext, StrategySignal
from app.strategies.pullback_in_trend import (
    ONE,
    PullbackInTrendStrategy,
    PullbackReferenceMode,
    StopMode,
    TargetMode,
    ZERO,
)

TriggerModeV2 = Literal["reclaim_and_break_prev_high", "close_above_ema20", "break_prev_high"]


class PullbackInTrendV2Config(BaseStrategyConfig):
    impulse_lookback_bars: int = Field(default=20, ge=4)
    impulse_min_return_pct: float = Field(default=0.008, ge=0)
    impulse_max_bars: int = Field(default=8, ge=2)
    impulse_min_body_pct: float = Field(default=0.001, ge=0)
    impulse_max_upper_wick_ratio: float = Field(default=1.2, gt=0)
    pullback_lookback_bars: int = Field(default=6, ge=2)
    pullback_ema_period: int = Field(default=20, ge=2)
    pullback_reference_mode: PullbackReferenceMode = "ema20"
    pullback_touch_tolerance_pct: float = Field(default=0.0015, ge=0)
    min_pullback_pct: float = Field(default=0.001, ge=0)
    max_pullback_pct: float = Field(default=0.015, gt=0)
    min_impulse_retrace_ratio: float = Field(default=0.25, ge=0, le=1)
    max_impulse_retrace_ratio: float = Field(default=0.5, gt=0, le=1)
    require_pullback_above_structure_low: bool = True
    require_pullback_volatility_contraction: bool = True
    trigger_mode: TriggerModeV2 = "reclaim_and_break_prev_high"
    require_trigger_green: bool = True
    trigger_min_body_pct: float = Field(default=0.0008, ge=0)
    require_close_near_high: bool = True
    close_near_high_threshold: float = Field(default=0.65, ge=0, le=1)
    atr_period: int = Field(default=14, ge=2)
    stop_mode: StopMode = "pullback_low"
    stop_buffer_atr_mult: float = Field(default=0.1, ge=0)
    max_stop_pct: float = Field(default=0.02, gt=0)
    target_mode: TargetMode = "stop_multiple"
    target_r_multiple: float = Field(default=1.2, gt=0)
    fixed_target_pct: float = Field(default=0.01, gt=0)
    max_bars_in_trade: int = Field(default=10, ge=1)
    pullback_failure_buffer_pct: float = Field(default=0.0, ge=0)
    fast_failure_bars: int = Field(default=3, ge=1)
    fast_failure_min_progress_r: float = Field(default=0.2, ge=0)
    require_cost_edge: bool = True
    min_tp_cost_multiple: float = Field(default=1.5, gt=0)
    regime_filter_enabled: bool = True
    regime_ema_period: int = Field(default=200, ge=2)
    require_close_above_ema200_1h: bool = True
    min_ema_slope: float = Field(default=0.0)
    regime_atr_period: int = Field(default=14, ge=2)
    min_atr_pct_1h: float = Field(default=0.0, ge=0)
    max_atr_pct_1h: float = Field(default=0.08, gt=0)
    htf_rsi_period: int = Field(default=14, ge=2)
    min_htf_rsi: float = Field(default=0.0, ge=0, le=100)
    filter_expanding_downside_volatility: bool = False
    downside_volatility_lookback: int = Field(default=6, ge=2)
    downside_volatility_expansion_ratio: float = Field(default=1.5, gt=0)
    require_trend_not_overextended: bool = True
    trend_extension_ema_short_period: int = Field(default=20, ge=2)
    trend_extension_ema_long_period: int = Field(default=50, ge=2)
    max_close_above_ema20_1h_pct: float = Field(default=0.03, ge=0)
    max_close_above_ema50_1h_pct: float = Field(default=0.05, ge=0)


class PullbackInTrendV2Strategy(PullbackInTrendStrategy):
    key = "pullback_in_trend_v2"
    name = "PullbackInTrendV2"
    description = "Long-only continuation strategy with stricter trend, pullback-shape, and trigger quality filters."
    status = "implemented"
    config_model = PullbackInTrendV2Config
    debug_counter_keys = (
        "context_pass_count",
        "impulse_candidate_count",
        "impulse_quality_pass_count",
        "pullback_candidate_count",
        "pullback_shape_pass_count",
        "trigger_confirmed_count",
        "entry_signal_count",
    )

    def required_history_bars(
        self,
        timeframe: str,
        strategy_config: PullbackInTrendV2Config | None = None,
    ) -> int:
        config = strategy_config or self.config_model()
        bars_for_setup = max(
            config.impulse_lookback_bars + config.pullback_lookback_bars + 2,
            config.pullback_ema_period + config.pullback_lookback_bars + 2,
            config.atr_period + 2,
            config.max_bars_in_trade + 2,
            config.fast_failure_bars + 2,
        )
        if not config.regime_filter_enabled:
            return bars_for_setup

        bars_per_hour = self._bars_per_hour(timeframe)
        regime_hour_bars = max(
            config.regime_ema_period + 2,
            config.regime_atr_period + 2,
            config.trend_extension_ema_short_period + 2,
            config.trend_extension_ema_long_period + 2,
            (config.htf_rsi_period + 2) if config.min_htf_rsi > 0 else 0,
            (config.downside_volatility_lookback * 2) + 2 if config.filter_expanding_downside_volatility else 0,
        )
        return max(bars_for_setup, regime_hour_bars * bars_per_hour)

    def _entry_signal(
        self,
        context: StrategyContext,
        history: Sequence[BacktestCandle],
        config: PullbackInTrendV2Config,
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

        pullback = self._detect_pullback(
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
            reason="pullback_in_trend_v2_entry",
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
        config: PullbackInTrendV2Config,
    ) -> StrategySignal:
        position = context.metadata.get("position") or {}
        entry_metadata = dict(position.get("entry_metadata") or {})
        pullback_low = self._decimal(entry_metadata.get("pullback_low"))
        entry_price = self._decimal(position.get("entry_price"))
        stop_price = self._decimal(entry_metadata.get("stop_price"))
        current_close = self._decimal(history[-1].close)
        bars_held = self._bars_held(history=history, entry_time=position.get("entry_time"))

        if pullback_low > ZERO:
            failure_level = pullback_low * (ONE - self._decimal(config.pullback_failure_buffer_pct))
            if current_close < failure_level:
                return StrategySignal(
                    action="exit",
                    reason="pullback_failure",
                    confidence=0.45,
                    metadata={"pullback_low": str(pullback_low)},
                )

        if entry_price > ZERO and stop_price > ZERO and entry_price > stop_price and bars_held >= config.fast_failure_bars:
            progress_r = (current_close - entry_price) / (entry_price - stop_price)
            if progress_r < self._decimal(config.fast_failure_min_progress_r):
                return StrategySignal(
                    action="exit",
                    reason="fast_failure",
                    confidence=0.45,
                    metadata={"bars_held": bars_held, "progress_r": float(progress_r)},
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
        config: PullbackInTrendV2Config,
        metadata: Mapping[str, object] | None = None,
    ) -> str | None:
        if not config.regime_filter_enabled:
            return None
        baseline_reason = super()._regime_block_reason(
            history=history,
            timeframe=timeframe,
            config=config,
            metadata=metadata,
        )
        if baseline_reason is not None:
            return baseline_reason
        if not config.require_trend_not_overextended:
            return None

        one_hour_closes = self._one_hour_closes(history=history, timeframe=timeframe, metadata=metadata)
        if len(one_hour_closes) < max(config.trend_extension_ema_short_period, config.trend_extension_ema_long_period):
            return "insufficient_history"

        current_close = one_hour_closes[-1]
        ema20 = self._ema_series(one_hour_closes, config.trend_extension_ema_short_period)[-1]
        ema50 = self._ema_series(one_hour_closes, config.trend_extension_ema_long_period)[-1]
        if current_close > ema20 * (ONE + self._decimal(config.max_close_above_ema20_1h_pct)):
            return "trend_too_extended"
        if current_close > ema50 * (ONE + self._decimal(config.max_close_above_ema50_1h_pct)):
            return "trend_too_extended"
        return None

    def _detect_impulse(
        self,
        impulse_window: Sequence[BacktestCandle],
        config: PullbackInTrendV2Config,
    ) -> dict[str, object]:
        lows = [self._decimal(bar.low) for bar in impulse_window]
        highs = [self._decimal(bar.high) for bar in impulse_window]
        low_index = min(range(len(impulse_window)), key=lambda index: lows[index])
        high_index = low_index + max(
            range(len(impulse_window[low_index:])),
            key=lambda offset: highs[low_index + offset],
        )
        if high_index <= low_index:
            return {
                "reason": "no_recent_impulse",
                "detail": "impulse_return_below_threshold",
            }

        impulse_segment = impulse_window[low_index : high_index + 1]
        impulse_low = lows[low_index]
        impulse_high = highs[high_index]
        impulse_range = impulse_high - impulse_low
        impulse_return_pct = self._ratio(impulse_range, impulse_low)
        impulse_start_bar = impulse_window[low_index]
        impulse_end_bar = impulse_window[high_index]
        start_open = self._decimal(impulse_start_bar.open)
        end_open = self._decimal(impulse_end_bar.open)
        end_close = self._decimal(impulse_end_bar.close)
        impulse_body_pct = self._ratio(end_close - start_open, start_open)

        if impulse_low <= ZERO or impulse_range <= ZERO or impulse_return_pct < self._decimal(config.impulse_min_return_pct):
            return {
                "reason": "no_recent_impulse",
                "detail": "impulse_return_below_threshold",
                "impulse_low": impulse_low,
                "impulse_high": impulse_high,
                "impulse_return_pct": impulse_return_pct,
            }

        if len(impulse_segment) > config.impulse_max_bars or impulse_body_pct < self._decimal(config.impulse_min_body_pct):
            return {
                "reason": "impulse_too_weak",
                "detail": "impulse_too_weak",
                "impulse_low": impulse_low,
                "impulse_high": impulse_high,
                "impulse_return_pct": impulse_return_pct,
                "impulse_body_pct": impulse_body_pct,
            }

        end_high = self._decimal(impulse_end_bar.high)
        body_size = abs(end_close - end_open)
        upper_wick = end_high - max(end_open, end_close)
        upper_wick_ratio = Decimal("999") if body_size <= ZERO else upper_wick / body_size
        if upper_wick_ratio > self._decimal(config.impulse_max_upper_wick_ratio):
            return {
                "reason": "impulse_too_noisy",
                "detail": "impulse_too_noisy",
                "impulse_low": impulse_low,
                "impulse_high": impulse_high,
                "upper_wick_ratio": upper_wick_ratio,
            }

        return {
            "reason": None,
            "detail": None,
            "impulse_low": impulse_low,
            "impulse_high": impulse_high,
            "impulse_range": impulse_range,
            "impulse_return_pct": impulse_return_pct,
            "impulse_segment": impulse_segment,
        }

    def _detect_pullback(
        self,
        pullback_window: Sequence[BacktestCandle],
        pullback_ema_values: Sequence[Decimal],
        impulse: Mapping[str, object],
        config: PullbackInTrendV2Config,
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
        retracement_touched = retracement_ratio >= self._decimal(config.min_impulse_retrace_ratio)
        reference_detected = (
            ema_touched
            if config.pullback_reference_mode == "ema20"
            else retracement_touched
            if config.pullback_reference_mode == "impulse_retrace"
            else ema_touched or retracement_touched
        )
        if pullback_depth_pct < self._decimal(config.min_pullback_pct) or not reference_detected:
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
            pullback_depth_pct > self._decimal(config.max_pullback_pct)
            or retracement_ratio > self._decimal(config.max_impulse_retrace_ratio)
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

    def _config_from_context(self, context: StrategyContext) -> PullbackInTrendV2Config:
        config = context.metadata.get("config")
        if isinstance(config, PullbackInTrendV2Config):
            return config
        if isinstance(config, BaseStrategyConfig):
            return self.parse_config(config.model_dump())  # type: ignore[arg-type]
        if isinstance(config, dict):
            return self.parse_config(config)
        return self.parse_config({})

    def _one_hour_closes(
        self,
        history: Sequence[BacktestCandle],
        timeframe: str,
        metadata: Mapping[str, object] | None,
    ) -> tuple[Decimal, ...]:
        if metadata is not None:
            raw = metadata.get("one_hour_closes")
            if isinstance(raw, Iterable) and not isinstance(raw, (str, bytes)):
                closes = tuple(self._decimal(value) for value in raw)
                if closes:
                    return closes
        return tuple(self._decimal(bar.close) for bar in self._aggregate_to_one_hour(history, timeframe))

    def _average_range(self, candles: Sequence[BacktestCandle]) -> Decimal:
        if not candles:
            return ZERO
        total = sum((self._decimal(bar.high) - self._decimal(bar.low) for bar in candles), ZERO)
        return total / Decimal(len(candles))

    def _close_location(self, high: Decimal, low: Decimal, close: Decimal) -> Decimal:
        range_size = high - low
        if range_size <= ZERO:
            return ZERO
        return (close - low) / range_size

    def _signal_metadata(self, payload: Mapping[str, object]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in payload.items():
            if isinstance(value, Decimal):
                result[key] = float(value)
        return result

    def _string_or_none(self, value: object) -> str | None:
        if value is None:
            return None
        return str(value)
