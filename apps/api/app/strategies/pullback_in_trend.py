from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from statistics import pstdev
from typing import Iterable, Literal, Mapping, Sequence

from pydantic import Field

from app.schemas.backtest import BacktestCandle
from app.strategies.base import BaseStrategy, BaseStrategyConfig, StrategyContext, StrategySignal

ZERO = Decimal("0")
ONE = Decimal("1")
TWO = Decimal("2")
HUNDRED = Decimal("100")

PullbackReferenceMode = Literal["ema20", "impulse_retrace", "either"]
TriggerMode = Literal["close_above_ema20", "break_prev_high", "either"]
StopMode = Literal["pullback_low", "trigger_low", "hybrid"]
TargetMode = Literal["stop_multiple", "fixed_pct"]


class PullbackInTrendConfig(BaseStrategyConfig):
    impulse_lookback_bars: int = Field(default=24, ge=4)
    impulse_min_return_pct: float = Field(default=0.006, ge=0)
    pullback_lookback_bars: int = Field(default=8, ge=2)
    pullback_ema_period: int = Field(default=20, ge=2)
    pullback_reference_mode: PullbackReferenceMode = "ema20"
    pullback_touch_tolerance_pct: float = Field(default=0.0015, ge=0)
    min_pullback_pct: float = Field(default=0.001, ge=0)
    max_pullback_pct: float = Field(default=0.02, gt=0)
    min_impulse_retrace_ratio: float = Field(default=0.2, ge=0, le=1)
    max_impulse_retrace_ratio: float = Field(default=0.65, gt=0, le=1)
    trigger_mode: TriggerMode = "close_above_ema20"
    require_trigger_green: bool = True
    trigger_min_body_pct: float = Field(default=0.0005, ge=0)
    atr_period: int = Field(default=14, ge=2)
    stop_mode: StopMode = "pullback_low"
    stop_buffer_atr_mult: float = Field(default=0.1, ge=0)
    max_stop_pct: float = Field(default=0.02, gt=0)
    target_mode: TargetMode = "stop_multiple"
    target_r_multiple: float = Field(default=1.0, gt=0)
    fixed_target_pct: float = Field(default=0.01, gt=0)
    max_bars_in_trade: int = Field(default=12, ge=1)
    pullback_failure_buffer_pct: float = Field(default=0.0, ge=0)
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


class PullbackInTrendStrategy(BaseStrategy):
    key = "pullback_in_trend"
    name = "PullbackInTrend"
    description = "Long-only continuation strategy entering controlled pullbacks inside a healthy uptrend."
    status = "implemented"
    config_model = PullbackInTrendConfig
    runtime_indicator_cache_enabled = True
    debug_counter_keys = (
        "impulse_candidate_count",
        "pullback_candidate_count",
        "trigger_confirmed_count",
        "entry_signal_count",
    )

    def required_history_bars(
        self,
        timeframe: str,
        strategy_config: PullbackInTrendConfig | None = None,
    ) -> int:
        config = strategy_config or self.config_model()
        bars_for_setup = max(
            config.impulse_lookback_bars + config.pullback_lookback_bars + 2,
            config.pullback_ema_period + config.pullback_lookback_bars + 2,
            config.atr_period + 2,
            config.max_bars_in_trade + 2,
        )
        if not config.regime_filter_enabled:
            return bars_for_setup

        bars_per_hour = self._bars_per_hour(timeframe)
        regime_hour_bars = max(
            config.regime_ema_period + 2,
            config.regime_atr_period + 2,
            (config.htf_rsi_period + 2) if config.min_htf_rsi > 0 else 0,
            (config.downside_volatility_lookback * 2) + 2 if config.filter_expanding_downside_volatility else 0,
        )
        return max(bars_for_setup, regime_hour_bars * bars_per_hour)

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        config = self._config_from_context(context)
        raw_history = context.metadata.get("history", [])
        history = raw_history if isinstance(raw_history, Sequence) else list(raw_history)

        if not config.enabled:
            return self._hold("disabled")

        required_history = self.required_history_bars(context.timeframe, config)
        if len(history) < required_history:
            return self._hold(
                "insufficient_history",
                debug_reject_reason="insufficient_lookback",
                debug_reject_detail="pullback_in_trend_not_ready",
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
        config: PullbackInTrendConfig,
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

        regime_block_reason = self._regime_block_reason(
            history=history,
            timeframe=context.timeframe,
            config=config,
            metadata=context.metadata,
        )
        if regime_block_reason is not None:
            return self._hold("regime_blocked", detail=regime_block_reason)

        impulse_low = min(self._decimal(bar.low) for bar in impulse_window)
        impulse_high = max(self._decimal(bar.high) for bar in impulse_window)
        impulse_range = impulse_high - impulse_low
        impulse_return_pct = self._ratio(impulse_range, impulse_low)
        if impulse_low <= ZERO or impulse_range <= ZERO or impulse_return_pct < self._decimal(config.impulse_min_return_pct):
            return self._hold(
                "no_recent_impulse",
                detail="impulse_return_below_threshold",
                impulse_return_pct=impulse_return_pct,
                impulse_high=impulse_high,
                impulse_low=impulse_low,
            )

        impulse_counters = {"impulse_candidate_count": 1}
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
            return self._hold(
                "pullback_not_detected",
                detail="pullback_reference_not_reached",
                debug_setup_detected=True,
                debug_strategy_counters_delta=impulse_counters,
                debug_reject_reason="no_entry_confirmation",
                debug_reject_detail="pullback_reference_not_reached",
                impulse_return_pct=impulse_return_pct,
                pullback_depth_pct=pullback_depth_pct,
                retracement_ratio=retracement_ratio,
            )

        pullback_counters = {
            "impulse_candidate_count": 1,
            "pullback_candidate_count": 1,
        }
        if (
            pullback_low <= impulse_low
            or pullback_depth_pct > self._decimal(config.max_pullback_pct)
            or retracement_ratio > self._decimal(config.max_impulse_retrace_ratio)
        ):
            return self._hold(
                "pullback_too_deep",
                detail="pullback_too_deep",
                debug_setup_detected=True,
                debug_strategy_counters_delta=pullback_counters,
                debug_reject_reason="no_entry_confirmation",
                debug_reject_detail="pullback_too_deep",
                pullback_depth_pct=pullback_depth_pct,
                retracement_ratio=retracement_ratio,
                pullback_low=pullback_low,
                impulse_low=impulse_low,
            )

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
            trigger_via_ema
            if config.trigger_mode == "close_above_ema20"
            else trigger_via_prev_high
            if config.trigger_mode == "break_prev_high"
            else trigger_via_ema or trigger_via_prev_high
        )

        if not trigger_confirmed:
            return self._hold(
                "trigger_not_confirmed",
                detail="trigger_conditions_not_met",
                debug_setup_detected=True,
                debug_strategy_counters_delta=pullback_counters,
                debug_reject_reason="no_entry_confirmation",
                debug_reject_detail="trigger_conditions_not_met",
                trigger_close=trigger_close,
                current_ema=current_ema,
                previous_high=previous_high,
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

        atr_value = self._atr(history, config.atr_period)
        if atr_value is None or atr_value <= ZERO:
            return self._hold("insufficient_history")

        entry_price = trigger_close
        trigger_counters = {
            "impulse_candidate_count": 1,
            "pullback_candidate_count": 1,
            "trigger_confirmed_count": 1,
        }
        stop_price = self._stop_price(
            pullback_low=pullback_low,
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
            round_trip_cost_pct = (fee_rate + slippage_rate) * TWO
            minimum_edge = round_trip_cost_pct * self._decimal(config.min_tp_cost_multiple)
            if take_profit_pct <= minimum_edge:
                return self._hold(
                    "insufficient_tp_vs_cost",
                    detail="insufficient_tp_vs_cost",
                    debug_setup_detected=True,
                    debug_strategy_counters_delta=trigger_counters,
                    take_profit_pct=take_profit_pct,
                    round_trip_cost_pct=round_trip_cost_pct,
                )

        return StrategySignal(
            action="enter",
            reason="pullback_in_trend_entry",
            confidence=0.6,
            metadata={
                "impulse_high": str(impulse_high),
                "impulse_low": str(impulse_low),
                "impulse_return_pct": float(impulse_return_pct),
                "pullback_low": str(pullback_low),
                "pullback_depth_pct": float(pullback_depth_pct),
                "retracement_ratio": float(retracement_ratio),
                "current_ema": str(current_ema),
                "atr": str(atr_value),
                "stop_mode": config.stop_mode,
                "target_mode": config.target_mode,
                "trigger_mode": config.trigger_mode,
                "stop_price": str(stop_price),
                "take_profit_price": str(take_profit_price),
                "debug_setup_detected": True,
                "debug_strategy_counters_delta": {
                    "impulse_candidate_count": 1,
                    "pullback_candidate_count": 1,
                    "trigger_confirmed_count": 1,
                    "entry_signal_count": 1,
                },
            },
        )

    def _exit_signal(
        self,
        context: StrategyContext,
        history: Sequence[BacktestCandle],
        config: PullbackInTrendConfig,
    ) -> StrategySignal:
        position = context.metadata.get("position") or {}
        entry_metadata = dict(position.get("entry_metadata") or {})
        pullback_low = self._decimal(entry_metadata.get("pullback_low"))
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
        config: PullbackInTrendConfig,
        metadata: Mapping[str, object] | None = None,
    ) -> str | None:
        if not config.regime_filter_enabled:
            return None

        cache_handled, cached_reason = self._cached_regime_block_reason(config=config, metadata=metadata)
        if cache_handled:
            return cached_reason

        one_hour_history = self._aggregate_to_one_hour(history, timeframe)
        closes = [self._decimal(candle.close) for candle in one_hour_history]
        if len(closes) < max(config.regime_ema_period, config.regime_atr_period + 1, 3):
            return "insufficient_history"

        ema_series = self._ema_series(closes, config.regime_ema_period)
        current_ema = ema_series[-1]
        previous_ema = ema_series[-2] if len(ema_series) > 1 else current_ema
        current_close = closes[-1]
        if config.require_close_above_ema200_1h and current_close <= current_ema:
            return "close_below_ema200_1h"

        ema_slope = self._ratio(current_ema - previous_ema, previous_ema)
        if ema_slope < self._decimal(config.min_ema_slope):
            return "ema200_slope_below_threshold"

        atr_value = self._atr(one_hour_history, config.regime_atr_period)
        if atr_value is not None and current_close > ZERO:
            atr_pct = atr_value / current_close
            if atr_pct < self._decimal(config.min_atr_pct_1h):
                return "atr_pct_below_min"
            if atr_pct > self._decimal(config.max_atr_pct_1h):
                return "atr_pct_above_max"

        if config.min_htf_rsi > 0:
            rsi_value = self._rsi(closes, config.htf_rsi_period)
            if rsi_value is None:
                return "insufficient_history"
            if rsi_value < self._decimal(config.min_htf_rsi):
                return "htf_rsi_below_threshold"

        if config.filter_expanding_downside_volatility and self._expanding_downside_volatility(
            closes=closes,
            lookback=config.downside_volatility_lookback,
            ratio_threshold=self._decimal(config.downside_volatility_expansion_ratio),
        ):
            return "expanding_downside_volatility"

        return None

    def _cached_regime_block_reason(
        self,
        config: PullbackInTrendConfig,
        metadata: Mapping[str, object] | None,
    ) -> tuple[bool, str | None]:
        if not self.runtime_indicator_cache_enabled or metadata is None:
            return False, None

        snapshot = metadata.get("regime_snapshot")
        if not isinstance(snapshot, Mapping):
            return False, None

        one_hour_bars = self._snapshot_int(snapshot.get("one_hour_bars"))
        current_close = self._snapshot_decimal(snapshot.get("regime_close_1h"))
        current_ema = self._snapshot_decimal(snapshot.get("regime_ema_1h"))
        previous_ema = self._snapshot_decimal(snapshot.get("regime_previous_ema_1h"))
        atr_pct = self._snapshot_decimal(snapshot.get("regime_atr_pct_1h"))
        htf_rsi = self._snapshot_decimal(snapshot.get("regime_rsi_1h"))
        closes_tail = self._snapshot_decimal_sequence(snapshot.get("regime_closes_tail"))

        minimum_bars = max(config.regime_ema_period, config.regime_atr_period + 1, 3)
        if one_hour_bars is None:
            return False, None
        if one_hour_bars < minimum_bars:
            return True, "insufficient_history"
        if current_close is None or current_ema is None or previous_ema is None:
            return False, None

        if config.require_close_above_ema200_1h and current_close <= current_ema:
            return True, "close_below_ema200_1h"

        ema_slope = self._ratio(current_ema - previous_ema, previous_ema)
        if ema_slope < self._decimal(config.min_ema_slope):
            return True, "ema200_slope_below_threshold"

        if atr_pct is None:
            return False, None
        if atr_pct < self._decimal(config.min_atr_pct_1h):
            return True, "atr_pct_below_min"
        if atr_pct > self._decimal(config.max_atr_pct_1h):
            return True, "atr_pct_above_max"

        if config.min_htf_rsi > 0:
            if htf_rsi is None:
                if one_hour_bars <= config.htf_rsi_period:
                    return True, "insufficient_history"
                return False, None
            if htf_rsi < self._decimal(config.min_htf_rsi):
                return True, "htf_rsi_below_threshold"

        if config.filter_expanding_downside_volatility:
            required_tail = (config.downside_volatility_lookback * 2) + 1
            if len(closes_tail) < required_tail:
                return True, None
            if self._expanding_downside_volatility(
                closes=closes_tail,
                lookback=config.downside_volatility_lookback,
                ratio_threshold=self._decimal(config.downside_volatility_expansion_ratio),
            ):
                return True, "expanding_downside_volatility"

        return True, None

    def _stop_price(
        self,
        pullback_low: Decimal,
        trigger_low: Decimal,
        atr_value: Decimal,
        entry_price: Decimal,
        config: PullbackInTrendConfig,
    ) -> Decimal | None:
        buffer_value = atr_value * self._decimal(config.stop_buffer_atr_mult)
        candidates: list[Decimal] = []
        if config.stop_mode in {"pullback_low", "hybrid"}:
            candidates.append(pullback_low - buffer_value)
        if config.stop_mode in {"trigger_low", "hybrid"}:
            candidates.append(trigger_low - buffer_value)

        valid_candidates = [candidate for candidate in candidates if candidate > ZERO and candidate < entry_price]
        if not valid_candidates:
            return None
        if config.stop_mode == "hybrid":
            return max(valid_candidates)
        return valid_candidates[0]

    def _take_profit_price(
        self,
        entry_price: Decimal,
        stop_price: Decimal,
        config: PullbackInTrendConfig,
    ) -> Decimal:
        if config.target_mode == "fixed_pct":
            return entry_price * (ONE + self._decimal(config.fixed_target_pct))
        risk_distance = entry_price - stop_price
        return entry_price + (risk_distance * self._decimal(config.target_r_multiple))

    def _hold(self, reason: str, detail: str | None = None, **metadata: object) -> StrategySignal:
        payload: dict[str, object] = {"reason_skipped": reason}
        if detail is not None:
            payload["skip_reason_detail"] = detail
        for key, value in metadata.items():
            if isinstance(value, Decimal):
                payload[key] = float(value)
            else:
                payload[key] = value
        return StrategySignal(action="hold", reason=reason, metadata=payload)

    def _bars_held(self, history: Sequence[BacktestCandle], entry_time: object) -> int:
        if not isinstance(entry_time, datetime):
            return 0
        return sum(1 for candle in history if candle.open_time > entry_time)

    def _config_from_context(self, context: StrategyContext) -> PullbackInTrendConfig:
        config = context.metadata.get("config")
        if isinstance(config, PullbackInTrendConfig):
            return config
        if isinstance(config, BaseStrategyConfig):
            return self.parse_config(config.model_dump())  # type: ignore[arg-type]
        if isinstance(config, dict):
            return self.parse_config(config)
        return self.parse_config({})

    def _aggregate_to_one_hour(
        self,
        history: Sequence[BacktestCandle],
        timeframe: str,
    ) -> list[BacktestCandle]:
        if timeframe == "1h":
            return list(history)

        one_hour_history: list[BacktestCandle] = []
        for candle in history:
            bucket_time = candle.open_time.replace(minute=0, second=0, microsecond=0)
            if not one_hour_history or one_hour_history[-1].open_time != bucket_time:
                one_hour_history.append(
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

            previous = one_hour_history[-1]
            one_hour_history[-1] = BacktestCandle(
                open_time=previous.open_time,
                open=previous.open,
                high=max(previous.high, self._decimal(candle.high)),
                low=min(previous.low, self._decimal(candle.low)),
                close=self._decimal(candle.close),
                volume=previous.volume + self._decimal(candle.volume),
            )
        return one_hour_history

    def _ema_series(self, values: Sequence[Decimal], period: int) -> list[Decimal]:
        if not values:
            return []
        alpha = TWO / (Decimal(period) + ONE)
        ema_values = [values[0]]
        for value in values[1:]:
            ema_values.append(ema_values[-1] + (value - ema_values[-1]) * alpha)
        return ema_values

    def _rsi(self, values: Sequence[Decimal], period: int) -> Decimal | None:
        if len(values) <= period:
            return None
        gains: list[Decimal] = []
        losses: list[Decimal] = []
        for previous, current in zip(values, values[1:]):
            delta = current - previous
            gains.append(max(delta, ZERO))
            losses.append(abs(min(delta, ZERO)))

        avg_gain = sum(gains[:period], ZERO) / Decimal(period)
        avg_loss = sum(losses[:period], ZERO) / Decimal(period)
        for gain, loss in zip(gains[period:], losses[period:]):
            avg_gain = ((avg_gain * Decimal(period - 1)) + gain) / Decimal(period)
            avg_loss = ((avg_loss * Decimal(period - 1)) + loss) / Decimal(period)

        if avg_loss <= ZERO:
            return HUNDRED
        relative_strength = avg_gain / avg_loss
        return HUNDRED - (HUNDRED / (ONE + relative_strength))

    def _atr(self, candles: Sequence[BacktestCandle], period: int) -> Decimal | None:
        if len(candles) <= period:
            return None
        true_ranges: list[Decimal] = []
        for index in range(1, len(candles)):
            candle = candles[index]
            previous_close = self._decimal(candles[index - 1].close)
            high = self._decimal(candle.high)
            low = self._decimal(candle.low)
            true_ranges.append(
                max(
                    high - low,
                    abs(high - previous_close),
                    abs(low - previous_close),
                )
            )
        atr_window = true_ranges[-period:]
        return sum(atr_window, ZERO) / Decimal(len(atr_window))

    def _expanding_downside_volatility(
        self,
        closes: Sequence[Decimal],
        lookback: int,
        ratio_threshold: Decimal,
    ) -> bool:
        if len(closes) <= (lookback * 2):
            return False
        returns = [self._ratio(current - previous, previous) for previous, current in zip(closes, closes[1:])]
        recent = [float(abs(value)) for value in returns[-lookback:] if value < ZERO]
        prior = [float(abs(value)) for value in returns[-(lookback * 2) : -lookback] if value < ZERO]
        if len(recent) < 2 or len(prior) < 2:
            return False
        recent_vol = Decimal(str(pstdev(recent)))
        prior_vol = Decimal(str(pstdev(prior)))
        if prior_vol <= ZERO:
            return False
        return recent_vol > (prior_vol * ratio_threshold) and returns[-1] < ZERO

    def _bars_per_hour(self, timeframe: str) -> int:
        mapping = {
            "1m": 60,
            "5m": 12,
            "15m": 4,
            "30m": 2,
            "1h": 1,
        }
        return mapping.get(timeframe, 12)

    def _decimal(self, value: object) -> Decimal:
        return Decimal(str(value))

    def _ratio(self, numerator: Decimal, denominator: Decimal) -> Decimal:
        if denominator == ZERO:
            return ZERO
        return numerator / denominator

    def _snapshot_decimal(self, value: object) -> Decimal | None:
        if value is None:
            return None
        return self._decimal(value)

    def _snapshot_decimal_sequence(self, values: object) -> tuple[Decimal, ...]:
        if not isinstance(values, Iterable) or isinstance(values, (str, bytes)):
            return tuple()
        result: list[Decimal] = []
        for value in values:
            if value is None:
                continue
            result.append(self._decimal(value))
        return tuple(result)

    def _snapshot_int(self, value: object) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
