from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from statistics import pstdev
from typing import Literal, Mapping, Sequence

from pydantic import Field

from app.schemas.backtest import BacktestCandle
from app.strategies.base import BaseStrategyConfig, StrategyContext, StrategySignal
from app.strategies.pullback_in_trend import HUNDRED, ONE, PullbackInTrendStrategy, TWO, ZERO

TriggerMode = Literal["wick_reclaim", "first_uptick", "break_prev_high", "wick_reclaim_and_break_prev_high"]
StopMode = Literal["event_low", "trigger_low", "hybrid"]
TargetMode = Literal["stop_multiple", "fixed_pct"]


class RSIMicroBounceV2Config(BaseStrategyConfig):
    rsi_period: int = Field(default=7, ge=2)
    rsi_oversold_threshold: float = Field(default=18, ge=0, le=100)
    oversold_fresh_bars: int = Field(default=2, ge=1)
    require_lower_band_stretch: bool = False
    bb_period: int = Field(default=20, ge=2)
    bb_stddev: float = Field(default=2.0, gt=0)
    ema_period: int = Field(default=20, ge=2)
    stretch_min_pct: float = Field(default=0.002, ge=0)

    context_filter_enabled: bool = True
    require_close_above_ema200_1h: bool = False
    regime_ema_period: int = Field(default=200, ge=2)
    require_atr_cap_1h: bool = True
    regime_atr_period: int = Field(default=14, ge=2)
    max_atr_pct_1h: float = Field(default=0.03, gt=0)
    require_not_in_freefall: bool = True
    freefall_lookback_bars: int = Field(default=6, ge=2)
    freefall_max_drop_pct: float = Field(default=0.015, gt=0)
    filter_expanding_downside_volatility: bool = False
    downside_volatility_lookback: int = Field(default=6, ge=2)
    downside_volatility_expansion_ratio: float = Field(default=1.5, gt=0)

    trigger_mode: TriggerMode = "wick_reclaim"
    require_trigger_green: bool = True
    min_wick_body_ratio: float = Field(default=1.5, gt=0)
    min_close_location: float = Field(default=0.6, ge=0, le=1)
    trigger_min_body_pct: float = Field(default=0.0004, ge=0)

    atr_period: int = Field(default=7, ge=2)
    stop_mode: StopMode = "event_low"
    stop_buffer_atr_mult: float = Field(default=0.1, ge=0)
    max_stop_pct: float = Field(default=0.012, gt=0)
    target_mode: TargetMode = "stop_multiple"
    target_r_multiple: float = Field(default=0.6, gt=0)
    fixed_target_pct: float = Field(default=0.004, gt=0)

    max_bars_in_trade: int = Field(default=4, ge=1)
    fast_failure_bars: int = Field(default=2, ge=1)
    fast_failure_min_progress_r: float = Field(default=0.15, ge=0)

    require_cost_edge: bool = True
    min_tp_cost_multiple: float = Field(default=1.5, gt=0)


class RSIMicroBounceV2Strategy(PullbackInTrendStrategy):
    key = "rsi_micro_bounce_v2"
    name = "RSIMicroBounceV2"
    description = "Very-short-horizon mean reversion after a local RSI flush, with fast monetization and fast failure exits."
    status = "implemented"
    config_model = RSIMicroBounceV2Config
    runtime_indicator_cache_enabled = True
    debug_counter_keys = (
        "context_pass_count",
        "oversold_event_count",
        "fresh_oversold_count",
        "trigger_candidate_count",
        "trigger_confirmed_count",
        "entry_signal_count",
        "fast_failure_exit_count",
    )

    def required_history_bars(
        self,
        timeframe: str,
        strategy_config: RSIMicroBounceV2Config | None = None,
    ) -> int:
        config = strategy_config or self.config_model()
        bars_for_setup = max(
            config.rsi_period + config.oversold_fresh_bars + 3,
            config.ema_period + 2,
            config.bb_period + 2,
            config.atr_period + 2,
            config.max_bars_in_trade + 2,
            config.fast_failure_bars + 2,
        )
        if not config.context_filter_enabled:
            return bars_for_setup

        bars_per_hour = self._bars_per_hour(timeframe)
        regime_hour_bars = max(
            (config.regime_ema_period + 2) if config.require_close_above_ema200_1h else 0,
            (config.regime_atr_period + 2) if config.require_atr_cap_1h else 0,
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
                debug_reject_detail="rsi_micro_bounce_v2_not_ready",
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
        config: RSIMicroBounceV2Config,
    ) -> StrategySignal:
        closes = [self._decimal(candle.close) for candle in history]
        ema_series = self._ema_series(closes, config.ema_period)
        bb_lower_series = self._bb_lower_series(closes, config.bb_period, config.bb_stddev)
        rsi_series = self._rsi_series(closes, config.rsi_period)
        current_index = len(history) - 1

        context_reason = self._context_block_reason(
            history=history,
            timeframe=context.timeframe,
            config=config,
            metadata=context.metadata,
        )
        if context_reason is not None:
            return self._hold("regime_blocked", detail=context_reason)

        context_counters = {"context_pass_count": 1}
        oversold_index = self._find_recent_oversold_index(
            rsi_values=rsi_series,
            current_index=current_index,
            freshness=config.oversold_fresh_bars,
            threshold=self._decimal(config.rsi_oversold_threshold),
        )
        if oversold_index is None:
            older_oversold = self._find_older_oversold_index(
                rsi_values=rsi_series,
                current_index=current_index,
                freshness=config.oversold_fresh_bars,
                threshold=self._decimal(config.rsi_oversold_threshold),
            )
            if older_oversold is not None:
                return self._hold(
                    "oversold_not_fresh",
                    detail="oversold_event_too_old",
                    debug_strategy_counters_delta=context_counters,
                )
            return self._hold(
                "oversold_not_detected",
                detail="rsi_not_low_enough",
                debug_strategy_counters_delta=context_counters,
            )

        event_candle = history[oversold_index]
        event_low = self._decimal(event_candle.low)
        event_close = self._decimal(event_candle.close)
        event_rsi = rsi_series[oversold_index]
        current_ema = ema_series[-1]
        event_ema = ema_series[oversold_index]
        event_lower_band = bb_lower_series[oversold_index]
        oversold_counters = {
            "context_pass_count": 1,
            "oversold_event_count": 1,
        }

        stretch_pct = ZERO
        if event_ema is not None and event_ema > ZERO:
            stretch_pct = self._ratio(max(event_ema - event_close, ZERO), event_ema)
        lower_band_breached = event_lower_band is not None and event_low <= event_lower_band
        if config.require_lower_band_stretch and not (
            lower_band_breached or stretch_pct >= self._decimal(config.stretch_min_pct)
        ):
            return self._hold(
                "stretch_not_large_enough",
                detail="lower_band_not_breached",
                debug_strategy_counters_delta=oversold_counters,
                oversold_rsi=event_rsi,
                stretch_pct=stretch_pct,
            )

        trigger_counters = {
            "context_pass_count": 1,
            "oversold_event_count": 1,
            "fresh_oversold_count": 1,
            "trigger_candidate_count": 1,
        }
        current_bar = history[-1]
        previous_bar = history[-2]
        current_open = self._decimal(current_bar.open)
        current_high = self._decimal(current_bar.high)
        current_low = self._decimal(current_bar.low)
        current_close = self._decimal(current_bar.close)
        previous_close = self._decimal(previous_bar.close)
        previous_high = self._decimal(previous_bar.high)

        body = abs(current_close - current_open)
        body_pct = self._ratio(max(current_close - current_open, ZERO), current_open)
        lower_wick = min(current_open, current_close) - current_low
        wick_body_ratio = self._safe_ratio(lower_wick, body)
        close_location = self._close_location(current_high, current_low, current_close)
        wick_reclaim = (
            wick_body_ratio >= self._decimal(config.min_wick_body_ratio)
            and close_location >= self._decimal(config.min_close_location)
            and current_close > current_ema
        )
        first_uptick = current_close > previous_close
        break_prev_high = current_close > previous_high
        trigger_confirmed = (
            wick_reclaim
            if config.trigger_mode == "wick_reclaim"
            else first_uptick
            if config.trigger_mode == "first_uptick"
            else break_prev_high
            if config.trigger_mode == "break_prev_high"
            else wick_reclaim and break_prev_high
        )
        if not trigger_confirmed:
            return self._hold(
                "trigger_not_confirmed",
                detail=self._trigger_failure_detail(config.trigger_mode),
                debug_setup_detected=True,
                debug_strategy_counters_delta=trigger_counters,
                debug_reject_reason="no_entry_confirmation",
                debug_reject_detail=self._trigger_failure_detail(config.trigger_mode),
                wick_body_ratio=wick_body_ratio,
                close_location=close_location,
            )

        if config.require_trigger_green and current_close <= current_open:
            return self._hold(
                "trigger_bar_too_weak",
                detail="trigger_bar_not_green",
                debug_setup_detected=True,
                debug_strategy_counters_delta=trigger_counters,
                debug_reject_reason="no_entry_confirmation",
                debug_reject_detail="trigger_bar_not_green",
                trigger_body_pct=body_pct,
            )

        if body_pct < self._decimal(config.trigger_min_body_pct):
            return self._hold(
                "trigger_bar_too_weak",
                detail="trigger_bar_too_weak",
                debug_setup_detected=True,
                debug_strategy_counters_delta=trigger_counters,
                debug_reject_reason="no_entry_confirmation",
                debug_reject_detail="trigger_bar_too_weak",
                trigger_body_pct=body_pct,
            )

        if close_location < self._decimal(config.min_close_location):
            return self._hold(
                "trigger_close_not_strong_enough",
                detail="close_not_strong_enough",
                debug_setup_detected=True,
                debug_strategy_counters_delta=trigger_counters,
                debug_reject_reason="no_entry_confirmation",
                debug_reject_detail="close_not_strong_enough",
                close_location=close_location,
            )

        atr_value = self._atr(history, config.atr_period)
        if atr_value is None or atr_value <= ZERO:
            return self._hold("insufficient_history")

        entry_price = current_close
        confirmed_counters = {
            "context_pass_count": 1,
            "oversold_event_count": 1,
            "fresh_oversold_count": 1,
            "trigger_candidate_count": 1,
            "trigger_confirmed_count": 1,
        }
        stop_price = self._stop_price(
            event_low=event_low,
            trigger_low=current_low,
            atr_value=atr_value,
            entry_price=entry_price,
            config=config,
        )
        if stop_price is None:
            return self._hold(
                "max_stop_exceeded",
                detail="invalid_stop_structure",
                debug_setup_detected=True,
                debug_strategy_counters_delta=confirmed_counters,
            )

        stop_distance = entry_price - stop_price
        stop_distance_pct = self._ratio(stop_distance, entry_price)
        if stop_distance <= ZERO or stop_distance_pct > self._decimal(config.max_stop_pct):
            return self._hold(
                "max_stop_exceeded",
                detail="max_stop_exceeded",
                debug_setup_detected=True,
                debug_strategy_counters_delta=confirmed_counters,
                stop_distance_pct=stop_distance_pct,
            )

        take_profit_price = self._take_profit_price(
            entry_price=entry_price,
            stop_price=stop_price,
            config=config,
        )
        if take_profit_price <= entry_price:
            return self._hold(
                "any_other_hold_reason",
                detail="invalid_target",
                debug_setup_detected=True,
                debug_strategy_counters_delta=confirmed_counters,
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
                    debug_strategy_counters_delta=confirmed_counters,
                    take_profit_pct=take_profit_pct,
                    round_trip_cost_pct=round_trip_cost_pct,
                )

        return StrategySignal(
            action="enter",
            reason="rsi_micro_bounce_v2_entry",
            confidence=0.6,
            metadata={
                "oversold_rsi": float(event_rsi) if event_rsi is not None else None,
                "oversold_index": oversold_index,
                "oversold_bar_time": event_candle.open_time.isoformat(),
                "event_low": str(event_low),
                "current_ema": str(current_ema),
                "atr": str(atr_value),
                "stretch_pct": float(stretch_pct),
                "trigger_mode": config.trigger_mode,
                "stop_mode": config.stop_mode,
                "target_mode": config.target_mode,
                "stop_price": str(stop_price),
                "take_profit_price": str(take_profit_price),
                "debug_setup_detected": True,
                "debug_strategy_counters_delta": {
                    "context_pass_count": 1,
                    "oversold_event_count": 1,
                    "fresh_oversold_count": 1,
                    "trigger_candidate_count": 1,
                    "trigger_confirmed_count": 1,
                    "entry_signal_count": 1,
                },
            },
        )

    def _exit_signal(
        self,
        context: StrategyContext,
        history: Sequence[BacktestCandle],
        config: RSIMicroBounceV2Config,
    ) -> StrategySignal:
        position = context.metadata.get("position") or {}
        entry_metadata = dict(position.get("entry_metadata") or {})
        entry_price = self._decimal(position.get("entry_price"))
        stop_price = self._decimal(entry_metadata.get("stop_price"))
        current_close = self._decimal(history[-1].close)
        bars_held = self._bars_held(history=history, entry_time=position.get("entry_time"))

        if bars_held >= config.fast_failure_bars and entry_price > ZERO and stop_price > ZERO and entry_price > stop_price:
            progress_r = self._ratio(current_close - entry_price, entry_price - stop_price)
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

        if bars_held >= config.max_bars_in_trade:
            return StrategySignal(
                action="exit",
                reason="time_stop",
                confidence=0.4,
                metadata={"bars_held": bars_held},
            )

        return StrategySignal(action="hold", reason="position_open")

    def _context_block_reason(
        self,
        history: Sequence[BacktestCandle],
        timeframe: str,
        config: RSIMicroBounceV2Config,
        metadata: Mapping[str, object] | None = None,
    ) -> str | None:
        if not config.context_filter_enabled:
            return None

        cache_handled, cached_reason = self._cached_context_block_reason(config=config, metadata=metadata)
        if cache_handled and cached_reason is not None:
            return cached_reason

        if config.require_not_in_freefall:
            trailing_high = max(
                (self._decimal(candle.close) for candle in history[-config.freefall_lookback_bars :]),
                default=ZERO,
            )
            current_close = self._decimal(history[-1].close)
            if trailing_high > ZERO and self._ratio(trailing_high - current_close, trailing_high) > self._decimal(
                config.freefall_max_drop_pct
            ):
                return "freefall_filter_blocked"

        one_hour_history = self._aggregate_to_one_hour(history, timeframe)
        closes = [self._decimal(candle.close) for candle in one_hour_history]
        minimum_bars = max(
            (config.regime_ema_period + 2) if config.require_close_above_ema200_1h else 0,
            (config.regime_atr_period + 2) if config.require_atr_cap_1h else 0,
            (config.downside_volatility_lookback * 2) + 2 if config.filter_expanding_downside_volatility else 0,
            3,
        )
        if len(closes) < minimum_bars:
            return "insufficient_history"

        current_close = closes[-1]
        if config.require_close_above_ema200_1h:
            ema_series = self._ema_series(closes, config.regime_ema_period)
            if current_close <= ema_series[-1]:
                return "close_below_ema200_1h"

        if config.require_atr_cap_1h:
            atr_value = self._atr(one_hour_history, config.regime_atr_period)
            if atr_value is None:
                return "insufficient_history"
            atr_pct = atr_value / current_close if current_close > ZERO else ZERO
            if atr_pct > self._decimal(config.max_atr_pct_1h):
                return "atr_pct_above_max"

        if config.filter_expanding_downside_volatility and self._expanding_downside_volatility(
            closes=closes,
            lookback=config.downside_volatility_lookback,
            ratio_threshold=self._decimal(config.downside_volatility_expansion_ratio),
        ):
            return "expanding_downside_volatility"

        return None

    def _cached_context_block_reason(
        self,
        config: RSIMicroBounceV2Config,
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
        atr_pct = self._snapshot_decimal(snapshot.get("regime_atr_pct_1h"))
        closes_tail = self._snapshot_decimal_sequence(snapshot.get("regime_closes_tail"))

        minimum_bars = max(
            (config.regime_ema_period + 2) if config.require_close_above_ema200_1h else 0,
            (config.regime_atr_period + 2) if config.require_atr_cap_1h else 0,
            (config.downside_volatility_lookback * 2) + 2 if config.filter_expanding_downside_volatility else 0,
            3,
        )
        if one_hour_bars is None:
            return False, None
        if one_hour_bars < minimum_bars:
            return True, "insufficient_history"

        if config.require_close_above_ema200_1h:
            if current_close is None or current_ema is None:
                return False, None
            if current_close <= current_ema:
                return True, "close_below_ema200_1h"

        if config.require_atr_cap_1h:
            if atr_pct is None:
                return False, None
            if atr_pct > self._decimal(config.max_atr_pct_1h):
                return True, "atr_pct_above_max"

        if config.filter_expanding_downside_volatility:
            required_tail = (config.downside_volatility_lookback * 2) + 1
            if len(closes_tail) < required_tail:
                return True, "insufficient_history"
            if self._expanding_downside_volatility(
                closes=closes_tail,
                lookback=config.downside_volatility_lookback,
                ratio_threshold=self._decimal(config.downside_volatility_expansion_ratio),
            ):
                return True, "expanding_downside_volatility"

        return True, None

    def _find_recent_oversold_index(
        self,
        rsi_values: Sequence[Decimal | None],
        current_index: int,
        freshness: int,
        threshold: Decimal,
    ) -> int | None:
        earliest_index = max(0, current_index - freshness)
        for index in range(current_index, earliest_index - 1, -1):
            rsi_value = rsi_values[index]
            if rsi_value is not None and rsi_value <= threshold:
                return index
        return None

    def _find_older_oversold_index(
        self,
        rsi_values: Sequence[Decimal | None],
        current_index: int,
        freshness: int,
        threshold: Decimal,
    ) -> int | None:
        earliest_recent_index = max(0, current_index - freshness)
        for index in range(earliest_recent_index - 1, -1, -1):
            rsi_value = rsi_values[index]
            if rsi_value is not None and rsi_value <= threshold:
                return index
        return None

    def _trigger_failure_detail(self, mode: TriggerMode) -> str:
        mapping = {
            "wick_reclaim": "wick_reclaim_failed",
            "first_uptick": "first_uptick_failed",
            "break_prev_high": "break_prev_high_failed",
            "wick_reclaim_and_break_prev_high": "wick_reclaim_and_break_prev_high_failed",
        }
        return mapping[mode]

    def _stop_price(
        self,
        event_low: Decimal,
        trigger_low: Decimal,
        atr_value: Decimal,
        entry_price: Decimal,
        config: RSIMicroBounceV2Config,
    ) -> Decimal | None:
        buffer_value = atr_value * self._decimal(config.stop_buffer_atr_mult)
        candidates: list[Decimal] = []
        if config.stop_mode in {"event_low", "hybrid"}:
            candidates.append(event_low - buffer_value)
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
        config: RSIMicroBounceV2Config,
    ) -> Decimal:
        if config.target_mode == "fixed_pct":
            return entry_price * (ONE + self._decimal(config.fixed_target_pct))
        risk_distance = entry_price - stop_price
        return entry_price + (risk_distance * self._decimal(config.target_r_multiple))

    def _rsi_series(self, values: Sequence[Decimal], period: int) -> list[Decimal | None]:
        if not values:
            return []
        series: list[Decimal | None] = [None] * len(values)
        if len(values) <= period:
            return series

        gains: list[Decimal] = []
        losses: list[Decimal] = []
        for previous, current in zip(values, values[1:]):
            delta = current - previous
            gains.append(max(delta, ZERO))
            losses.append(abs(min(delta, ZERO)))

        avg_gain = sum(gains[:period], ZERO) / Decimal(period)
        avg_loss = sum(losses[:period], ZERO) / Decimal(period)
        series[period] = HUNDRED if avg_loss <= ZERO else HUNDRED - (HUNDRED / (ONE + (avg_gain / avg_loss)))

        for offset, (gain, loss) in enumerate(zip(gains[period:], losses[period:]), start=period + 1):
            avg_gain = ((avg_gain * Decimal(period - 1)) + gain) / Decimal(period)
            avg_loss = ((avg_loss * Decimal(period - 1)) + loss) / Decimal(period)
            if avg_loss <= ZERO:
                series[offset] = HUNDRED
            else:
                relative_strength = avg_gain / avg_loss
                series[offset] = HUNDRED - (HUNDRED / (ONE + relative_strength))
        return series

    def _bb_lower_series(
        self,
        values: Sequence[Decimal],
        period: int,
        stddev_mult: float,
    ) -> list[Decimal | None]:
        series: list[Decimal | None] = [None] * len(values)
        if len(values) < period:
            return series
        multiplier = self._decimal(stddev_mult)
        for index in range(period - 1, len(values)):
            window = values[index - period + 1 : index + 1]
            mean_value = sum(window, ZERO) / Decimal(period)
            std_value = Decimal(str(pstdev([float(value) for value in window])))
            series[index] = mean_value - (std_value * multiplier)
        return series

    def _close_location(self, high: Decimal, low: Decimal, close: Decimal) -> Decimal:
        if high <= low:
            return ONE
        return self._ratio(close - low, high - low)

    def _safe_ratio(self, numerator: Decimal, denominator: Decimal) -> Decimal:
        if denominator <= ZERO:
            return HUNDRED
        return numerator / denominator

    def _config_from_context(self, context: StrategyContext) -> RSIMicroBounceV2Config:
        config = context.metadata.get("config")
        if isinstance(config, RSIMicroBounceV2Config):
            return config
        if isinstance(config, BaseStrategyConfig):
            return self.parse_config(config.model_dump())  # type: ignore[arg-type]
        if isinstance(config, dict):
            return self.parse_config(config)
        return self.parse_config({})
