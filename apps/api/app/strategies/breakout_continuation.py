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

StopMode = Literal["breakout_candle_low", "range_high_minus_buffer", "hybrid"]
TargetMode = Literal["stop_multiple", "fixed_pct"]


class BreakoutContinuationConfig(BaseStrategyConfig):
    range_lookback_bars: int = Field(default=12, ge=4)
    max_range_width_pct: float = Field(default=0.018, gt=0)
    require_breakout_bar_green: bool = True
    min_breakout_bar_body_pct: float = Field(default=0.0015, ge=0)
    max_breakout_bar_body_pct: float = Field(default=0.012, gt=0)
    max_breakout_extension_pct: float = Field(default=0.004, gt=0)
    atr_period: int = Field(default=14, ge=2)
    stop_mode: StopMode = "hybrid"
    stop_buffer_atr_mult: float = Field(default=0.15, ge=0)
    max_stop_pct: float = Field(default=0.025, gt=0)
    target_mode: TargetMode = "stop_multiple"
    target_r_multiple: float = Field(default=1.0, gt=0)
    fixed_target_pct: float = Field(default=0.01, gt=0)
    max_bars_in_trade: int = Field(default=12, ge=1)
    breakout_failure_buffer_pct: float = Field(default=0.0, ge=0)
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


class BreakoutContinuationStrategy(BaseStrategy):
    key = "breakout_continuation"
    name = "BreakoutContinuation"
    description = "Research strategy for long-only breakout continuation after a local base."
    status = "implemented"
    config_model = BreakoutContinuationConfig
    runtime_indicator_cache_enabled = True

    def required_history_bars(
        self,
        timeframe: str,
        strategy_config: BreakoutContinuationConfig | None = None,
    ) -> int:
        config = strategy_config or self.config_model()
        bars_for_setup = max(
            config.range_lookback_bars + 1,
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
        if len(history) < self.required_history_bars(context.timeframe, config):
            return self._hold("insufficient_history")
        if context.metadata.get("has_position"):
            return self._exit_signal(context=context, history=history, config=config)
        return self._entry_signal(context=context, history=history, config=config)

    def _entry_signal(
        self,
        context: StrategyContext,
        history: Sequence[BacktestCandle],
        config: BreakoutContinuationConfig,
    ) -> StrategySignal:
        current = history[-1]
        prior_range = history[-(config.range_lookback_bars + 1) : -1]
        range_high = max(self._decimal(bar.high) for bar in prior_range)
        range_low = min(self._decimal(bar.low) for bar in prior_range)
        range_width_pct = self._ratio(range_high - range_low, range_high)

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
                breakout_level=range_high,
                range_width_pct=range_width_pct,
            )

        if range_width_pct > self._decimal(config.max_range_width_pct):
            return self._hold(
                "range_not_tight_enough",
                detail="range_not_tight_enough",
                breakout_level=range_high,
                range_width_pct=range_width_pct,
            )

        close_price = self._decimal(current.close)
        open_price = self._decimal(current.open)
        breakout_level = range_high
        if close_price <= breakout_level:
            return self._hold(
                "breakout_not_confirmed",
                detail="breakout_not_confirmed",
                breakout_level=breakout_level,
                range_width_pct=range_width_pct,
            )

        if config.require_breakout_bar_green and close_price <= open_price:
            return self._hold(
                "breakout_bar_not_green",
                detail="breakout_bar_not_green",
                breakout_level=breakout_level,
            )

        breakout_bar_body_pct = self._ratio(close_price - open_price, open_price)
        if breakout_bar_body_pct < self._decimal(config.min_breakout_bar_body_pct):
            return self._hold(
                "breakout_bar_too_weak",
                detail="breakout_bar_too_weak",
                breakout_level=breakout_level,
                breakout_bar_body_pct=breakout_bar_body_pct,
            )

        breakout_extension_pct = self._ratio(close_price - breakout_level, breakout_level)
        if (
            breakout_bar_body_pct > self._decimal(config.max_breakout_bar_body_pct)
            or breakout_extension_pct > self._decimal(config.max_breakout_extension_pct)
        ):
            return self._hold(
                "breakout_bar_too_extended",
                detail="breakout_bar_too_extended",
                breakout_level=breakout_level,
                breakout_bar_body_pct=breakout_bar_body_pct,
                breakout_extension_pct=breakout_extension_pct,
            )

        atr_value = self._atr(history, config.atr_period)
        if atr_value is None or atr_value <= ZERO:
            return self._hold("insufficient_history")

        entry_price = close_price
        stop_price = self._stop_price(
            current=current,
            range_high=range_high,
            atr_value=atr_value,
            entry_price=entry_price,
            config=config,
        )
        if stop_price is None:
            return self._hold(
                "max_stop_exceeded",
                detail="invalid_stop_structure",
                breakout_level=breakout_level,
            )

        stop_distance = entry_price - stop_price
        stop_distance_pct = self._ratio(stop_distance, entry_price)
        if stop_distance <= ZERO or stop_distance_pct > self._decimal(config.max_stop_pct):
            return self._hold(
                "max_stop_exceeded",
                detail="max_stop_exceeded",
                breakout_level=breakout_level,
                stop_distance_pct=stop_distance_pct,
            )

        take_profit_price = self._take_profit_price(
            entry_price=entry_price,
            stop_price=stop_price,
            config=config,
        )
        if take_profit_price <= entry_price:
            return self._hold("invalid_target", detail="invalid_target")

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
                    breakout_level=breakout_level,
                    take_profit_pct=take_profit_pct,
                    round_trip_cost_pct=round_trip_cost_pct,
                )

        confidence = self._confidence(
            range_width_pct=range_width_pct,
            breakout_bar_body_pct=breakout_bar_body_pct,
            breakout_extension_pct=breakout_extension_pct,
        )
        return StrategySignal(
            action="enter",
            reason="breakout_continuation_entry",
            confidence=confidence,
            metadata={
                "breakout_level": str(breakout_level),
                "range_high": str(range_high),
                "range_low": str(range_low),
                "range_width_pct": float(range_width_pct),
                "breakout_bar_body_pct": float(breakout_bar_body_pct),
                "breakout_extension_pct": float(breakout_extension_pct),
                "atr": str(atr_value),
                "stop_mode": config.stop_mode,
                "target_mode": config.target_mode,
                "stop_price": str(stop_price),
                "take_profit_price": str(take_profit_price),
            },
        )

    def _exit_signal(
        self,
        context: StrategyContext,
        history: Sequence[BacktestCandle],
        config: BreakoutContinuationConfig,
    ) -> StrategySignal:
        position = context.metadata.get("position") or {}
        entry_metadata = dict(position.get("entry_metadata") or {})
        breakout_level = self._decimal(entry_metadata.get("breakout_level"))
        current_close = self._decimal(history[-1].close)
        bars_held = self._bars_held(history=history, entry_time=position.get("entry_time"))

        if breakout_level > ZERO:
            failure_level = breakout_level * (ONE - self._decimal(config.breakout_failure_buffer_pct))
            if current_close < failure_level:
                return StrategySignal(
                    action="exit",
                    reason="breakout_failure",
                    confidence=0.5,
                    metadata={"breakout_level": str(breakout_level)},
                )

        if bars_held >= config.max_bars_in_trade:
            return StrategySignal(
                action="exit",
                reason="time_stop",
                confidence=0.4,
                metadata={"bars_held": bars_held},
            )

        return StrategySignal(action="hold", reason="position_open")

    def _config_from_context(self, context: StrategyContext) -> BreakoutContinuationConfig:
        config = context.metadata.get("config")
        if isinstance(config, BreakoutContinuationConfig):
            return config
        if isinstance(config, BaseStrategyConfig):
            return self.parse_config(config.model_dump())  # type: ignore[arg-type]
        if isinstance(config, dict):
            return self.parse_config(config)
        return self.parse_config({})

    def _regime_block_reason(
        self,
        history: Sequence[BacktestCandle],
        timeframe: str,
        config: BreakoutContinuationConfig,
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
        config: BreakoutContinuationConfig,
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
        current: BacktestCandle,
        range_high: Decimal,
        atr_value: Decimal,
        entry_price: Decimal,
        config: BreakoutContinuationConfig,
    ) -> Decimal | None:
        buffer_value = atr_value * self._decimal(config.stop_buffer_atr_mult)
        candidates: list[Decimal] = []
        if config.stop_mode in {"breakout_candle_low", "hybrid"}:
            candidates.append(self._decimal(current.low) - buffer_value)
        if config.stop_mode in {"range_high_minus_buffer", "hybrid"}:
            candidates.append(range_high - buffer_value)

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
        config: BreakoutContinuationConfig,
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

    def _confidence(
        self,
        range_width_pct: Decimal,
        breakout_bar_body_pct: Decimal,
        breakout_extension_pct: Decimal,
    ) -> float:
        confidence = Decimal("0.5")
        if range_width_pct <= Decimal("0.01"):
            confidence += Decimal("0.1")
        if breakout_bar_body_pct >= Decimal("0.003"):
            confidence += Decimal("0.1")
        if breakout_extension_pct <= Decimal("0.002"):
            confidence += Decimal("0.1")
        return float(max(Decimal("0.0"), min(Decimal("1.0"), confidence)))

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
