from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import Field

from app.schemas.backtest import BacktestCandle
from app.strategies.base import BaseStrategy, BaseStrategyConfig, StrategyContext, StrategySignal
from app.strategies.registry import register_strategy

ZERO = Decimal("0")
ONE = Decimal("1")
TWO = Decimal("2")


class OndoShortDeltaFadeConfig(BaseStrategyConfig):
    impulse_bars: int = Field(default=3, ge=2, le=8)
    impulse_min_return_pct: float = Field(default=0.02, gt=0)
    breakout_lookback_bars: int = Field(default=8, ge=4, le=48)
    breakout_proximity_pct: float = Field(default=0.003, ge=0, le=0.02)
    ema_period: int = Field(default=20, ge=5, le=80)
    stretch_above_ema_pct: float = Field(default=0.01, gt=0)
    volume_sma_period: int = Field(default=20, ge=5, le=80)
    volume_spike_mult: float = Field(default=1.15, gt=0)
    rejection_close_location_max: float = Field(default=0.55, gt=0, le=1)
    upper_wick_min_range_ratio: float = Field(default=0.20, gt=0, le=1)
    entry_breakdown_pct: float = Field(default=0.0005, ge=0, le=0.02)
    entry_followthrough_close_location_max: float = Field(default=0.35, gt=0, le=1)
    stop_buffer_pct: float = Field(default=0.001, ge=0, le=0.02)
    max_stop_distance_pct: float = Field(default=0.012, gt=0, le=0.05)
    max_gap_up_pct: float = Field(default=0.004, ge=0)
    time_exit_bars: int = Field(default=1, ge=1, le=12)


class OndoShortDeltaFadeStrategy(BaseStrategy):
    key = "ondo_short_delta_fade_v1"
    name = "ONDO Short Delta Fade v4"
    description = (
        "Short-only ONDO rejection proxy with confirmed next-bar breakdown, capped stop geometry, and a one-bar "
        "time exit that is allowed to outrun an overly tight scalp target."
    )
    spot_only = False
    long_only = False
    primary_side = "short"
    status = "experimental"
    config_model = OndoShortDeltaFadeConfig

    symbol = "ONDO-USDT"
    timeframe = "1h"

    def default_config(self) -> dict[str, Any]:
        payload = super().default_config()
        payload.update(
            {
                "symbols": [self.symbol],
                "timeframes": [self.timeframe],
                "position_size_pct": 0.1,
                "stop_loss_pct": 0.0035,
                "take_profit_pct": 0.004,
                "time_exit_bars": 1,
            }
        )
        return payload

    def required_history_bars(
        self,
        timeframe: str,
        strategy_config: OndoShortDeltaFadeConfig | None = None,
    ) -> int:
        config = strategy_config or self.config_model()
        return max(
            config.breakout_lookback_bars + 3,
            config.volume_sma_period + 3,
            config.ema_period + 3,
            config.impulse_bars + 4,
            config.time_exit_bars + 4,
        )

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        config = self.parse_config(context.metadata.get("config", {}))
        if context.symbol != self.symbol or context.timeframe != self.timeframe:
            return StrategySignal(
                action="hold",
                side="short",
                reason="unsupported_stream",
                metadata={
                    "reason_skipped": "unsupported_stream",
                    "skip_reason_detail": f"{context.symbol}:{context.timeframe}",
                },
            )

        history = list(context.metadata.get("history", []))
        if len(history) < self.required_history_bars(context.timeframe, config):
            return StrategySignal(
                action="hold",
                side="short",
                reason="insufficient_history",
                metadata={
                    "reason_skipped": "insufficient_history",
                    "skip_reason_detail": f"history_bars={len(history)}",
                },
            )

        current_candle = history[-1]
        signal_candle = history[-2]
        prior_history = history[:-2]
        if context.metadata.get("has_position"):
            position = context.metadata.get("position") or {}
            bars_held = self._bars_held(position, context.metadata.get("bar_index"), context.timestamp, context.timeframe)
            if bars_held >= config.time_exit_bars:
                return StrategySignal(
                    action="exit",
                    side="short",
                    reason="time_exit",
                    confidence=0.62,
                    metadata={
                        "bars_held": bars_held,
                        "target_horizon_bars": config.time_exit_bars,
                    },
                )
            return StrategySignal(action="hold", side="short", reason="position_open")

        if len(prior_history) < max(config.breakout_lookback_bars, config.volume_sma_period, config.ema_period):
            return StrategySignal(
                action="hold",
                side="short",
                reason="insufficient_history",
                metadata={
                    "reason_skipped": "insufficient_history",
                    "skip_reason_detail": f"prior_history={len(prior_history)}",
                },
            )

        setup = self._match_setup(prior_history=prior_history, signal_candle=signal_candle, config=config)
        if not setup["matched"]:
            return StrategySignal(
                action="hold",
                side="short",
                reason=setup["reason"],
                metadata={
                    "reason_skipped": setup["reason"],
                    "skip_reason_detail": setup["detail"],
                    "match_context": setup["context"],
                },
            )

        signal_close = Decimal(str(signal_candle.close))
        signal_open = Decimal(str(signal_candle.open))
        signal_high = Decimal(str(signal_candle.high))
        signal_low = Decimal(str(signal_candle.low))
        current_open = Decimal(str(current_candle.open))
        max_gap_open = signal_close * (ONE + Decimal(str(config.max_gap_up_pct)))
        if current_open > max_gap_open:
            return StrategySignal(
                action="hold",
                side="short",
                reason="gap_up_too_large",
                metadata={
                    "reason_skipped": "gap_up_too_large",
                    "skip_reason_detail": f"current_open={current_open}",
                    "match_context": setup["context"],
                },
            )

        current_close = Decimal(str(current_candle.close))
        current_open = Decimal(str(current_candle.open))
        current_high = Decimal(str(current_candle.high))
        signal_range = signal_high - signal_low
        if signal_range <= ZERO:
            return StrategySignal(
                action="hold",
                side="short",
                reason="invalid_signal_geometry",
                metadata={
                    "reason_skipped": "invalid_signal_geometry",
                    "skip_reason_detail": f"signal_range={signal_range}",
                    "match_context": setup["context"],
                },
            )

        if current_close >= current_open:
            return StrategySignal(
                action="hold",
                side="short",
                reason="entry_bar_not_red",
                metadata={
                    "reason_skipped": "entry_bar_not_red",
                    "skip_reason_detail": f"current_close={current_close}",
                    "match_context": setup["context"],
                },
            )

        breakdown_threshold = signal_close * (ONE - Decimal(str(config.entry_breakdown_pct)))
        if current_close > breakdown_threshold:
            return StrategySignal(
                action="hold",
                side="short",
                reason="entry_breakdown_missing",
                metadata={
                    "reason_skipped": "entry_breakdown_missing",
                    "skip_reason_detail": f"current_close={current_close}",
                    "match_context": setup["context"],
                },
            )

        current_close_location = (current_close - signal_low) / signal_range
        if current_close_location > Decimal(str(config.entry_followthrough_close_location_max)):
            return StrategySignal(
                action="hold",
                side="short",
                reason="entry_followthrough_too_shallow",
                metadata={
                    "reason_skipped": "entry_followthrough_too_shallow",
                    "skip_reason_detail": f"current_close_location={current_close_location}",
                    "match_context": setup["context"],
                },
            )

        stop_anchor = max(current_high, signal_open)
        stop_price = stop_anchor * (ONE + Decimal(str(config.stop_buffer_pct)))
        stop_distance_pct = (stop_price / current_close) - ONE
        if stop_distance_pct > Decimal(str(config.max_stop_distance_pct)):
            return StrategySignal(
                action="hold",
                side="short",
                reason="stop_too_wide_for_v3",
                metadata={
                    "reason_skipped": "stop_too_wide_for_v3",
                    "skip_reason_detail": f"stop_distance_pct={stop_distance_pct}",
                    "match_context": {
                        **setup["context"],
                        "stop_anchor": float(stop_anchor),
                        "stop_distance_pct": round(float(stop_distance_pct), 6),
                        "entry_close_location": round(float(current_close_location), 6),
                    },
                },
            )

        take_profit_price = current_close * (ONE - Decimal(str(config.take_profit_pct)))
        if take_profit_price <= ZERO or stop_price <= current_close:
            return StrategySignal(
                action="hold",
                side="short",
                reason="invalid_short_risk_geometry",
                metadata={
                    "reason_skipped": "invalid_short_risk_geometry",
                    "skip_reason_detail": f"stop_price={stop_price}",
                    "match_context": setup["context"],
                },
            )

        return StrategySignal(
            action="enter",
            side="short",
            reason="ondo_short_delta_fade",
            confidence=setup["confidence"],
            metadata={
                "pattern_code": "ondo_short_delta_fade",
                "pattern_name": self.name,
                "stop_price": stop_price,
                "take_profit_price": take_profit_price,
                "entry_bar_index": context.metadata.get("bar_index"),
                "match_context": {
                    **setup["context"],
                    "entry_close_location": round(float(current_close_location), 6),
                    "stop_anchor": float(stop_anchor),
                    "stop_distance_pct": round(float(stop_distance_pct), 6),
                },
            },
        )

    def _match_setup(
        self,
        prior_history: list[BacktestCandle],
        signal_candle: BacktestCandle,
        config: OndoShortDeltaFadeConfig,
    ) -> dict[str, Any]:
        signal_close = Decimal(str(signal_candle.close))
        signal_open = Decimal(str(signal_candle.open))
        signal_high = Decimal(str(signal_candle.high))
        signal_low = Decimal(str(signal_candle.low))
        signal_volume = Decimal(str(signal_candle.volume))

        impulse_anchor = Decimal(str(prior_history[-config.impulse_bars].close))
        if impulse_anchor <= ZERO:
            return self._no_match("invalid_impulse_anchor", "impulse_anchor_non_positive", {})
        impulse_return = (signal_close / impulse_anchor) - ONE

        breakout_high = max(Decimal(str(candle.high)) for candle in prior_history[-config.breakout_lookback_bars :])
        breakout_threshold = breakout_high * (ONE - Decimal(str(config.breakout_proximity_pct)))
        if signal_high < breakout_threshold:
            return self._no_match(
                "failed_breakout_missing",
                f"signal_high={signal_high}",
                {"breakout_high": breakout_high, "breakout_threshold": breakout_threshold},
            )

        candle_range = signal_high - signal_low
        if candle_range <= ZERO:
            return self._no_match("flat_signal_bar", "candle_range_zero", {})

        close_location = (signal_close - signal_low) / candle_range
        upper_wick = signal_high - max(signal_open, signal_close)
        upper_wick_ratio = upper_wick / candle_range
        real_body_ratio = abs(signal_close - signal_open) / candle_range

        volume_window = prior_history[-config.volume_sma_period :]
        volume_sma = sum((Decimal(str(candle.volume)) for candle in volume_window), ZERO) / Decimal(len(volume_window))

        ema_source = [Decimal(str(candle.close)) for candle in prior_history[-config.ema_period :]] + [signal_close]
        ema_value = self._ema(ema_source, config.ema_period)
        stretch_above_ema = (signal_close / ema_value) - ONE if ema_value > ZERO else ZERO
        impulse_return = (signal_high / impulse_anchor) - ONE

        context = {
            "impulse_return_pct": round(float(impulse_return), 6),
            "breakout_high": float(breakout_high),
            "breakout_threshold": float(breakout_threshold),
            "signal_range": float(candle_range),
            "close_location": round(float(close_location), 6),
            "upper_wick_ratio": round(float(upper_wick_ratio), 6),
            "real_body_ratio": round(float(real_body_ratio), 6),
            "volume_sma": float(volume_sma),
            "volume_spike_ratio": round(float(signal_volume / volume_sma), 6) if volume_sma > ZERO else 0,
            "stretch_above_ema_pct": round(float(stretch_above_ema), 6),
        }

        if impulse_return < Decimal(str(config.impulse_min_return_pct)):
            return self._no_match("impulse_not_large_enough", f"impulse_return={impulse_return}", context)
        if close_location > Decimal(str(config.rejection_close_location_max)):
            return self._no_match("rejection_close_too_high", f"close_location={close_location}", context)
        if upper_wick_ratio < Decimal(str(config.upper_wick_min_range_ratio)):
            return self._no_match("upper_wick_too_small", f"upper_wick_ratio={upper_wick_ratio}", context)
        if signal_close >= signal_open and close_location > Decimal("0.35"):
            return self._no_match("signal_bar_not_weak_enough", f"signal_close={signal_close}", context)
        if real_body_ratio > Decimal("0.65") and signal_close >= signal_open:
            return self._no_match("signal_body_too_strong", f"real_body_ratio={real_body_ratio}", context)
        if volume_sma <= ZERO or signal_volume < volume_sma * Decimal(str(config.volume_spike_mult)):
            return self._no_match("volume_spike_missing", f"signal_volume={signal_volume}", context)
        if stretch_above_ema < Decimal(str(config.stretch_above_ema_pct)):
            return self._no_match("stretch_above_ema_missing", f"stretch_above_ema={stretch_above_ema}", context)

        confidence = 0.64
        confidence += min(0.08, float(max(ZERO, impulse_return - Decimal(str(config.impulse_min_return_pct))) * Decimal("2")))
        confidence += min(0.08, float(max(ZERO, upper_wick_ratio - Decimal(str(config.upper_wick_min_range_ratio))) * Decimal("0.5")))
        confidence += min(0.05, float(max(ZERO, stretch_above_ema - Decimal(str(config.stretch_above_ema_pct))) * Decimal("2")))

        return {
            "matched": True,
            "reason": "ondo_short_delta_fade_confirmed",
            "detail": "validated",
            "confidence": round(min(0.9, confidence), 4),
            "context": context,
        }

    def _bars_held(self, position: dict[str, Any], current_bar_index: Any, timestamp, timeframe: str) -> int:
        entry_bar_index = position.get("entry_bar_index") or position.get("entry_metadata", {}).get("entry_bar_index")
        if isinstance(entry_bar_index, int) and isinstance(current_bar_index, int):
            return max(0, current_bar_index - entry_bar_index)
        entry_time = position.get("entry_time")
        if entry_time is None or timestamp is None:
            return 0
        seconds = max(0, int((timestamp - entry_time).total_seconds()))
        duration_seconds = self._timeframe_to_seconds(timeframe)
        if duration_seconds <= 0:
            return 0
        return seconds // duration_seconds

    def _ema(self, closes: list[Decimal], period: int) -> Decimal:
        if not closes:
            return ZERO
        smoothing = TWO / Decimal(period + 1)
        value = closes[0]
        for close in closes[1:]:
            value = (close * smoothing) + (value * (ONE - smoothing))
        return value

    def _no_match(self, reason: str, detail: str, context: dict[str, Any]) -> dict[str, Any]:
        return {
            "matched": False,
            "reason": reason,
            "detail": detail,
            "confidence": 0.0,
            "context": context,
        }

    def _timeframe_to_seconds(self, timeframe: str) -> int:
        mapping = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "1h": 3600,
            "4h": 14400,
        }
        return mapping.get(timeframe, 0)


REGISTERED_ONDO_SHORT_STRATEGY = register_strategy(OndoShortDeltaFadeStrategy())
