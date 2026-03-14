from __future__ import annotations

from decimal import Decimal
from typing import Sequence

from pydantic import Field, model_validator

from app.schemas.backtest import BacktestCandle
from app.strategies.base import BaseStrategy, BaseStrategyConfig, StrategyContext, StrategySignal

ZERO = Decimal("0")
ONE = Decimal("1")


class MeanReversionHardStopConfig(BaseStrategyConfig):
    lookback_period: int = Field(default=30, ge=3, le=300)
    hard_stop_pct: float = Field(default=0.025, gt=0, lt=1)
    stop_loss_pct: float = Field(default=0.025, gt=0, lt=1)
    take_profit_pct: float = Field(default=0.04, ge=0, lt=2)
    entry_deviation_pct: float = Field(default=0.006, gt=0, lt=0.2)
    exit_deviation_pct: float = Field(default=0.0015, ge=0, lt=0.1)
    min_bounce_pct: float = Field(default=0.001, ge=0, lt=0.1)

    @model_validator(mode="after")
    def sync_stop_fields(self) -> "MeanReversionHardStopConfig":
        # Keep the explicit hard-stop field aligned with the generic risk config
        # consumed by the existing risk engine.
        self.stop_loss_pct = self.hard_stop_pct
        return self


class MeanReversionHardStopStrategy(BaseStrategy):
    key = "mean_reversion_hard_stop"
    name = "MeanReversionHardStop"
    description = "Mean reversion entries after oversold bounces with one hard stop and no DCA."
    status = "implemented"
    config_model = MeanReversionHardStopConfig

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        history = self._history_from_context(context)
        config = self._config_from_context(context)
        if len(history) < config.lookback_period + 1:
            return StrategySignal(action="hold", reason="insufficient_history")

        closes = [self._as_decimal(candle.close) for candle in history]
        previous_close = closes[-2]
        current_close = closes[-1]
        if previous_close <= ZERO or current_close <= ZERO:
            return StrategySignal(action="hold", reason="invalid_prices")

        previous_mean = self._mean(closes[-config.lookback_period - 1 : -1])
        current_mean = self._mean(closes[-config.lookback_period :])
        if previous_mean <= ZERO or current_mean <= ZERO:
            return StrategySignal(action="hold", reason="invalid_moving_average")

        has_position = bool(context.metadata.get("has_position"))
        position = context.metadata.get("position") or {}
        entry_threshold = self._as_decimal(config.entry_deviation_pct)
        exit_threshold = self._as_decimal(config.exit_deviation_pct)
        min_bounce = self._as_decimal(config.min_bounce_pct)

        deviation_pct = (previous_mean - previous_close) / previous_mean
        bounce_pct = (current_close - previous_close) / previous_close

        if not has_position:
            entry_level = previous_mean * (ONE - entry_threshold)
            if previous_close <= entry_level and current_close > previous_close and bounce_pct >= min_bounce:
                confidence = self._bounded_ratio(deviation_pct, entry_threshold)
                return StrategySignal(
                    action="enter",
                    side="long",
                    reason="oversold_bounce_entry",
                    confidence=confidence,
                    metadata={
                        "previous_close": str(previous_close),
                        "current_close": str(current_close),
                        "moving_average": str(previous_mean),
                        "deviation_pct": str(deviation_pct),
                        "bounce_pct": str(bounce_pct),
                    },
                )

            return StrategySignal(action="hold", reason="entry_conditions_not_met")

        entry_price = self._as_decimal(position.get("entry_price", current_close))
        exit_level = current_mean * (ONE - exit_threshold)
        if current_close >= exit_level and current_close >= entry_price:
            confidence = self._bounded_ratio(current_close - exit_level, current_mean)
            return StrategySignal(
                action="exit",
                side="long",
                reason="mean_reversion_complete",
                confidence=confidence,
                metadata={
                    "entry_price": str(entry_price),
                    "current_close": str(current_close),
                    "moving_average": str(current_mean),
                    "exit_level": str(exit_level),
                },
            )

        # If price bounces into profit but immediately starts rolling over, exit early
        # instead of waiting for a perfect touch of the mean.
        if current_close > entry_price and current_close < previous_close:
            confidence = self._bounded_ratio(current_close - entry_price, entry_price)
            return StrategySignal(
                action="exit",
                side="long",
                reason="rebound_stalling",
                confidence=confidence,
                metadata={
                    "entry_price": str(entry_price),
                    "current_close": str(current_close),
                    "previous_close": str(previous_close),
                },
            )

        return StrategySignal(action="hold", reason="position_open_waiting_for_reversion")

    def _config_from_context(self, context: StrategyContext) -> MeanReversionHardStopConfig:
        payload = context.metadata.get("config")
        if isinstance(payload, MeanReversionHardStopConfig):
            return payload
        if hasattr(payload, "model_dump"):
            return self.parse_config(payload.model_dump())
        if isinstance(payload, dict):
            return self.parse_config(payload)
        return self.parse_config()

    def _history_from_context(self, context: StrategyContext) -> list[BacktestCandle]:
        raw_history = context.metadata.get("history") or []
        return list(raw_history)

    def _mean(self, values: Sequence[Decimal]) -> Decimal:
        if not values:
            return ZERO
        return sum(values, ZERO) / Decimal(len(values))

    def _as_decimal(self, value: object) -> Decimal:
        return Decimal(str(value))

    def _bounded_ratio(self, numerator: Decimal, denominator: Decimal) -> float:
        if denominator <= ZERO or numerator <= ZERO:
            return 0.0
        ratio = numerator / denominator
        if ratio > ONE:
            ratio = ONE
        return float(ratio)
