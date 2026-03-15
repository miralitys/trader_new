from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal, Sequence

from pydantic import Field, model_validator

from app.schemas.backtest import BacktestCandle
from app.strategies.base import BaseStrategy, BaseStrategyConfig, StrategyContext, StrategySignal

ZERO = Decimal("0")
ONE = Decimal("1")
HUNDRED = Decimal("100")


@dataclass(frozen=True)
class OversoldEvent:
    index: int
    candle_time: datetime
    low: Decimal
    close: Decimal
    rsi: Decimal


@dataclass(frozen=True)
class StopCandidate:
    stop_mode: str
    price: Decimal
    distance_pct: Decimal


class RSIMicroBounceConfig(BaseStrategyConfig):
    timeframes: list[str] = Field(default_factory=lambda: ["5m"])
    position_size_pct: float = Field(default=0.1, gt=0, le=1)
    stop_loss_pct: float = Field(default=0.015, gt=0, lt=1)
    take_profit_pct: float = Field(default=0, ge=0, lt=1)
    rsi_period: int = Field(default=7, ge=2, le=50)
    rsi_oversold_threshold: float = Field(default=20, gt=0, lt=50)
    oversold_lookback_bars: int = Field(default=2, ge=1, le=20)
    entry_mode: Literal["first_uptick", "two_candle_reversal", "wick_rejection"] = "first_uptick"
    require_green_entry_candle: bool = True
    min_uptick_pct: float = Field(default=0, ge=0, lt=1)
    wick_rejection_min_close_location: float = Field(default=0.6, gt=0, lt=1)
    wick_rejection_min_wick_body_ratio: float = Field(default=1.5, ge=0, lt=20)
    wick_rejection_min_body_atr: float = Field(default=0.05, ge=0, lt=10)
    atr_period: int = Field(default=7, ge=2, le=50)
    stop_mode: Literal["signal_low", "oversold_low", "lookback_low"] = "signal_low"
    stop_lookback_bars: int = Field(default=3, ge=1, le=20)
    stop_atr_buffer: float = Field(default=0.1, ge=0, lt=10)
    max_stop_pct: float = Field(default=0.015, gt=0, lt=1)
    target_mode: Literal["stop_multiple", "atr_multiple", "fixed_pct"] = "stop_multiple"
    target_r_multiple: float = Field(default=0.5, gt=0, lt=10)
    target_atr_multiple: float = Field(default=0.75, gt=0, lt=10)
    target_fixed_pct: float = Field(default=0.004, gt=0, lt=1)
    require_cost_edge: bool = True
    cost_multiplier: float = Field(default=2.0, gt=0, lt=10)
    max_bars_in_trade: int = Field(default=6, ge=1, le=200)
    soft_context_filter_enabled: bool = True
    require_atr_cap: bool = True
    atr_pct_max: float = Field(default=0.02, gt=0, lt=1)
    require_downside_acceleration_filter: bool = True
    downside_acceleration_lookback: int = Field(default=6, ge=2, le=50)
    downside_acceleration_ratio: float = Field(default=1.2, gt=0, lt=10)

    @model_validator(mode="after")
    def sync_risk_fallbacks(self) -> "RSIMicroBounceConfig":
        self.stop_loss_pct = min(float(self.stop_loss_pct), float(self.max_stop_pct))
        self.take_profit_pct = 0.0
        return self


class RSIMicroBounceStrategy(BaseStrategy):
    key = "rsi_micro_bounce"
    name = "RSIMicroBounce"
    description = "Research strategy for very-short-horizon RSI-driven micro-bounces with early entries and fast monetization."
    status = "implemented"
    config_model = RSIMicroBounceConfig

    def required_history_bars(
        self,
        timeframe: str,
        config: RSIMicroBounceConfig | None = None,
    ) -> int:
        active_config = config or self.parse_config()
        minimum_history = max(
            active_config.rsi_period + active_config.oversold_lookback_bars + 2,
            active_config.atr_period + active_config.stop_lookback_bars + 2,
            4,
        )
        if active_config.soft_context_filter_enabled and active_config.require_downside_acceleration_filter:
            minimum_history = max(minimum_history, (active_config.downside_acceleration_lookback * 2) + 2)
        return minimum_history

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        history = self._history_from_context(context)
        config = self._config_from_context(context)
        bars_seen = int(context.metadata.get("bars_seen", len(history)))
        minimum_history = self.required_history_bars(context.timeframe, config)
        if bars_seen < minimum_history:
            return StrategySignal(action="hold", reason="insufficient_history")

        closes = self._closes_from_context(context, history)
        current_candle = history[-1]
        previous_candle = history[-2]
        current_open = self._as_decimal(current_candle.open)
        current_high = self._as_decimal(current_candle.high)
        current_low = self._as_decimal(current_candle.low)
        current_close = closes[-1]
        previous_close = closes[-2]
        if current_close <= ZERO or previous_close <= ZERO:
            return StrategySignal(action="hold", reason="invalid_prices")

        rsi_series = self._rsi_series(closes, config.rsi_period)
        current_rsi = rsi_series[-1]
        previous_rsi = rsi_series[-2]
        if current_rsi is None or previous_rsi is None:
            return StrategySignal(action="hold", reason="insufficient_rsi_history")

        current_atr = self._atr(history, config.atr_period)
        if current_atr <= ZERO:
            return StrategySignal(action="hold", reason="invalid_atr")

        fee_rate = self._as_decimal(context.metadata.get("fee_rate", ZERO))
        slippage_rate = self._as_decimal(context.metadata.get("slippage_rate", ZERO))
        estimated_cost_pct = fee_rate + fee_rate + slippage_rate + slippage_rate
        has_position = bool(context.metadata.get("has_position"))
        position = context.metadata.get("position") or {}

        if not has_position:
            return self._entry_signal(
                history=history,
                closes=closes,
                current_candle=current_candle,
                previous_candle=previous_candle,
                current_open=current_open,
                current_high=current_high,
                current_low=current_low,
                current_close=current_close,
                previous_close=previous_close,
                previous_rsi=previous_rsi,
                current_rsi=current_rsi,
                rsi_series=rsi_series,
                current_atr=current_atr,
                estimated_cost_pct=estimated_cost_pct,
                config=config,
            )

        return self._exit_signal(
            history=history,
            current_close=current_close,
            position=position,
            current_bar_index=int(context.metadata.get("bar_index", len(history) - 1)),
            config=config,
        )

    def _entry_signal(
        self,
        history: Sequence[BacktestCandle],
        closes: Sequence[Decimal],
        current_candle: BacktestCandle,
        previous_candle: BacktestCandle,
        current_open: Decimal,
        current_high: Decimal,
        current_low: Decimal,
        current_close: Decimal,
        previous_close: Decimal,
        previous_rsi: Decimal,
        current_rsi: Decimal,
        rsi_series: Sequence[Decimal | None],
        current_atr: Decimal,
        estimated_cost_pct: Decimal,
        config: RSIMicroBounceConfig,
    ) -> StrategySignal:
        oversold_event = self._oversold_event(history, rsi_series, config)
        atr_pct = current_atr / current_close if current_close > ZERO else ZERO
        context_ok, context_reason, context_metadata = self._passes_context_filter(
            closes=closes,
            atr_pct=atr_pct,
            config=config,
        )
        trigger_confirmed, trigger_detail, trigger_metadata = self._entry_mode_confirmation(
            history=history,
            closes=closes,
            current_candle=current_candle,
            previous_candle=previous_candle,
            current_open=current_open,
            current_high=current_high,
            current_low=current_low,
            current_close=current_close,
            previous_close=previous_close,
            current_atr=current_atr,
            config=config,
        )
        stop_candidate, stop_reason, stop_metadata = self._select_stop_candidate(
            history=history,
            oversold_event=oversold_event,
            entry_price=current_close,
            atr=current_atr,
            config=config,
        )
        target_price = self._target_price(
            entry_price=current_close,
            atr=current_atr,
            stop_candidate=stop_candidate,
            config=config,
        )
        planned_tp_pct = (target_price - current_close) / current_close if target_price > ZERO else ZERO
        minimum_planned_tp_pct = estimated_cost_pct * self._as_decimal(config.cost_multiplier)

        entry_metadata: dict[str, object] = {
            "stage": "entry_check",
            "setup_type": "rsi_only",
            "event_study_label": "rsi_micro_bounce_candidate",
            "candidate_entry_active": oversold_event is not None,
            "entry_mode": config.entry_mode,
            "entry_mode_confirmed": trigger_confirmed,
            "entry_trigger_detail": trigger_detail,
            "current_open": str(current_open),
            "current_high": str(current_high),
            "current_low": str(current_low),
            "current_close": str(current_close),
            "previous_close": str(previous_close),
            "previous_rsi": str(previous_rsi),
            "current_rsi": str(current_rsi),
            "rsi_period": config.rsi_period,
            "rsi_oversold_threshold": str(config.rsi_oversold_threshold),
            "oversold_detected": oversold_event is not None,
            "oversold_bar_index": oversold_event.index if oversold_event is not None else None,
            "oversold_bar_time": oversold_event.candle_time.isoformat() if oversold_event is not None else None,
            "oversold_bar_low": str(oversold_event.low) if oversold_event is not None else None,
            "oversold_bar_close": str(oversold_event.close) if oversold_event is not None else None,
            "oversold_rsi": str(oversold_event.rsi) if oversold_event is not None else None,
            "oversold_distance_bars": (len(history) - 1 - oversold_event.index) if oversold_event is not None else None,
            "atr": str(current_atr),
            "atr_pct": str(atr_pct),
            "planned_tp_pct": str(planned_tp_pct),
            "estimated_cost_pct": str(estimated_cost_pct),
            "cost_multiplier": str(config.cost_multiplier),
            "require_cost_edge": config.require_cost_edge,
            "target_mode": config.target_mode,
            "target_r_multiple": str(config.target_r_multiple),
            "target_atr_multiple": str(config.target_atr_multiple),
            "target_fixed_pct": str(config.target_fixed_pct),
            "target_price": str(target_price) if target_price > ZERO else None,
            **context_metadata,
            **trigger_metadata,
            **stop_metadata,
        }

        if oversold_event is None:
            return self._hold_signal(
                reason="safety_guard_failed",
                detail="oversold_not_detected",
                metadata=entry_metadata,
            )
        if not trigger_confirmed:
            return self._hold_signal(
                reason="safety_guard_failed",
                detail=trigger_detail,
                metadata=entry_metadata,
            )
        if not context_ok:
            return self._hold_signal(
                reason="regime_blocked",
                detail=context_reason,
                metadata=entry_metadata,
            )
        if stop_candidate is None:
            return self._hold_signal(
                reason=stop_reason or "safety_guard_failed",
                detail="stop_not_usable",
                metadata=entry_metadata,
            )
        if config.require_cost_edge and (planned_tp_pct <= ZERO or planned_tp_pct < minimum_planned_tp_pct):
            return self._hold_signal(
                reason="insufficient_tp_vs_cost",
                detail="planned_tp_too_small",
                metadata={
                    **entry_metadata,
                    "minimum_planned_tp_pct": str(minimum_planned_tp_pct),
                },
            )

        confidence = (
            self._bounded_ratio(current_close - oversold_event.close, current_atr if current_atr > ZERO else ONE)
            + self._bounded_ratio(self._as_decimal(config.rsi_oversold_threshold) - oversold_event.rsi, HUNDRED)
            + self._bounded_ratio(
                planned_tp_pct,
                minimum_planned_tp_pct if minimum_planned_tp_pct > ZERO else ONE,
            )
        ) / 3

        return StrategySignal(
            action="enter",
            side="long",
            reason="rsi_micro_bounce_entry",
            confidence=confidence,
            metadata={
                **entry_metadata,
                "stage": "entry_confirmed",
                "stop_price": str(stop_candidate.price),
                "take_profit_price": str(target_price),
                "distance_to_stop_pct": str(stop_candidate.distance_pct),
                "distance_to_tp_pct": str(planned_tp_pct),
                "stop_mode_used": stop_candidate.stop_mode,
                "reason_skipped": None,
            },
        )

    def _exit_signal(
        self,
        history: Sequence[BacktestCandle],
        current_close: Decimal,
        position: dict[str, object],
        current_bar_index: int,
        config: RSIMicroBounceConfig,
    ) -> StrategySignal:
        entry_price = self._as_decimal(position.get("entry_price", current_close))
        position_take_profit_price = position.get("take_profit_price")
        effective_target_price = (
            self._as_decimal(position_take_profit_price) if position_take_profit_price is not None else ZERO
        )
        entry_time = position.get("entry_time")
        entry_bar_index = position.get("entry_bar_index")
        bars_held = self._bars_held(
            history=history,
            entry_time=entry_time,
            current_bar_index=current_bar_index,
            entry_bar_index=entry_bar_index,
        )

        if effective_target_price > entry_price and current_close >= effective_target_price:
            return StrategySignal(
                action="exit",
                side="long",
                reason="tp",
                confidence=self._bounded_ratio(current_close - effective_target_price, effective_target_price),
                metadata={
                    "entry_price": str(entry_price),
                    "current_close": str(current_close),
                    "target_price": str(effective_target_price),
                    "bars_held": bars_held,
                    "exit_reason_label": "tp",
                },
            )

        if bars_held >= config.max_bars_in_trade:
            return StrategySignal(
                action="exit",
                side="long",
                reason="time_stop",
                confidence=self._bounded_ratio(Decimal(bars_held), Decimal(config.max_bars_in_trade)),
                metadata={
                    "entry_price": str(entry_price),
                    "current_close": str(current_close),
                    "bars_held": bars_held,
                    "max_bars_in_trade": config.max_bars_in_trade,
                    "exit_reason_label": "time_stop",
                },
            )

        return StrategySignal(action="hold", reason="position_open_waiting_for_micro_bounce_exit")

    def _config_from_context(self, context: StrategyContext) -> RSIMicroBounceConfig:
        payload = context.metadata.get("config")
        if isinstance(payload, RSIMicroBounceConfig):
            return payload
        if hasattr(payload, "model_dump"):
            return self.parse_config(payload.model_dump())
        if isinstance(payload, dict):
            return self.parse_config(payload)
        return self.parse_config()

    def _history_from_context(self, context: StrategyContext) -> Sequence[BacktestCandle]:
        raw_history = context.metadata.get("history") or []
        if isinstance(raw_history, Sequence):
            return raw_history
        return tuple(raw_history)

    def _closes_from_context(
        self,
        context: StrategyContext,
        history: Sequence[BacktestCandle],
    ) -> Sequence[Decimal]:
        raw_closes = context.metadata.get("closes")
        if isinstance(raw_closes, Sequence) and len(raw_closes) == len(history):
            return raw_closes
        return [self._as_decimal(candle.close) for candle in history]

    def _hold_signal(
        self,
        reason: str,
        detail: str,
        metadata: dict[str, object],
    ) -> StrategySignal:
        return StrategySignal(
            action="hold",
            reason=reason,
            metadata={
                **metadata,
                "reason_skipped": reason,
                "skip_reason_detail": detail,
            },
        )

    def _oversold_event(
        self,
        history: Sequence[BacktestCandle],
        rsi_series: Sequence[Decimal | None],
        config: RSIMicroBounceConfig,
    ) -> OversoldEvent | None:
        threshold = self._as_decimal(config.rsi_oversold_threshold)
        start_index = max(0, len(history) - 1 - config.oversold_lookback_bars)
        for index in range(len(history) - 2, start_index - 1, -1):
            rsi_value = rsi_series[index]
            if rsi_value is None or rsi_value > threshold:
                continue
            candle = history[index]
            return OversoldEvent(
                index=index,
                candle_time=candle.open_time,
                low=self._as_decimal(candle.low),
                close=self._as_decimal(candle.close),
                rsi=rsi_value,
            )
        return None

    def _entry_mode_confirmation(
        self,
        history: Sequence[BacktestCandle],
        closes: Sequence[Decimal],
        current_candle: BacktestCandle,
        previous_candle: BacktestCandle,
        current_open: Decimal,
        current_high: Decimal,
        current_low: Decimal,
        current_close: Decimal,
        previous_close: Decimal,
        current_atr: Decimal,
        config: RSIMicroBounceConfig,
    ) -> tuple[bool, str, dict[str, object]]:
        green_ok = (not config.require_green_entry_candle) or current_close > current_open
        current_range = current_high - current_low
        uptick_pct = (current_close - previous_close) / previous_close if previous_close > ZERO else ZERO

        if config.entry_mode == "first_uptick":
            confirmed = green_ok and current_close > previous_close and uptick_pct >= self._as_decimal(config.min_uptick_pct)
            return confirmed, "first_uptick_not_confirmed", {
                "entry_green_candle": green_ok,
                "entry_uptick_pct": str(uptick_pct),
            }

        if config.entry_mode == "two_candle_reversal":
            close_two_bars_ago = closes[-3]
            previous_open = self._as_decimal(previous_candle.open)
            previous_green = previous_close > previous_open
            confirmed = (
                green_ok
                and previous_green
                and previous_close > close_two_bars_ago
                and current_close > previous_close
            )
            return confirmed, "two_candle_reversal_not_confirmed", {
                "entry_green_candle": green_ok,
                "previous_green_candle": previous_green,
                "two_bar_reclaim_pct": str(
                    (current_close - close_two_bars_ago) / close_two_bars_ago if close_two_bars_ago > ZERO else ZERO
                ),
            }

        lower_wick = min(current_open, current_close) - current_low
        body = abs(current_close - current_open)
        body_floor = max(current_atr * self._as_decimal(config.wick_rejection_min_body_atr), Decimal("0.00000001"))
        effective_body = max(body, body_floor)
        close_location = (current_close - current_low) / current_range if current_range > ZERO else ZERO
        wick_body_ratio = lower_wick / effective_body if effective_body > ZERO else ZERO
        confirmed = (
            green_ok
            and current_close > previous_close
            and close_location >= self._as_decimal(config.wick_rejection_min_close_location)
            and wick_body_ratio >= self._as_decimal(config.wick_rejection_min_wick_body_ratio)
        )
        return confirmed, "wick_rejection_not_confirmed", {
            "entry_green_candle": green_ok,
            "entry_uptick_pct": str(uptick_pct),
            "wick_close_location": str(close_location),
            "wick_body_ratio": str(wick_body_ratio),
        }

    def _passes_context_filter(
        self,
        closes: Sequence[Decimal],
        atr_pct: Decimal,
        config: RSIMicroBounceConfig,
    ) -> tuple[bool, str, dict[str, object]]:
        if not config.soft_context_filter_enabled:
            return True, "soft_context_disabled", {"soft_context_enabled": False}

        downside_flag, short_downside, long_downside, recent_return = self._is_downside_accelerating(
            closes=closes,
            lookback=config.downside_acceleration_lookback,
            expansion_ratio=self._as_decimal(config.downside_acceleration_ratio),
        )
        metadata = {
            "soft_context_enabled": True,
            "atr_pct": str(atr_pct),
            "require_atr_cap": config.require_atr_cap,
            "require_downside_acceleration_filter": config.require_downside_acceleration_filter,
            "downside_short_mean": str(short_downside),
            "downside_long_mean": str(long_downside),
            "downside_recent_return": str(recent_return),
        }
        if config.require_atr_cap and atr_pct > self._as_decimal(config.atr_pct_max):
            return False, "atr_pct_above_max", metadata
        if config.require_downside_acceleration_filter and downside_flag:
            return False, "downside_acceleration_active", metadata
        return True, "soft_context_passed", metadata

    def _is_downside_accelerating(
        self,
        closes: Sequence[Decimal],
        lookback: int,
        expansion_ratio: Decimal,
    ) -> tuple[bool, Decimal, Decimal, Decimal]:
        if len(closes) < (lookback * 2) + 1:
            return False, ZERO, ZERO, ZERO

        returns: list[Decimal] = []
        for previous_close, current_close in zip(closes[:-1], closes[1:]):
            if previous_close <= ZERO:
                return False, ZERO, ZERO, ZERO
            returns.append((current_close - previous_close) / previous_close)

        short_window = returns[-lookback:]
        long_window = returns[-(lookback * 2) : -lookback]
        short_downside = self._mean([abs(value) if value < ZERO else ZERO for value in short_window])
        long_downside = self._mean([abs(value) if value < ZERO else ZERO for value in long_window])
        recent_return = (closes[-1] - closes[-1 - lookback]) / closes[-1 - lookback]

        if short_downside <= ZERO or recent_return >= ZERO:
            return False, short_downside, long_downside, recent_return
        if long_downside <= ZERO:
            return True, short_downside, long_downside, recent_return
        return short_downside >= long_downside * expansion_ratio, short_downside, long_downside, recent_return

    def _select_stop_candidate(
        self,
        history: Sequence[BacktestCandle],
        oversold_event: OversoldEvent | None,
        entry_price: Decimal,
        atr: Decimal,
        config: RSIMicroBounceConfig,
    ) -> tuple[StopCandidate | None, str | None, dict[str, object]]:
        signal_candidate = self._build_stop_candidate(
            label="signal_low",
            reference_low=self._as_decimal(history[-1].low),
            entry_price=entry_price,
            atr=atr,
            buffer_atr=self._as_decimal(config.stop_atr_buffer),
        )
        oversold_candidate = (
            self._build_stop_candidate(
                label="oversold_low",
                reference_low=oversold_event.low,
                entry_price=entry_price,
                atr=atr,
                buffer_atr=self._as_decimal(config.stop_atr_buffer),
            )
            if oversold_event is not None
            else None
        )
        lookback_candidate = self._build_stop_candidate(
            label="lookback_low",
            reference_low=min(self._as_decimal(candle.low) for candle in history[-config.stop_lookback_bars :]),
            entry_price=entry_price,
            atr=atr,
            buffer_atr=self._as_decimal(config.stop_atr_buffer),
        )

        diagnostics = {
            "stop_mode": config.stop_mode,
            "signal_low_stop_price": str(signal_candidate.price) if signal_candidate is not None else None,
            "oversold_low_stop_price": str(oversold_candidate.price) if oversold_candidate is not None else None,
            "lookback_low_stop_price": str(lookback_candidate.price) if lookback_candidate is not None else None,
            "max_stop_pct": str(config.max_stop_pct),
        }

        if config.stop_mode == "signal_low":
            chosen = signal_candidate
        elif config.stop_mode == "oversold_low":
            chosen = oversold_candidate
        else:
            chosen = lookback_candidate

        if chosen is None:
            return None, "safety_guard_failed", diagnostics
        if chosen.distance_pct > self._as_decimal(config.max_stop_pct):
            return None, "max_stop_exceeded", {**diagnostics, "selected_stop_mode": chosen.stop_mode}
        return chosen, None, {**diagnostics, "selected_stop_mode": chosen.stop_mode}

    def _build_stop_candidate(
        self,
        label: str,
        reference_low: Decimal,
        entry_price: Decimal,
        atr: Decimal,
        buffer_atr: Decimal,
    ) -> StopCandidate | None:
        if reference_low <= ZERO or entry_price <= ZERO:
            return None
        stop_price = reference_low - (atr * buffer_atr)
        if stop_price <= ZERO or stop_price >= entry_price:
            return None
        distance_pct = (entry_price - stop_price) / entry_price
        return StopCandidate(stop_mode=label, price=stop_price, distance_pct=distance_pct)

    def _target_price(
        self,
        entry_price: Decimal,
        atr: Decimal,
        stop_candidate: StopCandidate | None,
        config: RSIMicroBounceConfig,
    ) -> Decimal:
        if entry_price <= ZERO:
            return ZERO
        if config.target_mode == "atr_multiple":
            return entry_price + (atr * self._as_decimal(config.target_atr_multiple))
        if config.target_mode == "fixed_pct":
            return entry_price * (ONE + self._as_decimal(config.target_fixed_pct))
        if stop_candidate is None:
            return ZERO
        return entry_price + ((entry_price - stop_candidate.price) * self._as_decimal(config.target_r_multiple))

    def _bars_held(
        self,
        history: Sequence[BacktestCandle],
        entry_time: object,
        current_bar_index: int,
        entry_bar_index: object,
    ) -> int:
        if isinstance(entry_bar_index, int):
            return max(0, current_bar_index - entry_bar_index)
        if not isinstance(entry_time, datetime):
            return 0
        for index in range(len(history) - 1, -1, -1):
            if history[index].open_time == entry_time:
                return max(0, current_bar_index - index)
        return 0

    def _rsi_series(self, values: Sequence[Decimal], period: int) -> list[Decimal | None]:
        series: list[Decimal | None] = [None] * len(values)
        if len(values) <= period:
            return series

        gains: list[Decimal] = []
        losses: list[Decimal] = []
        for index in range(1, period + 1):
            delta = values[index] - values[index - 1]
            gains.append(max(delta, ZERO))
            losses.append(abs(min(delta, ZERO)))

        avg_gain = self._mean(gains)
        avg_loss = self._mean(losses)
        series[period] = self._rsi_from_average_moves(avg_gain, avg_loss)

        for index in range(period + 1, len(values)):
            delta = values[index] - values[index - 1]
            gain = max(delta, ZERO)
            loss = abs(min(delta, ZERO))
            avg_gain = ((avg_gain * Decimal(period - 1)) + gain) / Decimal(period)
            avg_loss = ((avg_loss * Decimal(period - 1)) + loss) / Decimal(period)
            series[index] = self._rsi_from_average_moves(avg_gain, avg_loss)

        return series

    def _rsi_from_average_moves(self, avg_gain: Decimal, avg_loss: Decimal) -> Decimal:
        if avg_gain <= ZERO and avg_loss <= ZERO:
            return Decimal("50")
        if avg_loss <= ZERO:
            return HUNDRED
        relative_strength = avg_gain / avg_loss
        return HUNDRED - (HUNDRED / (ONE + relative_strength))

    def _atr(self, candles: Sequence[BacktestCandle], period: int) -> Decimal:
        if len(candles) < period + 1:
            return ZERO
        relevant = candles[-(period + 1) :]
        true_ranges: list[Decimal] = []
        for previous_candle, current_candle in zip(relevant[:-1], relevant[1:]):
            previous_close = self._as_decimal(previous_candle.close)
            high = self._as_decimal(current_candle.high)
            low = self._as_decimal(current_candle.low)
            true_ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
        return self._mean(true_ranges[-period:])

    def _mean(self, values: Sequence[Decimal]) -> Decimal:
        if not values:
            return ZERO
        return sum(values, ZERO) / Decimal(len(values))

    def _as_decimal(self, value: object) -> Decimal:
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    def _bounded_ratio(self, numerator: Decimal, denominator: Decimal) -> float:
        if numerator <= ZERO or denominator <= ZERO:
            return 0.0
        ratio = numerator / denominator
        if ratio > ONE:
            ratio = ONE
        return float(ratio)
