from __future__ import annotations

from decimal import Decimal
from typing import Literal, Sequence

from pydantic import Field, model_validator

from app.integrations.coinbase.schemas import CoinbaseTimeframe
from app.schemas.backtest import BacktestCandle
from app.strategies.base import BaseStrategyConfig, StrategyContext, StrategySignal
from app.strategies.mean_reversion_hard_stop import MeanReversionHardStopStrategy, StopCandidate, ONE, ZERO


class DeepFlushMicroReclaimConfig(BaseStrategyConfig):
    timeframes: list[str] = Field(default_factory=lambda: ["5m"])
    position_size_pct: float = Field(default=0.1, gt=0, le=1)
    stop_loss_pct: float = Field(default=0.015, gt=0, lt=1)
    take_profit_pct: float = Field(default=0, ge=0, lt=1)

    flush_lookback_bars: int = Field(default=12, ge=4, le=100)
    context_lookback_bars: int = Field(default=36, ge=4, le=300)
    min_drawdown_from_high_pct: float = Field(default=0.013, gt=0, lt=0.2)
    max_rebound_from_low_pct: float = Field(default=0.0047, gt=0, lt=0.05)
    max_context_return_pct: float = Field(default=0.0, gt=-0.2, lt=0.2)
    require_negative_context_return: bool = True
    flush_low_max_age_bars: int = Field(default=3, ge=0, le=20)
    require_green_entry_candle: bool = True
    min_entry_bar_return_pct: float = Field(default=0.001, ge=0, lt=0.05)
    max_entry_bar_return_pct: float = Field(default=0.005, gt=0, lt=0.1)

    atr_period: int = Field(default=14, ge=2, le=100)
    stop_mode: Literal["signal_low", "lookback_low", "hybrid"] = "signal_low"
    stop_lookback_bars: int = Field(default=6, ge=2, le=50)
    stop_atr_buffer: float = Field(default=0.1, ge=0, lt=5)
    max_stop_pct: float = Field(default=0.015, gt=0, lt=1)

    target_mode: Literal["stop_multiple", "fixed_pct"] = "stop_multiple"
    target_r_multiple: float = Field(default=1.2, gt=0, lt=10)
    target_fixed_pct: float = Field(default=0.005, gt=0, lt=0.1)
    require_cost_edge: bool = True
    cost_multiplier: float = Field(default=2.0, gt=0, lt=10)

    exit_ema_period: int = Field(default=9, ge=2, le=100)
    exit_on_ema_loss: bool = True
    exit_on_stall: bool = True
    min_hold_bars: int = Field(default=2, ge=0, le=100)
    max_bars_in_trade: int = Field(default=12, ge=1, le=200)

    regime_filter_enabled: bool = True
    regime_ema_period: int = Field(default=200, ge=2, le=500)
    require_close_above_ema200_1h: bool = True
    require_positive_slope_1h: bool = True
    require_atr_band_1h: bool = True
    require_htf_rsi: bool = True
    require_downside_volatility_filter: bool = True
    min_slope: float = Field(default=0.0005, gt=-1, lt=1)
    regime_atr_period: int = Field(default=14, ge=2, le=100)
    atr_pct_min: float = Field(default=0, ge=0, lt=1)
    atr_pct_max: float = Field(default=0.02, ge=0, lt=1)
    htf_rsi_period: int = Field(default=14, ge=2, le=100)
    htf_rsi_min: float = Field(default=45, gt=0, lt=100)
    downside_volatility_lookback: int = Field(default=6, ge=2, le=100)
    downside_volatility_expansion_ratio: float = Field(default=1.2, ge=1, lt=10)

    @model_validator(mode="after")
    def normalize_risk(self) -> "DeepFlushMicroReclaimConfig":
        self.stop_loss_pct = min(float(self.stop_loss_pct), float(self.max_stop_pct))
        self.take_profit_pct = 0.0
        return self


class DeepFlushMicroReclaimStrategy(MeanReversionHardStopStrategy):
    key = "deep_flush_micro_reclaim"
    name = "DeepFlushMicroReclaim"
    description = (
        "Research strategy derived from the best mean-reversion winners: deep local flushes "
        "inside a healthy 1H regime, entered only on early reclaim bars near the washout low."
    )
    status = "implemented"
    config_model = DeepFlushMicroReclaimConfig

    def required_history_bars(
        self,
        timeframe: str,
        config: DeepFlushMicroReclaimConfig | None = None,
    ) -> int:
        active_config = config or self.parse_config()
        minimum_history = max(
            active_config.flush_lookback_bars + 1,
            active_config.context_lookback_bars + 1,
            active_config.atr_period + 1,
            active_config.stop_lookback_bars + 1,
            active_config.exit_ema_period + 1,
            3,
        )
        if not active_config.regime_filter_enabled:
            return minimum_history

        one_hour_history = max(
            active_config.regime_ema_period + 1
            if active_config.require_close_above_ema200_1h or active_config.require_positive_slope_1h
            else 0,
            active_config.regime_atr_period + 1 if active_config.require_atr_band_1h else 0,
            active_config.htf_rsi_period + 1 if active_config.require_htf_rsi else 0,
            ((active_config.downside_volatility_lookback * 2) + 1)
            if active_config.require_downside_volatility_filter
            else 0,
        )
        if one_hour_history <= 0:
            return minimum_history

        timeframe_seconds = CoinbaseTimeframe.from_code(timeframe).granularity_seconds
        bars_per_hour = max(1, 3600 // timeframe_seconds)
        return max(minimum_history, (one_hour_history * bars_per_hour) + bars_per_hour)

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        history = self._history_from_context(context)
        config = self._config_from_context(context)
        bars_seen = int(context.metadata.get("bars_seen", len(history)))
        minimum_history = self.required_history_bars(context.timeframe, config)
        if bars_seen < minimum_history:
            return StrategySignal(action="hold", reason="insufficient_history")

        closes = self._closes_from_context(context, history)
        current_candle = history[-1]
        current_close = closes[-1]
        previous_close = closes[-2]
        current_open = self._as_decimal(current_candle.open)
        if current_close <= ZERO or previous_close <= ZERO:
            return StrategySignal(action="hold", reason="invalid_prices")

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
                context=context,
                history=history,
                closes=closes,
                current_open=current_open,
                current_close=current_close,
                previous_close=previous_close,
                current_atr=current_atr,
                estimated_cost_pct=estimated_cost_pct,
                config=config,
            )

        return self._exit_signal(
            history=history,
            closes=closes,
            current_close=current_close,
            previous_close=previous_close,
            position=position,
            current_bar_index=int(context.metadata.get("bar_index", len(history) - 1)),
            config=config,
        )

    def _entry_signal(
        self,
        context: StrategyContext,
        history: Sequence[BacktestCandle],
        closes: Sequence[Decimal],
        current_open: Decimal,
        current_close: Decimal,
        previous_close: Decimal,
        current_atr: Decimal,
        estimated_cost_pct: Decimal,
        config: DeepFlushMicroReclaimConfig,
    ) -> StrategySignal:
        flush_window = history[-config.flush_lookback_bars :]
        flush_high = max(self._as_decimal(candle.high) for candle in flush_window)
        flush_low = min(self._as_decimal(candle.low) for candle in flush_window)
        flush_low_age_bars = self._flush_low_age_bars(flush_window)
        drawdown_from_high_pct = (current_close - flush_high) / flush_high if flush_high > ZERO else ZERO
        rebound_from_low_pct = (current_close - flush_low) / flush_low if flush_low > ZERO else ZERO
        context_reference = closes[-(config.context_lookback_bars + 1)]
        context_return_pct = (current_close - context_reference) / context_reference if context_reference > ZERO else ZERO
        entry_bar_return_pct = (current_close - previous_close) / previous_close

        regime_ok, regime_reason, regime_metadata = self._passes_regime_filter(context=context, history=history, config=config)
        stop_candidate, stop_reason, stop_metadata = self._select_stop_candidate(
            history=history,
            entry_price=current_close,
            atr=current_atr,
            config=config,
        )
        target_price = self._target_price(current_close, stop_candidate, config)
        planned_tp_pct = (target_price - current_close) / current_close if target_price > current_close else ZERO

        entry_metadata: dict[str, object] = {
            "stage": "entry_check",
            "event_study_label": "deep_flush_micro_reclaim_candidate",
            "candidate_entry_active": True,
            "current_open": str(current_open),
            "current_close": str(current_close),
            "previous_close": str(previous_close),
            "flush_high": str(flush_high),
            "flush_low": str(flush_low),
            "drawdown_from_high_pct": str(drawdown_from_high_pct),
            "rebound_from_low_pct": str(rebound_from_low_pct),
            "context_return_pct": str(context_return_pct),
            "entry_bar_return_pct": str(entry_bar_return_pct),
            "flush_low_age_bars": flush_low_age_bars,
            "estimated_cost_pct": str(estimated_cost_pct),
            "cost_multiplier": str(config.cost_multiplier),
            "atr": str(current_atr),
            "planned_tp_pct": str(planned_tp_pct),
            "target_price": str(target_price) if target_price > ZERO else None,
            "target_mode": config.target_mode,
            "target_r_multiple": str(config.target_r_multiple),
            "target_fixed_pct": str(config.target_fixed_pct),
            **regime_metadata,
            **stop_metadata,
        }

        if not regime_ok:
            return self._hold_signal("regime_blocked", regime_reason, entry_metadata)
        if drawdown_from_high_pct > -self._as_decimal(config.min_drawdown_from_high_pct):
            return self._hold_signal("safety_guard_failed", "flush_not_deep_enough", entry_metadata)
        if rebound_from_low_pct > self._as_decimal(config.max_rebound_from_low_pct):
            return self._hold_signal("safety_guard_failed", "late_rebound_entry", entry_metadata)
        if flush_low_age_bars > config.flush_low_max_age_bars:
            return self._hold_signal("safety_guard_failed", "flush_low_too_old", entry_metadata)
        if config.require_negative_context_return and context_return_pct > self._as_decimal(config.max_context_return_pct):
            return self._hold_signal("safety_guard_failed", "context_not_reset", entry_metadata)
        if current_close <= previous_close:
            return self._hold_signal("safety_guard_failed", "no_reclaim_close", entry_metadata)
        if config.require_green_entry_candle and current_close <= current_open:
            return self._hold_signal("safety_guard_failed", "entry_candle_not_green", entry_metadata)
        if entry_bar_return_pct < self._as_decimal(config.min_entry_bar_return_pct):
            return self._hold_signal("safety_guard_failed", "entry_bar_not_strong_enough", entry_metadata)
        if entry_bar_return_pct > self._as_decimal(config.max_entry_bar_return_pct):
            return self._hold_signal("safety_guard_failed", "entry_bar_overextended", entry_metadata)
        if stop_candidate is None:
            return self._hold_signal(stop_reason or "max_stop_exceeded", "stop_not_usable", entry_metadata)

        minimum_planned_tp_pct = estimated_cost_pct * self._as_decimal(config.cost_multiplier)
        if config.require_cost_edge and (planned_tp_pct <= ZERO or planned_tp_pct < minimum_planned_tp_pct):
            return self._hold_signal(
                "insufficient_tp_vs_cost",
                "planned_tp_too_small",
                {
                    **entry_metadata,
                    "minimum_planned_tp_pct": str(minimum_planned_tp_pct),
                },
            )

        depth_score = self._bounded_ratio(
            abs(drawdown_from_high_pct),
            self._as_decimal(config.min_drawdown_from_high_pct) * Decimal("2"),
        )
        timing_score = self._bounded_ratio(
            self._as_decimal(config.max_rebound_from_low_pct) - rebound_from_low_pct,
            self._as_decimal(config.max_rebound_from_low_pct),
        )
        reclaim_score = self._bounded_ratio(
            entry_bar_return_pct,
            self._as_decimal(config.min_entry_bar_return_pct) * Decimal("3"),
        )
        confidence = (depth_score + timing_score + reclaim_score) / 3

        return StrategySignal(
            action="enter",
            side="long",
            reason="deep_flush_micro_reclaim_entry",
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
        closes: Sequence[Decimal],
        current_close: Decimal,
        previous_close: Decimal,
        position: dict[str, object],
        current_bar_index: int,
        config: DeepFlushMicroReclaimConfig,
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
        current_profit_pct = (current_close - entry_price) / entry_price if entry_price > ZERO else ZERO
        exit_ema = self._ema(closes, config.exit_ema_period)
        previous_previous_close = closes[-3] if len(closes) >= 3 else previous_close

        if effective_target_price > entry_price and current_close >= effective_target_price:
            return StrategySignal(
                action="exit",
                side="long",
                reason="take_profit",
                confidence=self._bounded_ratio(current_close - effective_target_price, effective_target_price),
                metadata={
                    "entry_price": str(entry_price),
                    "current_close": str(current_close),
                    "bars_held": bars_held,
                    "target_price": str(effective_target_price),
                    "exit_reason_label": "take_profit",
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
                    "exit_reason_label": "time_stop",
                },
            )

        if (
            config.exit_on_stall
            and bars_held >= config.min_hold_bars
            and current_profit_pct > ZERO
            and current_close < previous_close
        ):
            return StrategySignal(
                action="exit",
                side="long",
                reason="rebound_stalling",
                confidence=self._bounded_ratio(previous_close - current_close, previous_close if previous_close > ZERO else ONE),
                metadata={
                    "entry_price": str(entry_price),
                    "current_close": str(current_close),
                    "previous_close": str(previous_close),
                    "bars_held": bars_held,
                    "exit_reason_label": "rebound_stalling",
                },
            )

        if (
            config.exit_on_ema_loss
            and bars_held >= config.min_hold_bars
            and current_close < exit_ema
            and current_close < previous_close
            and previous_close < previous_previous_close
        ):
            return StrategySignal(
                action="exit",
                side="long",
                reason="reclaim_failure",
                confidence=self._bounded_ratio(exit_ema - current_close, exit_ema if exit_ema > ZERO else ONE),
                metadata={
                    "entry_price": str(entry_price),
                    "current_close": str(current_close),
                    "exit_ema": str(exit_ema),
                    "bars_held": bars_held,
                    "exit_reason_label": "reclaim_failure",
                },
            )

        return StrategySignal(action="hold", reason="position_open_waiting_for_resolution")

    def _target_price(
        self,
        entry_price: Decimal,
        stop_candidate: StopCandidate | None,
        config: DeepFlushMicroReclaimConfig,
    ) -> Decimal:
        if entry_price <= ZERO:
            return ZERO
        if config.target_mode == "stop_multiple" and stop_candidate is not None:
            risk = entry_price - stop_candidate.price
            if risk <= ZERO:
                return ZERO
            return entry_price + (risk * self._as_decimal(config.target_r_multiple))
        return entry_price * (ONE + self._as_decimal(config.target_fixed_pct))

    def _flush_low_age_bars(self, candles: Sequence[BacktestCandle]) -> int:
        indexed_lows = [(index, self._as_decimal(candle.low)) for index, candle in enumerate(candles)]
        if not indexed_lows:
            return 0
        lowest_price = min(price for _index, price in indexed_lows)
        latest_low_index = max(index for index, price in indexed_lows if price == lowest_price)
        return len(candles) - 1 - latest_low_index
