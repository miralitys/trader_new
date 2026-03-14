from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal, Sequence

from pydantic import Field, model_validator

from app.integrations.coinbase.schemas import CoinbaseTimeframe
from app.schemas.backtest import BacktestCandle
from app.strategies.base import BaseStrategy, BaseStrategyConfig, StrategyContext, StrategySignal

ZERO = Decimal("0")
ONE = Decimal("1")
TWO = Decimal("2")
HUNDRED = Decimal("100")


@dataclass(frozen=True)
class BollingerBand:
    middle: Decimal
    upper: Decimal
    lower: Decimal


@dataclass(frozen=True)
class StopCandidate:
    stop_mode: str
    price: Decimal
    distance_pct: Decimal


class MeanReversionHardStopConfig(BaseStrategyConfig):
    research_overrides_enabled: bool = False
    lookback_period: int = Field(default=20, ge=5, le=300)
    bb_stddev_mult: float = Field(default=2.0, gt=0, lt=10)
    hard_stop_pct: float = Field(default=0.018, gt=0, lt=1)
    stop_loss_pct: float = Field(default=0.018, gt=0, lt=1)
    take_profit_pct: float = Field(default=0, ge=0, lt=2)

    bb_reentry_required: bool = True
    oversold_detection_mode: Literal["band", "rsi", "either", "both"] = "rsi"
    oversold_lookback_bars: int = Field(default=5, ge=1, le=50)
    rsi_period: int = Field(default=14, ge=2, le=100)
    rsi_oversold_threshold: float = Field(default=27, gt=0, lt=100)
    rsi_reclaim_threshold: float = Field(default=30, gt=0, lt=100)
    min_band_overshoot_atr: float = Field(default=0.2, ge=0, lt=5)
    atr_period: int = Field(default=14, ge=2, le=100)
    min_bounce_pct: float = Field(default=0.003, ge=0, lt=0.1)
    min_recovery_atr: float = Field(default=0.15, ge=0, lt=5)

    cost_multiplier: float = Field(default=2.5, ge=0, lt=20)
    target_source: Literal["bb_mid", "sma", "ema", "stop_multiple"] = "bb_mid"
    target_r_multiple: float = Field(default=1.0, gt=0, lt=10)
    exit_deviation_pct: float = Field(default=0.001, ge=0, lt=0.1)

    stop_mode: Literal["signal_low", "lookback_low", "hybrid"] = "signal_low"
    stop_lookback_bars: int = Field(default=8, ge=2, le=50)
    stop_atr_buffer: float = Field(default=0.1, ge=0, lt=5)
    max_stop_pct: float = Field(default=0.015, gt=0, lt=1)

    exit_ema_period: int = Field(default=20, ge=2, le=100)
    exit_on_ema20_loss: bool = True
    exit_on_rsi_rollover: bool = True
    exit_rsi_threshold: float = Field(default=45, gt=0, lt=100)
    min_hold_bars: int = Field(default=3, ge=0, le=100)
    max_bars_in_trade: int = Field(default=24, ge=1, le=500)

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
    use_htf_rsi_filter: bool = True
    htf_rsi_period: int = Field(default=14, ge=2, le=100)
    htf_rsi_min: float = Field(default=45, gt=0, lt=100)
    downside_volatility_filter_enabled: bool = True
    downside_volatility_lookback: int = Field(default=6, ge=2, le=100)
    downside_volatility_expansion_ratio: float = Field(default=1.2, ge=1, lt=10)

    @model_validator(mode="before")
    @classmethod
    def migrate_legacy_fields(cls, payload: object) -> object:
        if not isinstance(payload, dict):
            return payload

        normalized = dict(payload)
        field_map = {
            "rsi_extreme_threshold": "rsi_oversold_threshold",
            "rsi_cross_level": "rsi_reclaim_threshold",
            "min_bb_atr_breach": "min_band_overshoot_atr",
            "target_to_cost_ratio": "cost_multiplier",
            "stop_reference": "stop_mode",
            "stop_lookback_period": "stop_lookback_bars",
            "stop_buffer_atr": "stop_atr_buffer",
            "time_stop_bars": "max_bars_in_trade",
            "regime_min_slope_pct": "min_slope",
            "regime_max_atr_pct": "atr_pct_max",
            "use_htf_rsi_filter": "require_htf_rsi",
            "downside_volatility_filter_enabled": "require_downside_volatility_filter",
        }
        for old_key, new_key in field_map.items():
            if old_key in normalized and new_key not in normalized:
                normalized[new_key] = normalized[old_key]

        if normalized.get("stop_mode") == "rolling_low":
            normalized["stop_mode"] = "lookback_low"

        return normalized

    @model_validator(mode="after")
    def validate_thresholds(self) -> "MeanReversionHardStopConfig":
        if not self.research_overrides_enabled:
            self.bb_reentry_required = True
            self.stop_mode = "signal_low"
            self.oversold_detection_mode = "rsi"
            self.max_stop_pct = 0.015
            self.cost_multiplier = 2.5
        self.use_htf_rsi_filter = self.require_htf_rsi
        self.downside_volatility_filter_enabled = self.require_downside_volatility_filter

        if self.rsi_oversold_threshold > self.rsi_reclaim_threshold:
            raise ValueError("rsi_oversold_threshold must be less than or equal to rsi_reclaim_threshold")
        if self.atr_pct_min > self.atr_pct_max:
            raise ValueError("atr_pct_min must be less than or equal to atr_pct_max")

        fallback_stop = min(self.hard_stop_pct, self.stop_loss_pct, self.max_stop_pct)
        self.hard_stop_pct = fallback_stop
        self.stop_loss_pct = fallback_stop
        return self


class MeanReversionHardStopStrategy(BaseStrategy):
    key = "mean_reversion_hard_stop"
    name = "MeanReversionHardStop"
    description = "Two-stage mean reversion with oversold detection, reclaim confirmation, cost-aware targets, and capped structure stops."
    status = "implemented"
    config_model = MeanReversionHardStopConfig

    def required_history_bars(
        self,
        timeframe: str,
        config: MeanReversionHardStopConfig | None = None,
    ) -> int:
        active_config = config or self.parse_config()
        minimum_history = max(
            active_config.lookback_period + active_config.oversold_lookback_bars + 1,
            active_config.rsi_period + active_config.oversold_lookback_bars + 1,
            active_config.atr_period + active_config.oversold_lookback_bars + 1,
            active_config.exit_ema_period,
            active_config.stop_lookback_bars,
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
        minimum_history = self.required_history_bars(context.timeframe, config)
        bars_seen = int(context.metadata.get("bars_seen", len(history)))
        if bars_seen < minimum_history:
            return StrategySignal(action="hold", reason="insufficient_history")

        closes = self._closes_from_context(context, history)
        previous_close = closes[-2]
        current_close = closes[-1]
        if previous_close <= ZERO or current_close <= ZERO:
            return StrategySignal(action="hold", reason="invalid_prices")

        current_bb = self._bollinger_bands(
            closes[-config.lookback_period :],
            self._as_decimal(config.bb_stddev_mult),
        )
        if current_bb.middle <= ZERO:
            return StrategySignal(action="hold", reason="invalid_moving_average")

        rsi_series = self._rsi_series_from_context(context, closes, config.rsi_period)
        previous_rsi = rsi_series[-2]
        current_rsi = rsi_series[-1]
        if previous_rsi is None or current_rsi is None:
            return StrategySignal(action="hold", reason="insufficient_rsi_history")

        current_atr = self._atr(history, config.atr_period)
        if current_atr <= ZERO:
            return StrategySignal(action="hold", reason="invalid_atr")

        fee_rate = self._as_decimal(context.metadata.get("fee_rate", ZERO))
        slippage_rate = self._as_decimal(context.metadata.get("slippage_rate", ZERO))
        estimated_cost_pct = fee_rate + fee_rate + slippage_rate + slippage_rate
        has_position = bool(context.metadata.get("has_position"))
        position = context.metadata.get("position") or {}

        exit_ema_value = self._exit_ema_from_context(context, closes, config.exit_ema_period)
        target_price = self._target_price(
            closes=closes,
            current_band=current_bb,
            exit_ema_value=exit_ema_value,
            config=config,
        )
        if target_price <= ZERO:
            return StrategySignal(action="hold", reason="invalid_target")

        if not has_position:
            return self._entry_signal(
                context=context,
                history=history,
                closes=closes,
                rsi_series=rsi_series,
                current_bb=current_bb,
                current_atr=current_atr,
                previous_rsi=previous_rsi,
                current_rsi=current_rsi,
                previous_close=previous_close,
                current_close=current_close,
                estimated_cost_pct=estimated_cost_pct,
                target_price=target_price,
                config=config,
            )

        return self._exit_signal(
            history=history,
            closes=closes,
            current_bb=current_bb,
            previous_rsi=previous_rsi,
            current_rsi=current_rsi,
            previous_close=previous_close,
            current_close=current_close,
            target_price=target_price,
            exit_ema_value=exit_ema_value,
            position=position,
            current_bar_index=int(context.metadata.get("bar_index", len(history) - 1)),
            config=config,
        )

    def _entry_signal(
        self,
        context: StrategyContext,
        history: Sequence[BacktestCandle],
        closes: Sequence[Decimal],
        rsi_series: Sequence[Decimal | None],
        current_bb: BollingerBand,
        current_atr: Decimal,
        previous_rsi: Decimal,
        current_rsi: Decimal,
        previous_close: Decimal,
        current_close: Decimal,
        estimated_cost_pct: Decimal,
        target_price: Decimal,
        config: MeanReversionHardStopConfig,
    ) -> StrategySignal:
        band_oversold, band_overshoot_atr = self._band_oversold_detected(history, closes, config)
        rsi_min_last_n = self._recent_rsi_min(rsi_series, config.oversold_lookback_bars)
        rsi_oversold = rsi_min_last_n is not None and rsi_min_last_n <= self._as_decimal(config.rsi_oversold_threshold)
        setup_type = self._setup_type(band_oversold, rsi_oversold)
        oversold_detected = self._oversold_detected(
            band_oversold=band_oversold,
            rsi_oversold=rsi_oversold,
            mode=config.oversold_detection_mode,
        )

        bb_reclaimed = (not config.bb_reentry_required) or current_close > current_bb.lower
        rsi_reclaimed = previous_rsi < self._as_decimal(config.rsi_reclaim_threshold) and current_rsi >= self._as_decimal(
            config.rsi_reclaim_threshold
        )
        bounce_pct = (current_close - previous_close) / previous_close
        recovery_amount = current_close - previous_close
        strong_recovery = (
            current_close > previous_close
            and bounce_pct >= self._as_decimal(config.min_bounce_pct)
            and recovery_amount >= current_atr * self._as_decimal(config.min_recovery_atr)
        )

        regime_ok, regime_reason, regime_metadata = self._passes_regime_filter(
            context=context,
            history=history,
            config=config,
        )
        stop_candidate, stop_reason, stop_metadata = self._select_stop_candidate(
            history=history,
            entry_price=current_close,
            atr=current_atr,
            config=config,
        )
        resolved_target_price = target_price
        if config.target_source == "stop_multiple" and stop_candidate is not None:
            resolved_target_price = current_close + (
                (current_close - stop_candidate.price) * self._as_decimal(config.target_r_multiple)
            )
        planned_tp_pct = (resolved_target_price - current_close) / current_close

        entry_metadata = {
            "stage": "entry_check",
            "oversold_detected": oversold_detected,
            "reclaim_confirmed": bb_reclaimed and rsi_reclaimed,
            "setup_type": setup_type,
            "oversold_detection_mode": config.oversold_detection_mode,
            "bb_reentry_required": config.bb_reentry_required,
            "bb_reclaimed": bb_reclaimed,
            "rsi_reclaimed": rsi_reclaimed,
            "band_oversold": band_oversold,
            "rsi_oversold": rsi_oversold,
            "rsi_min_last_n": str(rsi_min_last_n) if rsi_min_last_n is not None else None,
            "band_overshoot_atr": str(band_overshoot_atr),
            "previous_rsi": str(previous_rsi),
            "current_rsi": str(current_rsi),
            "current_close": str(current_close),
            "bb_lower": str(current_bb.lower),
            "bb_middle": str(current_bb.middle),
            "bounce_pct": str(bounce_pct),
            "planned_tp_pct": str(planned_tp_pct),
            "estimated_cost_pct": str(estimated_cost_pct),
            "cost_multiplier": str(config.cost_multiplier),
            "target_price": str(resolved_target_price),
            "target_source": config.target_source,
            "target_r_multiple": str(config.target_r_multiple),
            "min_recovery_atr": str(config.min_recovery_atr),
            "min_bounce_pct": str(config.min_bounce_pct),
            **regime_metadata,
            **stop_metadata,
        }

        if not regime_ok:
            return self._hold_signal(
                reason="regime_blocked",
                detail=regime_reason,
                metadata=entry_metadata,
            )
        if not oversold_detected:
            return self._hold_signal(
                reason="safety_guard_failed",
                detail="oversold_not_detected",
                metadata=entry_metadata,
            )
        if not bb_reclaimed:
            return self._hold_signal(
                reason="safety_guard_failed",
                detail="bb_reentry_not_confirmed",
                metadata=entry_metadata,
            )
        if not rsi_reclaimed:
            return self._hold_signal(
                reason="safety_guard_failed",
                detail="rsi_reclaim_not_confirmed",
                metadata=entry_metadata,
            )
        if not strong_recovery:
            return self._hold_signal(
                reason="safety_guard_failed",
                detail="recovery_not_strong_enough",
                metadata=entry_metadata,
            )
        minimum_planned_tp_pct = estimated_cost_pct * self._as_decimal(config.cost_multiplier)
        if planned_tp_pct <= ZERO or planned_tp_pct < minimum_planned_tp_pct:
            return self._hold_signal(
                reason="insufficient_tp_vs_cost",
                detail="planned_tp_too_small",
                metadata={
                    **entry_metadata,
                    "minimum_planned_tp_pct": str(minimum_planned_tp_pct),
                },
            )
        if stop_candidate is None:
            return self._hold_signal(
                reason=stop_reason or "safety_guard_failed",
                detail="stop_not_usable",
                metadata=entry_metadata,
            )

        confidence = (
            self._bounded_ratio(planned_tp_pct, minimum_planned_tp_pct if minimum_planned_tp_pct > ZERO else ONE)
            + self._bounded_ratio(recovery_amount, current_atr)
            + self._bounded_ratio(
                self._as_decimal(config.rsi_reclaim_threshold) - self._as_decimal(config.rsi_oversold_threshold),
                self._as_decimal(config.rsi_reclaim_threshold),
            )
        ) / 3

        return StrategySignal(
            action="enter",
            side="long",
            reason="oversold_reclaim_entry",
            confidence=confidence,
            metadata={
                **entry_metadata,
                "stage": "entry_confirmed",
                "previous_close": str(previous_close),
                "atr": str(current_atr),
                "stop_price": str(stop_candidate.price),
                "take_profit_price": str(resolved_target_price),
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
        current_bb: BollingerBand,
        previous_rsi: Decimal,
        current_rsi: Decimal,
        previous_close: Decimal,
        current_close: Decimal,
        target_price: Decimal,
        exit_ema_value: Decimal,
        position: dict[str, object],
        current_bar_index: int,
        config: MeanReversionHardStopConfig,
    ) -> StrategySignal:
        entry_price = self._as_decimal(position.get("entry_price", current_close))
        position_take_profit_price = position.get("take_profit_price")
        effective_target_price = (
            self._as_decimal(position_take_profit_price) if position_take_profit_price is not None else target_price
        )
        entry_time = position.get("entry_time")
        entry_bar_index = position.get("entry_bar_index")
        bars_held = self._bars_held(
            history=history,
            entry_time=entry_time,
            current_bar_index=current_bar_index,
            entry_bar_index=entry_bar_index,
        )
        current_profit_pct = ZERO
        if entry_price > ZERO:
            current_profit_pct = (current_close - entry_price) / entry_price

        if current_close >= effective_target_price and current_close >= entry_price:
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

        exit_ema = exit_ema_value
        if config.exit_on_ema20_loss and bars_held >= config.min_hold_bars and current_close < exit_ema:
            return StrategySignal(
                action="exit",
                side="long",
                reason="ema_failure",
                confidence=self._bounded_ratio(exit_ema - current_close, exit_ema if exit_ema > ZERO else ONE),
                metadata={
                    "entry_price": str(entry_price),
                    "current_close": str(current_close),
                    "exit_ema": str(exit_ema),
                    "previous_close": str(previous_close),
                    "bars_held": bars_held,
                    "exit_reason_label": "ema_failure",
                },
            )

        if (
            config.exit_on_rsi_rollover
            and bars_held >= config.min_hold_bars
            and previous_rsi >= self._as_decimal(config.exit_rsi_threshold)
            and current_rsi < self._as_decimal(config.exit_rsi_threshold)
            and current_profit_pct > ZERO
        ):
            return StrategySignal(
                action="exit",
                side="long",
                reason="rebound_failure",
                confidence=self._bounded_ratio(
                    self._as_decimal(config.exit_rsi_threshold) - current_rsi,
                    self._as_decimal(config.exit_rsi_threshold),
                ),
                metadata={
                    "entry_price": str(entry_price),
                    "current_close": str(current_close),
                    "previous_rsi": str(previous_rsi),
                    "current_rsi": str(current_rsi),
                    "bars_held": bars_held,
                    "exit_reason_label": "rebound_failure",
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

    def _rsi_series_from_context(
        self,
        context: StrategyContext,
        closes: Sequence[Decimal],
        period: int,
    ) -> Sequence[Decimal | None]:
        payload = context.metadata.get("rsi_series_tail")
        if isinstance(payload, Sequence) and len(payload) >= 2:
            return payload
        return self._rsi_series(closes, period)

    def _exit_ema_from_context(
        self,
        context: StrategyContext,
        closes: Sequence[Decimal],
        period: int,
    ) -> Decimal:
        payload = context.metadata.get("exit_ema_value")
        if payload is not None:
            return self._as_decimal(payload)
        return self._ema(closes, period)

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

    def _oversold_detected(
        self,
        band_oversold: bool,
        rsi_oversold: bool,
        mode: str,
    ) -> bool:
        if mode == "band":
            return band_oversold
        if mode == "rsi":
            return rsi_oversold
        if mode == "both":
            return band_oversold and rsi_oversold
        return band_oversold or rsi_oversold

    def _setup_type(self, band_oversold: bool, rsi_oversold: bool) -> str:
        if band_oversold and rsi_oversold:
            return "bb_and_rsi"
        if band_oversold:
            return "bb_only"
        if rsi_oversold:
            return "rsi_only"
        return "none"

    def _target_price(
        self,
        closes: Sequence[Decimal],
        current_band: BollingerBand,
        exit_ema_value: Decimal,
        config: MeanReversionHardStopConfig,
    ) -> Decimal:
        if config.target_source == "stop_multiple":
            # Research mode resolves the exact target from the selected stop candidate in _entry_signal.
            return current_band.middle
        if config.target_source == "ema":
            base_target = exit_ema_value
        elif config.target_source == "sma":
            base_target = self._mean(closes[-config.lookback_period :])
        else:
            base_target = current_band.middle

        if base_target <= ZERO:
            return ZERO
        return base_target * (ONE - self._as_decimal(config.exit_deviation_pct))

    def _select_stop_candidate(
        self,
        history: Sequence[BacktestCandle],
        entry_price: Decimal,
        atr: Decimal,
        config: MeanReversionHardStopConfig,
    ) -> tuple[StopCandidate | None, str | None, dict[str, object]]:
        signal_candidate = self._build_stop_candidate(
            label="signal_low",
            reference_low=self._as_decimal(history[-1].low),
            entry_price=entry_price,
            atr=atr,
            buffer_atr=self._as_decimal(config.stop_atr_buffer),
        )
        lookback_candidate = self._build_stop_candidate(
            label="lookback_low",
            reference_low=min(self._as_decimal(candle.low) for candle in history[-config.stop_lookback_bars :]),
            entry_price=entry_price,
            atr=atr,
            buffer_atr=self._as_decimal(config.stop_atr_buffer),
        )

        diagnostics: dict[str, object] = {
            "stop_mode": config.stop_mode,
            "signal_low_stop_price": str(signal_candidate.price) if signal_candidate is not None else None,
            "signal_low_stop_pct": str(signal_candidate.distance_pct) if signal_candidate is not None else None,
            "lookback_low_stop_price": str(lookback_candidate.price) if lookback_candidate is not None else None,
            "lookback_low_stop_pct": str(lookback_candidate.distance_pct) if lookback_candidate is not None else None,
            "max_stop_pct": str(config.max_stop_pct),
        }

        chosen: StopCandidate | None = None
        if config.stop_mode == "signal_low":
            chosen = signal_candidate
        elif config.stop_mode == "lookback_low":
            chosen = lookback_candidate
        else:
            valid_candidates = [candidate for candidate in (signal_candidate, lookback_candidate) if candidate is not None]
            if valid_candidates:
                chosen = max(valid_candidates, key=lambda candidate: candidate.price)

        if chosen is None:
            return None, "safety_guard_failed", diagnostics

        if chosen.distance_pct > self._as_decimal(config.max_stop_pct):
            return None, "max_stop_exceeded", {**diagnostics, "selected_stop_mode": chosen.stop_mode}

        return chosen, None, {
            **diagnostics,
            "selected_stop_mode": chosen.stop_mode,
        }

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
        return StopCandidate(
            stop_mode=label,
            price=stop_price,
            distance_pct=distance_pct,
        )

    def _band_oversold_detected(
        self,
        history: Sequence[BacktestCandle],
        closes: Sequence[Decimal],
        config: MeanReversionHardStopConfig,
    ) -> tuple[bool, Decimal]:
        best_overshoot_atr = ZERO
        start_index = max(config.lookback_period - 1, len(history) - 1 - config.oversold_lookback_bars)
        end_index = len(history) - 1

        for index in range(start_index, end_index):
            band = self._bollinger_bands(
                closes[index - config.lookback_period + 1 : index + 1],
                self._as_decimal(config.bb_stddev_mult),
            )
            atr = self._atr(history[: index + 1], config.atr_period)
            if atr <= ZERO:
                continue

            close_value = closes[index]
            if close_value >= band.lower:
                continue

            overshoot_atr = (band.lower - close_value) / atr
            if overshoot_atr > best_overshoot_atr:
                best_overshoot_atr = overshoot_atr

        return best_overshoot_atr >= self._as_decimal(config.min_band_overshoot_atr), best_overshoot_atr

    def _recent_rsi_min(
        self,
        rsi_series: Sequence[Decimal | None],
        lookback_bars: int,
    ) -> Decimal | None:
        recent_values = [value for value in rsi_series[-1 - lookback_bars : -1] if value is not None]
        if not recent_values:
            return None
        return min(recent_values)

    def _passes_regime_filter(
        self,
        context: StrategyContext,
        history: Sequence[BacktestCandle],
        config: MeanReversionHardStopConfig,
    ) -> tuple[bool, str, dict[str, object]]:
        if not config.regime_filter_enabled:
            return True, "regime_filter_disabled", {}

        snapshot = self._regime_snapshot_from_context(context)
        if snapshot is not None:
            return self._passes_regime_filter_from_snapshot(snapshot, config)

        one_hour_history = self._one_hour_history_from_context(context)
        if one_hour_history is None:
            one_hour_history = self._resample_to_one_hour(history, context.timeframe)
        minimum_one_hour_history = max(
            config.regime_ema_period + 1
            if config.require_close_above_ema200_1h or config.require_positive_slope_1h
            else 0,
            config.regime_atr_period + 1 if config.require_atr_band_1h else 0,
            config.htf_rsi_period + 1 if config.require_htf_rsi else 0,
            ((config.downside_volatility_lookback * 2) + 1)
            if config.require_downside_volatility_filter
            else 0,
        )
        if minimum_one_hour_history <= 0:
            return True, "regime_passed", {"one_hour_bars": len(one_hour_history)}
        if len(one_hour_history) < minimum_one_hour_history:
            return False, "regime_history_insufficient", {"one_hour_bars": len(one_hour_history)}

        closes = self._one_hour_closes_from_context(context, one_hour_history)
        ema_series = self._ema_series(closes, config.regime_ema_period)
        current_ema = ema_series[-1]
        previous_ema = ema_series[-2]
        if current_ema <= ZERO or previous_ema <= ZERO:
            return False, "regime_filter_invalid_ema", {}

        current_close = closes[-1]
        slope_pct = (current_ema - previous_ema) / previous_ema
        regime_atr = self._atr(one_hour_history, config.regime_atr_period)
        atr_pct = regime_atr / current_close if current_close > ZERO else ZERO
        htf_rsi = None
        if config.require_htf_rsi:
            htf_rsi_series = self._rsi_series(closes, config.htf_rsi_period)
            htf_rsi = htf_rsi_series[-1]

        metadata: dict[str, object] = {
            "regime_close_1h": str(current_close),
            "regime_ema_1h": str(current_ema),
            "regime_slope_1h": str(slope_pct),
            "regime_atr_pct_1h": str(atr_pct),
            "regime_rsi_1h": str(htf_rsi) if htf_rsi is not None else None,
            "one_hour_bars": len(one_hour_history),
        }
        if config.require_close_above_ema200_1h and current_close <= current_ema:
            return False, "close_below_ema200_1h", metadata
        if config.require_positive_slope_1h and slope_pct <= self._as_decimal(config.min_slope):
            return False, "ema200_slope_below_threshold", metadata
        if config.require_atr_band_1h and atr_pct < self._as_decimal(config.atr_pct_min):
            return False, "atr_pct_below_min", metadata
        if config.require_atr_band_1h and atr_pct > self._as_decimal(config.atr_pct_max):
            return False, "atr_pct_above_max", metadata
        if config.require_htf_rsi and (htf_rsi is None or htf_rsi < self._as_decimal(config.htf_rsi_min)):
            return False, "htf_rsi_below_min", metadata
        if config.require_downside_volatility_filter and self._is_expanding_downside_volatility(
            closes=closes,
            lookback=config.downside_volatility_lookback,
            expansion_ratio=self._as_decimal(config.downside_volatility_expansion_ratio),
        ):
            return False, "expanding_downside_volatility", metadata
        return True, "regime_passed", metadata

    def _regime_snapshot_from_context(self, context: StrategyContext) -> dict[str, object] | None:
        payload = context.metadata.get("regime_snapshot")
        if isinstance(payload, dict):
            return payload
        return None

    def _passes_regime_filter_from_snapshot(
        self,
        snapshot: dict[str, object],
        config: MeanReversionHardStopConfig,
    ) -> tuple[bool, str, dict[str, object]]:
        one_hour_bars = int(snapshot.get("one_hour_bars", 0) or 0)
        minimum_one_hour_history = max(
            config.regime_ema_period + 1
            if config.require_close_above_ema200_1h or config.require_positive_slope_1h
            else 0,
            config.regime_atr_period + 1 if config.require_atr_band_1h else 0,
            config.htf_rsi_period + 1 if config.require_htf_rsi else 0,
            ((config.downside_volatility_lookback * 2) + 1)
            if config.require_downside_volatility_filter
            else 0,
        )
        if minimum_one_hour_history <= 0:
            return True, "regime_passed", {"one_hour_bars": one_hour_bars}
        if one_hour_bars < minimum_one_hour_history:
            return False, "regime_history_insufficient", {"one_hour_bars": one_hour_bars}

        current_close = self._as_decimal(snapshot.get("regime_close_1h", ZERO))
        current_ema = self._as_decimal(snapshot.get("regime_ema_1h", ZERO))
        previous_ema_payload = snapshot.get("regime_previous_ema_1h")
        previous_ema = self._as_decimal(previous_ema_payload) if previous_ema_payload is not None else ZERO
        atr_pct_payload = snapshot.get("regime_atr_pct_1h")
        atr_pct = self._as_decimal(atr_pct_payload) if atr_pct_payload is not None else ZERO
        htf_rsi_payload = snapshot.get("regime_rsi_1h")
        htf_rsi = self._as_decimal(htf_rsi_payload) if htf_rsi_payload is not None else None
        slope_pct = (current_ema - previous_ema) / previous_ema if previous_ema > ZERO else ZERO
        closes_tail = [self._as_decimal(value) for value in snapshot.get("regime_closes_tail", [])]

        metadata: dict[str, object] = {
            "regime_close_1h": str(current_close),
            "regime_ema_1h": str(current_ema),
            "regime_slope_1h": str(slope_pct),
            "regime_atr_pct_1h": str(atr_pct),
            "regime_rsi_1h": str(htf_rsi) if htf_rsi is not None else None,
            "one_hour_bars": one_hour_bars,
        }
        if current_ema <= ZERO or previous_ema <= ZERO:
            return False, "regime_filter_invalid_ema", metadata
        if config.require_close_above_ema200_1h and current_close <= current_ema:
            return False, "close_below_ema200_1h", metadata
        if config.require_positive_slope_1h and slope_pct <= self._as_decimal(config.min_slope):
            return False, "ema200_slope_below_threshold", metadata
        if config.require_atr_band_1h and atr_pct < self._as_decimal(config.atr_pct_min):
            return False, "atr_pct_below_min", metadata
        if config.require_atr_band_1h and atr_pct > self._as_decimal(config.atr_pct_max):
            return False, "atr_pct_above_max", metadata
        if config.require_htf_rsi and (htf_rsi is None or htf_rsi < self._as_decimal(config.htf_rsi_min)):
            return False, "htf_rsi_below_min", metadata
        if config.require_downside_volatility_filter and self._is_expanding_downside_volatility(
            closes=closes_tail,
            lookback=config.downside_volatility_lookback,
            expansion_ratio=self._as_decimal(config.downside_volatility_expansion_ratio),
        ):
            return False, "expanding_downside_volatility", metadata
        return True, "regime_passed", metadata

    def _is_expanding_downside_volatility(
        self,
        closes: Sequence[Decimal],
        lookback: int,
        expansion_ratio: Decimal,
    ) -> bool:
        if len(closes) < (lookback * 2) + 1:
            return False

        returns = []
        for previous_close, current_close in zip(closes[:-1], closes[1:]):
            if previous_close <= ZERO:
                return False
            returns.append((current_close - previous_close) / previous_close)

        short_window = returns[-lookback:]
        long_window = returns[-(lookback * 2) : -lookback]
        short_downside = self._mean([abs(value) if value < ZERO else ZERO for value in short_window])
        long_downside = self._mean([abs(value) if value < ZERO else ZERO for value in long_window])
        recent_return = (closes[-1] - closes[-1 - lookback]) / closes[-1 - lookback]

        if short_downside <= ZERO or recent_return >= ZERO:
            return False
        if long_downside <= ZERO:
            return True
        return short_downside >= long_downside * expansion_ratio

    def _resample_to_one_hour(
        self,
        history: Sequence[BacktestCandle],
        timeframe: str,
    ) -> list[BacktestCandle]:
        if timeframe == "1h":
            return list(history)

        aggregated: list[BacktestCandle] = []
        current_bucket: BacktestCandle | None = None
        current_bucket_time: datetime | None = None

        for candle in history:
            bucket_time = candle.open_time.replace(minute=0, second=0, microsecond=0)
            if current_bucket is None or current_bucket_time != bucket_time:
                if current_bucket is not None:
                    aggregated.append(current_bucket)
                current_bucket = BacktestCandle(
                    open_time=bucket_time,
                    open=self._as_decimal(candle.open),
                    high=self._as_decimal(candle.high),
                    low=self._as_decimal(candle.low),
                    close=self._as_decimal(candle.close),
                    volume=self._as_decimal(candle.volume),
                )
                current_bucket_time = bucket_time
                continue

            current_bucket = BacktestCandle(
                open_time=current_bucket.open_time,
                open=current_bucket.open,
                high=max(current_bucket.high, self._as_decimal(candle.high)),
                low=min(current_bucket.low, self._as_decimal(candle.low)),
                close=self._as_decimal(candle.close),
                volume=current_bucket.volume + self._as_decimal(candle.volume),
            )

        if current_bucket is not None:
            aggregated.append(current_bucket)
        return aggregated

    def _one_hour_history_from_context(self, context: StrategyContext) -> Sequence[BacktestCandle] | None:
        payload = context.metadata.get("one_hour_history")
        if isinstance(payload, Sequence):
            return payload
        return None

    def _one_hour_closes_from_context(
        self,
        context: StrategyContext,
        one_hour_history: Sequence[BacktestCandle],
    ) -> Sequence[Decimal]:
        payload = context.metadata.get("one_hour_closes")
        if isinstance(payload, Sequence) and len(payload) == len(one_hour_history):
            return payload
        return [self._as_decimal(candle.close) for candle in one_hour_history]

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
        return sum(1 for candle in history if candle.open_time > entry_time)

    def _bollinger_bands(self, values: Sequence[Decimal], stddev_mult: Decimal) -> BollingerBand:
        middle = self._mean(values)
        stddev = self._stddev(values)
        offset = stddev * stddev_mult
        return BollingerBand(
            middle=middle,
            upper=middle + offset,
            lower=middle - offset,
        )

    def _mean(self, values: Sequence[Decimal]) -> Decimal:
        if not values:
            return ZERO
        return sum(values, ZERO) / Decimal(len(values))

    def _stddev(self, values: Sequence[Decimal]) -> Decimal:
        if len(values) < 2:
            return ZERO
        mean = self._mean(values)
        variance = sum(((value - mean) ** 2 for value in values), ZERO) / Decimal(len(values))
        return variance.sqrt()

    def _ema(self, values: Sequence[Decimal], period: int) -> Decimal:
        series = self._ema_series(values, period)
        if not series:
            return ZERO
        return series[-1]

    def _ema_series(self, values: Sequence[Decimal], period: int) -> list[Decimal]:
        if not values:
            return []

        alpha = TWO / (Decimal(period) + ONE)
        ema = values[0]
        series = [ema]
        for value in values[1:]:
            ema = ema + (value - ema) * alpha
            series.append(ema)
        return series

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
        if avg_gain <= ZERO and avg_loss <= ZERO:
            series[period] = Decimal("50")
        elif avg_loss <= ZERO:
            series[period] = HUNDRED
        else:
            relative_strength = avg_gain / avg_loss
            series[period] = HUNDRED - (HUNDRED / (ONE + relative_strength))

        for index in range(period + 1, len(values)):
            delta = values[index] - values[index - 1]
            gain = max(delta, ZERO)
            loss = abs(min(delta, ZERO))
            avg_gain = ((avg_gain * Decimal(period - 1)) + gain) / Decimal(period)
            avg_loss = ((avg_loss * Decimal(period - 1)) + loss) / Decimal(period)
            if avg_gain <= ZERO and avg_loss <= ZERO:
                series[index] = Decimal("50")
            elif avg_loss <= ZERO:
                series[index] = HUNDRED
            else:
                relative_strength = avg_gain / avg_loss
                series[index] = HUNDRED - (HUNDRED / (ONE + relative_strength))

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

        relevant_candles = candles[-(period + 1) :]
        true_ranges: list[Decimal] = []
        for previous_candle, current_candle in zip(relevant_candles[:-1], relevant_candles[1:]):
            previous_close = self._as_decimal(previous_candle.close)
            high = self._as_decimal(current_candle.high)
            low = self._as_decimal(current_candle.low)
            true_ranges.append(
                max(
                    high - low,
                    abs(high - previous_close),
                    abs(low - previous_close),
                )
            )
        return self._mean(true_ranges[-period:])

    def _as_decimal(self, value: object) -> Decimal:
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    def _bounded_ratio(self, numerator: Decimal, denominator: Decimal) -> float:
        if denominator <= ZERO or numerator <= ZERO:
            return 0.0
        ratio = numerator / denominator
        if ratio > ONE:
            ratio = ONE
        return float(ratio)
