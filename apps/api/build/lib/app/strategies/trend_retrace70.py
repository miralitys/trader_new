from __future__ import annotations

from app.domain.enums import SignalAction
from app.strategies.base import BaseStrategy
from app.strategies.types import BaseStrategyConfig, CandleInput, RiskDecision, SignalDecision, StrategyContext


class TrendRetrace70Config(BaseStrategyConfig):
    swing_lookback: int = 40
    retrace_ratio: float = 0.7
    confirmation_bars: int = 2


class TrendRetrace70Strategy(BaseStrategy):
    key = "trend_retrace70"
    name = "TrendRetrace70"
    description = "Deep 70 percent retracement entry after trend impulse. Long-only spot scaffold."
    config_model = TrendRetrace70Config

    def generate_signal(self, candles: list[CandleInput], context: StrategyContext) -> SignalDecision:
        cfg = context.config
        needed = max(cfg.warmup_candles, cfg.swing_lookback + cfg.confirmation_bars + 2)
        if len(candles) < needed:
            return SignalDecision(action=SignalAction.HOLD, reason="warming_up")

        window = candles[-cfg.swing_lookback :]
        swing_high = max(c.high for c in window)
        swing_low = min(c.low for c in window)
        impulse = swing_high - swing_low
        retrace_level = swing_high - impulse * cfg.retrace_ratio
        last = candles[-1]
        confirmations = candles[-cfg.confirmation_bars :]
        bullish_confirmation = all(c.close >= c.open for c in confirmations)

        if (
            not context.has_open_position
            and impulse > 0
            and last.low <= retrace_level <= last.high
            and bullish_confirmation
        ):
            return SignalDecision(
                action=SignalAction.ENTER,
                strength=0.68,
                reason="retrace70_confirmed",
                stop_loss=swing_low * (1 - cfg.stop_loss_pct),
                take_profit=swing_high,
                metadata={"swing_high": swing_high, "swing_low": swing_low, "retrace_level": retrace_level},
            )

        if context.has_open_position:
            if context.position.stop_loss and last.low <= context.position.stop_loss:
                return SignalDecision(action=SignalAction.EXIT, strength=1.0, reason="retracement_stop")
            if context.position.take_profit and last.high >= context.position.take_profit:
                return SignalDecision(action=SignalAction.EXIT, strength=0.9, reason="retracement_target")

        return SignalDecision(action=SignalAction.HOLD, reason="no_retrace_setup")

    def evaluate_risk(self, signal: SignalDecision, context: StrategyContext) -> RiskDecision:
        decision = super().evaluate_risk(signal, context)
        if signal.action == SignalAction.ENTER and context.cash * context.config.position_size_pct > context.cash:
            return RiskDecision(approved=False, reason="size_exceeds_cash")
        return decision
