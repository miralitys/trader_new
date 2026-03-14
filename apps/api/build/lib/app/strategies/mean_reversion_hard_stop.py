from __future__ import annotations

from statistics import mean

from app.domain.enums import SignalAction
from app.strategies.base import BaseStrategy
from app.strategies.types import BaseStrategyConfig, CandleInput, RiskDecision, SignalDecision, StrategyContext


class MeanReversionHardStopConfig(BaseStrategyConfig):
    lookback: int = 30
    deviation_pct: float = 0.04
    hard_stop_pct: float = 0.025
    recovery_target_pct: float = 0.02


class MeanReversionHardStopStrategy(BaseStrategy):
    key = "mean_reversion_hard_stop"
    name = "MeanReversionHardStop"
    description = "Mean reversion with a single hard stop and no DCA. Long-only spot scaffold."
    config_model = MeanReversionHardStopConfig

    def generate_signal(self, candles: list[CandleInput], context: StrategyContext) -> SignalDecision:
        cfg = context.config
        if len(candles) < max(cfg.warmup_candles, cfg.lookback + 2):
            return SignalDecision(action=SignalAction.HOLD, reason="warming_up")

        closes = [c.close for c in candles]
        baseline = mean(closes[-cfg.lookback :])
        last = candles[-1]
        previous = candles[-2]
        deviation = (baseline - last.close) / baseline if baseline else 0.0

        if not context.has_open_position and deviation >= cfg.deviation_pct and last.close > previous.close:
            return SignalDecision(
                action=SignalAction.ENTER,
                strength=0.7,
                reason="mean_reversion_entry",
                stop_loss=last.close * (1 - cfg.hard_stop_pct),
                take_profit=baseline * (1 + cfg.recovery_target_pct),
                metadata={"baseline": baseline, "deviation": deviation},
            )

        if context.has_open_position:
            if context.position.stop_loss and last.low <= context.position.stop_loss:
                return SignalDecision(action=SignalAction.EXIT, strength=1.0, reason="hard_stop_hit")
            if last.close >= baseline:
                return SignalDecision(action=SignalAction.EXIT, strength=0.82, reason="mean_recovered")

        return SignalDecision(action=SignalAction.HOLD, reason="no_mean_reversion_setup")

    def evaluate_risk(self, signal: SignalDecision, context: StrategyContext) -> RiskDecision:
        decision = super().evaluate_risk(signal, context)
        if signal.action == SignalAction.ENTER and context.config.position_size_pct > 0.35:
            return RiskDecision(approved=False, reason="size_above_policy")
        return decision
