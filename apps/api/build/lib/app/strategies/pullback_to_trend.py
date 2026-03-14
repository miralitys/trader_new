from __future__ import annotations

from statistics import mean

from app.domain.enums import SignalAction
from app.strategies.base import BaseStrategy
from app.strategies.types import BaseStrategyConfig, CandleInput, RiskDecision, SignalDecision, StrategyContext


class PullbackToTrendConfig(BaseStrategyConfig):
    fast_ma: int = 20
    slow_ma: int = 50
    pullback_ma: int = 21
    min_trend_gap_pct: float = 0.003


class PullbackToTrendStrategy(BaseStrategy):
    key = "pullback_to_trend"
    name = "PullbackToTrend"
    description = "Trend-following pullback entries into moving averages. Long-only spot scaffold."
    config_model = PullbackToTrendConfig

    def generate_signal(self, candles: list[CandleInput], context: StrategyContext) -> SignalDecision:
        cfg = context.config
        needed = max(cfg.warmup_candles, cfg.slow_ma + 2)
        if len(candles) < needed:
            return SignalDecision(action=SignalAction.HOLD, reason="warming_up")

        closes = [c.close for c in candles]
        fast = mean(closes[-cfg.fast_ma :])
        slow = mean(closes[-cfg.slow_ma :])
        pullback = mean(closes[-cfg.pullback_ma :])
        last = candles[-1]
        trend_gap = (fast - slow) / slow if slow else 0.0

        if (
            not context.has_open_position
            and fast > slow
            and trend_gap >= cfg.min_trend_gap_pct
            and last.low <= pullback
            and last.close >= fast
        ):
            return SignalDecision(
                action=SignalAction.ENTER,
                strength=0.66,
                reason="trend_pullback_entry",
                stop_loss=slow * (1 - cfg.stop_loss_pct),
                take_profit=last.close * (1 + cfg.take_profit_pct),
                metadata={"fast_ma": fast, "slow_ma": slow, "pullback_ma": pullback},
            )

        if context.has_open_position and (last.close < slow or (context.position.stop_loss and last.low <= context.position.stop_loss)):
            return SignalDecision(action=SignalAction.EXIT, strength=0.78, reason="trend_breakdown")

        return SignalDecision(action=SignalAction.HOLD, reason="no_pullback_setup")

    def evaluate_risk(self, signal: SignalDecision, context: StrategyContext) -> RiskDecision:
        decision = super().evaluate_risk(signal, context)
        if signal.action == SignalAction.ENTER and context.cash * context.config.position_size_pct < 50:
            return RiskDecision(approved=False, reason="position_too_small")
        return decision
