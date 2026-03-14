from __future__ import annotations

from statistics import mean

from pydantic import Field

from app.domain.enums import SignalAction
from app.strategies.base import BaseStrategy
from app.strategies.types import BaseStrategyConfig, CandleInput, RiskDecision, SignalDecision, StrategyContext


class BreakoutRetestConfig(BaseStrategyConfig):
    breakout_lookback: int = 20
    retest_tolerance_pct: float = 0.004
    confirmation_bars: int = 2


class BreakoutRetestStrategy(BaseStrategy):
    key = "breakout_retest"
    name = "BreakoutRetest"
    description = "Breakout followed by retest confirmation. Long-only spot scaffold."
    config_model = BreakoutRetestConfig

    def generate_signal(self, candles: list[CandleInput], context: StrategyContext) -> SignalDecision:
        cfg = context.config
        if len(candles) < max(cfg.warmup_candles, cfg.breakout_lookback + cfg.confirmation_bars + 2):
            return SignalDecision(action=SignalAction.HOLD, reason="warming_up")

        last = candles[-1]
        confirmation_slice = candles[-cfg.confirmation_bars :]
        prior_highs = [c.high for c in candles[-cfg.breakout_lookback - cfg.confirmation_bars - 1 : -cfg.confirmation_bars - 1]]
        breakout_level = max(prior_highs)
        retest_band = breakout_level * cfg.retest_tolerance_pct
        confirmation_closes = [c.close for c in confirmation_slice]
        bullish_confirmation = mean(confirmation_closes) > breakout_level
        touched_retest = any(abs(c.low - breakout_level) <= retest_band for c in confirmation_slice)

        if not context.has_open_position and bullish_confirmation and touched_retest and last.close > breakout_level:
            return SignalDecision(
                action=SignalAction.ENTER,
                strength=0.72,
                reason="breakout_retest_confirmed",
                stop_loss=breakout_level * (1 - cfg.stop_loss_pct),
                take_profit=last.close * (1 + cfg.take_profit_pct),
                metadata={"breakout_level": breakout_level},
            )

        if context.has_open_position:
            if context.position.stop_loss and last.low <= context.position.stop_loss:
                return SignalDecision(action=SignalAction.EXIT, strength=1.0, reason="hard_stop_hit")
            if context.position.take_profit and last.high >= context.position.take_profit:
                return SignalDecision(action=SignalAction.EXIT, strength=0.9, reason="take_profit_hit")

        return SignalDecision(action=SignalAction.HOLD, reason="no_breakout_setup")

    def evaluate_risk(self, signal: SignalDecision, context: StrategyContext) -> RiskDecision:
        decision = super().evaluate_risk(signal, context)
        if signal.action == SignalAction.ENTER and context.cash < 100:
            return RiskDecision(approved=False, reason="cash_below_minimum")
        return decision
