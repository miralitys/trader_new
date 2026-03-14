from __future__ import annotations

from app.strategies.base import BaseStrategy
from app.strategies.types import RiskDecision, SignalDecision, StrategyContext


class RiskEngine:
    def evaluate(
        self,
        strategy: BaseStrategy,
        signal: SignalDecision,
        context: StrategyContext,
    ) -> RiskDecision:
        return strategy.evaluate_risk(signal, context)
