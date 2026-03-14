from __future__ import annotations

from pydantic import Field

from app.strategies.base import BaseStrategy, BaseStrategyConfig, StrategyContext, StrategySignal


class BreakoutRetestConfig(BaseStrategyConfig):
    breakout_lookback: int = Field(default=20)
    retest_tolerance_pct: float = Field(default=0.004)


class BreakoutRetestStrategy(BaseStrategy):
    key = "breakout_retest"
    name = "BreakoutRetest"
    description = "Breakout level followed by retest confirmation. LONG-only SPOT scaffold."
    config_model = BreakoutRetestConfig

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        return StrategySignal(
            action="hold",
            reason="breakout_retest_placeholder",
            metadata={"symbol": context.symbol, "timeframe": context.timeframe},
        )
