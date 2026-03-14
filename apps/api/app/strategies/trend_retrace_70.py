from __future__ import annotations

from pydantic import Field

from app.strategies.base import BaseStrategy, BaseStrategyConfig, StrategyContext, StrategySignal


class TrendRetrace70Config(BaseStrategyConfig):
    swing_lookback: int = Field(default=40)
    retrace_ratio: float = Field(default=0.70)


class TrendRetrace70Strategy(BaseStrategy):
    key = "trend_retrace_70"
    name = "TrendRetrace70"
    description = "Deep retracement setup around 70 percent of the previous impulse. LONG-only SPOT scaffold."
    config_model = TrendRetrace70Config

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        return StrategySignal(
            action="hold",
            reason="trend_retrace_70_placeholder",
            metadata={"symbol": context.symbol, "timeframe": context.timeframe},
        )
