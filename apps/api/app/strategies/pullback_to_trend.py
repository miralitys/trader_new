from __future__ import annotations

from pydantic import Field

from app.strategies.base import BaseStrategy, BaseStrategyConfig, StrategyContext, StrategySignal


class PullbackToTrendConfig(BaseStrategyConfig):
    trend_ma_period: int = Field(default=50)
    pullback_ma_period: int = Field(default=21)


class PullbackToTrendStrategy(BaseStrategy):
    key = "pullback_to_trend"
    name = "PullbackToTrend"
    description = "Trend continuation entries on pullbacks to moving averages. LONG-only SPOT scaffold."
    config_model = PullbackToTrendConfig

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        return StrategySignal(
            action="hold",
            reason="pullback_to_trend_placeholder",
            metadata={"symbol": context.symbol, "timeframe": context.timeframe},
        )
