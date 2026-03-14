from __future__ import annotations

from pydantic import Field

from app.strategies.base import BaseStrategy, BaseStrategyConfig, StrategyContext, StrategySignal


class MeanReversionHardStopConfig(BaseStrategyConfig):
    lookback_period: int = Field(default=30)
    hard_stop_pct: float = Field(default=0.025)


class MeanReversionHardStopStrategy(BaseStrategy):
    key = "mean_reversion_hard_stop"
    name = "MeanReversionHardStop"
    description = "Mean reversion entries with one hard stop and no DCA. LONG-only SPOT scaffold."
    config_model = MeanReversionHardStopConfig

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        return StrategySignal(
            action="hold",
            reason="mean_reversion_hard_stop_placeholder",
            metadata={"symbol": context.symbol, "timeframe": context.timeframe},
        )
