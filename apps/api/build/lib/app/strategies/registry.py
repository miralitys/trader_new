from __future__ import annotations

from app.strategies.base import BaseStrategy
from app.strategies.breakout_retest import BreakoutRetestStrategy
from app.strategies.mean_reversion_hard_stop import MeanReversionHardStopStrategy
from app.strategies.pullback_to_trend import PullbackToTrendStrategy
from app.strategies.trend_retrace70 import TrendRetrace70Strategy


_STRATEGIES: dict[str, BaseStrategy] = {
    strategy.key: strategy
    for strategy in [
        BreakoutRetestStrategy(),
        PullbackToTrendStrategy(),
        MeanReversionHardStopStrategy(),
        TrendRetrace70Strategy(),
    ]
}


def list_strategies() -> list[BaseStrategy]:
    return list(_STRATEGIES.values())


def get_strategy(key: str) -> BaseStrategy:
    return _STRATEGIES[key]
