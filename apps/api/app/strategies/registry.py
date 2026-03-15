from __future__ import annotations

from app.strategies.base import BaseStrategy
from app.strategies.breakout_retest import BreakoutRetestStrategy
from app.strategies.pullback_to_trend import PullbackToTrendStrategy
from app.strategies.trend_retrace_70 import TrendRetrace70Strategy


_STRATEGIES: dict[str, BaseStrategy] = {
    strategy.key: strategy
    for strategy in [
        BreakoutRetestStrategy(),
        PullbackToTrendStrategy(),
        TrendRetrace70Strategy(),
    ]
}


def list_strategies() -> list[BaseStrategy]:
    return list(_STRATEGIES.values())


def get_strategy(key: str) -> BaseStrategy:
    return _STRATEGIES[key]


def register_strategy(strategy: BaseStrategy) -> BaseStrategy:
    _STRATEGIES[strategy.key] = strategy
    return strategy


def unregister_strategy(key: str) -> None:
    _STRATEGIES.pop(key, None)


def strategy_descriptors() -> list[dict[str, str | bool]]:
    return [strategy.descriptor().model_dump() for strategy in list_strategies()]
