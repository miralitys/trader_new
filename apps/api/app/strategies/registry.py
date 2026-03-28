from __future__ import annotations

from app.strategies.base import BaseStrategy

_STRATEGIES: dict[str, BaseStrategy] = {}


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
