from __future__ import annotations

from app.strategies.base import BaseStrategy
from app.strategies.registry import (
    get_strategy,
    list_strategies,
    register_strategy,
    strategy_descriptors,
    unregister_strategy,
)


def test_strategy_registry_contains_four_scaffolded_strategies() -> None:
    keys = {strategy.key for strategy in list_strategies()}
    assert keys == {
        "breakout_retest",
        "pullback_to_trend",
        "mean_reversion_hard_stop",
        "trend_retrace_70",
    }


def test_strategy_descriptors_are_serializable() -> None:
    descriptors = strategy_descriptors()
    assert len(descriptors) == 4
    assert all(item["status"] == "scaffold" for item in descriptors)


class TestStrategy(BaseStrategy):
    key = "test_registry_strategy"
    name = "TestRegistryStrategy"
    description = "Temporary strategy used for registry tests."


def test_registry_can_register_and_unregister_strategies() -> None:
    strategy = TestStrategy()

    register_strategy(strategy)
    try:
        resolved = get_strategy(strategy.key)
        assert resolved.key == strategy.key
        assert resolved.name == strategy.name
    finally:
        unregister_strategy(strategy.key)
