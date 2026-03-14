from __future__ import annotations

from app.strategies.registry import list_strategies


def test_registry_contains_four_strategies() -> None:
    keys = {strategy.key for strategy in list_strategies()}
    assert keys == {
        "breakout_retest",
        "pullback_to_trend",
        "mean_reversion_hard_stop",
        "trend_retrace70",
    }
