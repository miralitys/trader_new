"""Legacy strategy package kept only for abstract types during the research-first pivot."""

from app.strategies.base import BaseStrategy, BaseStrategyConfig, StrategyContext, StrategySignal
from app.strategies.ondo_short_delta_fade import (
    REGISTERED_ALPINE_SHORT_V7_STRATEGY,
    REGISTERED_ONDO_SHORT_STRATEGY,
    REGISTERED_ONDO_SHORT_V7_STRATEGY,
    REGISTERED_SHORT_FADE_LAB_STRATEGY,
    REGISTERED_SHORT_FADE_LAB_V5_STRATEGY,
    REGISTERED_SHORT_FADE_LAB_V6_STRATEGY,
)
from app.strategies.pattern_candidates import REGISTERED_PATTERN_CANDIDATES

__all__ = [
    "BaseStrategy",
    "BaseStrategyConfig",
    "StrategyContext",
    "StrategySignal",
    "REGISTERED_ALPINE_SHORT_V7_STRATEGY",
    "REGISTERED_ONDO_SHORT_STRATEGY",
    "REGISTERED_ONDO_SHORT_V7_STRATEGY",
    "REGISTERED_SHORT_FADE_LAB_STRATEGY",
    "REGISTERED_SHORT_FADE_LAB_V5_STRATEGY",
    "REGISTERED_SHORT_FADE_LAB_V6_STRATEGY",
    "REGISTERED_PATTERN_CANDIDATES",
]
