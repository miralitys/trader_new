"""Legacy strategy package kept only for abstract types during the research-first pivot."""

from app.strategies.base import BaseStrategy, BaseStrategyConfig, StrategyContext, StrategySignal
from app.strategies.pattern_candidates import REGISTERED_PATTERN_CANDIDATES

__all__ = [
    "BaseStrategy",
    "BaseStrategyConfig",
    "StrategyContext",
    "StrategySignal",
    "REGISTERED_PATTERN_CANDIDATES",
]
