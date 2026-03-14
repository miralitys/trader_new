from __future__ import annotations

from app.engines.base import EngineBase


class StrategyEngine(EngineBase):
    engine_name = "strategy_engine"
    purpose = "Strategy orchestration, signal generation, and execution handoff."

    def evaluate_market_event(self, strategy_key: str, symbol: str, timeframe: str) -> dict[str, str | bool]:
        payload = self.describe()
        payload.update(
            {
                "strategy_key": strategy_key,
                "symbol": symbol,
                "timeframe": timeframe,
                "evaluation": "not_implemented",
            }
        )
        return payload
