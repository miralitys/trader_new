from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Optional

from app.engines.base import EngineBase
from app.services.market_data_service import MarketDataService


class MarketDataEngine(EngineBase):
    engine_name = "market_data_engine"
    purpose = "Historical ingestion, validation, deduplication, and sync orchestration."

    def __init__(self, service: Optional[MarketDataService] = None) -> None:
        self.service = service or MarketDataService()

    def schedule_sync(self, symbol: str, timeframe: str) -> dict[str, object]:
        payload = self.describe()
        payload.update({"symbol": symbol, "timeframe": timeframe, "scheduled": True})
        return payload

    def initial_sync(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> dict[str, object]:
        payload = self.describe()
        payload.update(
            asdict(
                self.service.initial_historical_sync(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_at=start_at,
                    end_at=end_at,
                )
            )
        )
        return payload

    def incremental_sync(self, symbol: str, timeframe: str) -> dict[str, object]:
        payload = self.describe()
        payload.update(asdict(self.service.incremental_sync(symbol=symbol, timeframe=timeframe)))
        return payload

    def manual_sync(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> dict[str, object]:
        payload = self.describe()
        payload.update(
            asdict(
                self.service.manual_sync(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_at=start_at,
                    end_at=end_at,
                )
            )
        )
        return payload
