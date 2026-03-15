from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any


class BaseExchangeIntegration:
    provider_name = "base_exchange"

    def __init__(self, base_url: str, timeout_seconds: int) -> None:
        self.base_url = base_url
        self.timeout_seconds = timeout_seconds

    def healthcheck(self) -> dict[str, str]:
        return {"provider": self.provider_name, "status": "configured"}

    def fetch_historical_candles(self, symbol: str, timeframe: str, **_: Any) -> dict[str, Any]:
        return {
            "provider": self.provider_name,
            "symbol": symbol,
            "timeframe": timeframe,
            "status": "not_implemented",
        }


@dataclass(frozen=True)
class NormalizedCandle:
    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
