from __future__ import annotations

from datetime import datetime
from typing import Iterator, Optional

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.core.config import Settings, get_settings
from app.integrations.binance_common import BinanceResearchTimeframe
from app.utils.research_symbols import to_binance_symbol


class BinanceSpotClientError(Exception):
    pass


class BinanceSpotClient:
    provider_name = "binance_spot"

    def __init__(
        self,
        settings: Optional[Settings] = None,
    ) -> None:
        app_settings = settings or get_settings()
        self.base_url = app_settings.binance_spot_api_base_url
        self.timeout_seconds = app_settings.funding_basis_timeout_seconds
        self.max_rows_per_request = app_settings.binance_spot_max_rows_per_request
        self.retry_attempts = app_settings.funding_basis_retry_attempts
        self.backoff_min_seconds = app_settings.funding_basis_backoff_min_seconds
        self.backoff_max_seconds = app_settings.funding_basis_backoff_max_seconds
        self._client = httpx.Client(
            base_url=self.base_url,
            timeout=self.timeout_seconds,
            headers={"Accept": "application/json", "User-Agent": "trader-research/0.1.0"},
        )

    def close(self) -> None:
        self._client.close()

    def iter_historical_klines(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> Iterator[list[list[object]]]:
        timeframe_value = BinanceResearchTimeframe.from_code(timeframe)
        for chunk_start, chunk_end in timeframe_value.iter_request_windows(
            start_at=start_at,
            end_at=end_at,
            max_rows_per_request=self.max_rows_per_request,
        ):
            yield self._request_klines(
                symbol=symbol,
                timeframe=timeframe_value,
                start_at=chunk_start,
                end_at=chunk_end,
            )

    def _request_klines(
        self,
        symbol: str,
        timeframe: BinanceResearchTimeframe,
        start_at: datetime,
        end_at: datetime,
    ) -> list[list[object]]:
        params = {
            "symbol": to_binance_symbol(symbol),
            "interval": timeframe.value,
            "startTime": int(start_at.timestamp() * 1000),
            "endTime": int(end_at.timestamp() * 1000),
            "limit": self.max_rows_per_request,
        }
        retrying = Retrying(
            stop=stop_after_attempt(self.retry_attempts),
            wait=wait_exponential(
                multiplier=1,
                min=self.backoff_min_seconds,
                max=self.backoff_max_seconds,
            ),
            retry=retry_if_exception_type((BinanceSpotClientError, httpx.HTTPError)),
            reraise=True,
        )
        for attempt in retrying:
            with attempt:
                response = self._client.get("/api/v3/klines", params=params)
                if response.status_code >= 400:
                    raise BinanceSpotClientError(
                        f"Binance spot request failed with status {response.status_code}: {response.text}"
                    )
                payload = response.json()
                if not isinstance(payload, list):
                    raise BinanceSpotClientError(f"Unexpected Binance spot payload: {payload!r}")
                return payload
        raise BinanceSpotClientError("Binance spot request failed after retries")
