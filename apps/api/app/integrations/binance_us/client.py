from __future__ import annotations

from datetime import datetime
from typing import Iterator, Optional

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.core.config import Settings, get_settings
from app.core.logging import get_logger
from app.integrations.binance_us.schemas import BinanceUSTimeframe, normalize_binance_us_symbol

logger = get_logger(__name__)


class BinanceUSClientError(Exception):
    """Base exception for Binance.US ingestion failures."""


class BinanceUSRateLimitError(BinanceUSClientError):
    """Raised when Binance.US rate limits a request."""


class BinanceUSTransientError(BinanceUSClientError):
    """Raised for retryable Binance.US failures."""


class BinanceUSResponseError(BinanceUSClientError):
    """Raised when the response payload is not in the expected format."""


class BinanceUSClient:
    provider_name = "binance_us"

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        max_candles_per_request: Optional[int] = None,
        retry_attempts: Optional[int] = None,
        backoff_min_seconds: Optional[float] = None,
        backoff_max_seconds: Optional[float] = None,
        settings: Optional[Settings] = None,
    ) -> None:
        app_settings = settings or get_settings()
        self.base_url = base_url or app_settings.binance_us_api_base_url
        self.timeout_seconds = timeout_seconds or app_settings.binance_us_timeout_seconds
        self.max_candles_per_request = (
            max_candles_per_request or app_settings.binance_us_max_candles_per_request
        )
        self.retry_attempts = retry_attempts or app_settings.binance_us_retry_attempts
        self.backoff_min_seconds = backoff_min_seconds or app_settings.binance_us_backoff_min_seconds
        self.backoff_max_seconds = backoff_max_seconds or app_settings.binance_us_backoff_max_seconds
        self._client = httpx.Client(
            base_url=self.base_url,
            timeout=self.timeout_seconds,
            headers={
                "Accept": "application/json",
                "User-Agent": "trader-mvp-ingestion/0.1.0",
            },
        )

    def close(self) -> None:
        self._client.close()

    def healthcheck(self) -> dict[str, str]:
        return {"provider": self.provider_name, "status": "configured"}

    def fetch_historical_candles(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> list[list[object]]:
        rows: list[list[object]] = []
        for chunk_rows in self.iter_historical_candles(
            symbol=symbol,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
        ):
            rows.extend(chunk_rows)
        return rows

    def iter_historical_candles(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> Iterator[list[list[object]]]:
        timeframe_value = BinanceUSTimeframe.from_code(timeframe)
        for chunk_start, chunk_end in timeframe_value.iter_request_windows(
            start_at=start_at,
            end_at=end_at,
            max_candles_per_request=self.max_candles_per_request,
        ):
            yield self._request_candles(
                symbol=symbol,
                timeframe=timeframe_value,
                start_at=chunk_start,
                end_at=chunk_end,
            )

    def _request_candles(
        self,
        symbol: str,
        timeframe: BinanceUSTimeframe,
        start_at: datetime,
        end_at: datetime,
    ) -> list[list[object]]:
        params = {
            "symbol": normalize_binance_us_symbol(symbol),
            "interval": timeframe.api_interval,
            "startTime": int(start_at.timestamp() * 1000),
            "endTime": int(end_at.timestamp() * 1000),
            "limit": self.max_candles_per_request,
        }

        retrying = Retrying(
            stop=stop_after_attempt(self.retry_attempts),
            wait=wait_exponential(
                multiplier=1,
                min=self.backoff_min_seconds,
                max=self.backoff_max_seconds,
            ),
            retry=retry_if_exception_type(
                (
                    BinanceUSRateLimitError,
                    BinanceUSTransientError,
                    httpx.NetworkError,
                    httpx.TimeoutException,
                    httpx.RemoteProtocolError,
                )
            ),
            reraise=True,
        )

        for attempt in retrying:
            with attempt:
                response = self._client.get("/api/v3/klines", params=params)
                if response.status_code in {418, 429}:
                    raise BinanceUSRateLimitError("Binance.US rate limit exceeded")
                if 500 <= response.status_code < 600:
                    raise BinanceUSTransientError(
                        f"Binance.US server error {response.status_code}: {response.text}"
                    )
                if 400 <= response.status_code < 500:
                    try:
                        payload = response.json()
                    except ValueError:
                        payload = None
                    if isinstance(payload, dict) and payload.get("msg"):
                        raise BinanceUSResponseError(str(payload["msg"]))
                    raise BinanceUSResponseError(
                        f"Binance.US request failed with status {response.status_code}"
                    )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, list):
                    raise BinanceUSResponseError(f"Unexpected Binance.US payload: {payload!r}")

                logger.debug(
                    "Fetched Binance.US candles chunk",
                    extra={
                        "symbol": symbol,
                        "timeframe": timeframe.value,
                        "start_at": params["startTime"],
                        "end_at": params["endTime"],
                        "row_count": len(payload),
                    },
                )
                return payload

        raise BinanceUSClientError("Binance.US candle request failed after retries")
