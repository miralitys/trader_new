from __future__ import annotations

from datetime import datetime
from typing import Iterator, Optional

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.core.config import Settings, get_settings
from app.core.logging import get_logger
from app.integrations.coinbase.schemas import CoinbaseTimeframe
from app.utils.time import to_iso8601

logger = get_logger(__name__)


class CoinbaseClientError(Exception):
    """Base exception for Coinbase ingestion failures."""


class CoinbaseRateLimitError(CoinbaseClientError):
    """Raised when Coinbase returns an HTTP 429."""


class CoinbaseTransientError(CoinbaseClientError):
    """Raised for retryable server-side failures."""


class CoinbaseResponseError(CoinbaseClientError):
    """Raised when the response payload is not in the expected format."""


class CoinbaseClient:
    provider_name = "coinbase"

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
        self.base_url = base_url or app_settings.coinbase_api_base_url
        self.timeout_seconds = timeout_seconds or app_settings.coinbase_timeout_seconds
        self.max_candles_per_request = (
            max_candles_per_request or app_settings.coinbase_max_candles_per_request
        )
        self.retry_attempts = retry_attempts or app_settings.coinbase_retry_attempts
        self.backoff_min_seconds = backoff_min_seconds or app_settings.coinbase_backoff_min_seconds
        self.backoff_max_seconds = backoff_max_seconds or app_settings.coinbase_backoff_max_seconds
        self._client = httpx.Client(
            base_url=self.base_url,
            timeout=self.timeout_seconds,
            headers={
                "Accept": "application/json",
                "User-Agent": "trader-mvp-ingestion/0.1.0",
            },
        )

    def __enter__(self) -> "CoinbaseClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

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
        timeframe_value = CoinbaseTimeframe.from_code(timeframe)
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
        timeframe: CoinbaseTimeframe,
        start_at: datetime,
        end_at: datetime,
    ) -> list[list[object]]:
        params = {
            "granularity": timeframe.granularity_seconds,
            "start": to_iso8601(start_at),
            "end": to_iso8601(end_at),
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
                    CoinbaseRateLimitError,
                    CoinbaseTransientError,
                    httpx.NetworkError,
                    httpx.TimeoutException,
                    httpx.RemoteProtocolError,
                )
            ),
            reraise=True,
        )

        for attempt in retrying:
            with attempt:
                response = self._client.get(f"/products/{symbol}/candles", params=params)
                if response.status_code == 429:
                    raise CoinbaseRateLimitError("Coinbase rate limit exceeded")
                if 500 <= response.status_code < 600:
                    raise CoinbaseTransientError(
                        f"Coinbase server error {response.status_code}: {response.text}"
                    )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, list):
                    raise CoinbaseResponseError(f"Unexpected candle payload: {payload!r}")

                logger.debug(
                    "Fetched Coinbase candles chunk",
                    extra={
                        "symbol": symbol,
                        "timeframe": timeframe.value,
                        "start_at": params["start"],
                        "end_at": params["end"],
                        "row_count": len(payload),
                    },
                )
                return payload

        raise CoinbaseClientError("Coinbase candle request failed after retries")
