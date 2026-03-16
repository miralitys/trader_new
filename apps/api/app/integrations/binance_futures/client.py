from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterator, Optional

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.core.config import Settings, get_settings
from app.integrations.binance_common import BinanceResearchTimeframe
from app.utils.research_symbols import to_binance_symbol
from app.utils.time import ensure_utc


class BinanceFuturesClientError(Exception):
    pass


class BinanceFuturesClient:
    provider_name = "binance_futures"

    def __init__(self, settings: Optional[Settings] = None) -> None:
        app_settings = settings or get_settings()
        self.base_url = app_settings.binance_futures_api_base_url
        self.timeout_seconds = app_settings.funding_basis_timeout_seconds
        self.max_rows_per_request = app_settings.binance_futures_max_rows_per_request
        self.max_open_interest_rows = app_settings.binance_futures_open_interest_max_rows_per_request
        self.max_funding_rows = app_settings.binance_futures_max_funding_rows_per_request
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

    def iter_mark_price_klines(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> Iterator[list[list[object]]]:
        yield from self._iter_kline_endpoint("/fapi/v1/markPriceKlines", symbol, timeframe, start_at, end_at)

    def iter_index_price_klines(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> Iterator[list[list[object]]]:
        yield from self._iter_kline_endpoint("/fapi/v1/indexPriceKlines", symbol, timeframe, start_at, end_at)

    def iter_trade_klines(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> Iterator[list[list[object]]]:
        yield from self._iter_kline_endpoint("/fapi/v1/klines", symbol, timeframe, start_at, end_at)

    def iter_open_interest_hist(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> Iterator[list[dict[str, object]]]:
        timeframe_value = BinanceResearchTimeframe.from_code(timeframe)
        cursor = ensure_utc(start_at)
        normalized_end = ensure_utc(end_at)
        while cursor < normalized_end:
            payload = self._request_json(
                "/futures/data/openInterestHist",
                params={
                    "symbol": to_binance_symbol(symbol),
                    "period": timeframe_value.value,
                    "startTime": int(cursor.timestamp() * 1000),
                    "endTime": int(normalized_end.timestamp() * 1000),
                    "limit": self.max_open_interest_rows,
                },
            )
            if not isinstance(payload, list):
                raise BinanceFuturesClientError(f"Unexpected open interest payload: {payload!r}")
            yield payload
            if not payload:
                break
            last_ts_ms = int(str(payload[-1]["timestamp"]))
            next_cursor = datetime.utcfromtimestamp(last_ts_ms / 1000).replace(tzinfo=cursor.tzinfo) + timeframe_value.interval
            if next_cursor <= cursor:
                break
            cursor = next_cursor

    def iter_funding_rates(
        self,
        symbol: str,
        start_at: datetime,
        end_at: datetime,
    ) -> Iterator[list[dict[str, object]]]:
        cursor = ensure_utc(start_at)
        normalized_end = ensure_utc(end_at)
        while cursor < normalized_end:
            payload = self._request_json(
                "/fapi/v1/fundingRate",
                params={
                    "symbol": to_binance_symbol(symbol),
                    "startTime": int(cursor.timestamp() * 1000),
                    "endTime": int(normalized_end.timestamp() * 1000),
                    "limit": self.max_funding_rows,
                },
            )
            if not isinstance(payload, list):
                raise BinanceFuturesClientError(f"Unexpected funding payload: {payload!r}")
            yield payload
            if not payload:
                break
            last_ts_ms = int(str(payload[-1]["fundingTime"]))
            next_cursor = datetime.utcfromtimestamp(last_ts_ms / 1000).replace(tzinfo=cursor.tzinfo) + timedelta(milliseconds=1)
            if next_cursor <= cursor:
                break
            cursor = next_cursor

    def _iter_kline_endpoint(
        self,
        path: str,
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
            payload = self._request_json(
                path,
                params={
                    "symbol": to_binance_symbol(symbol),
                    "interval": timeframe_value.value,
                    "startTime": int(chunk_start.timestamp() * 1000),
                    "endTime": int(chunk_end.timestamp() * 1000),
                    "limit": self.max_rows_per_request,
                },
            )
            if not isinstance(payload, list):
                raise BinanceFuturesClientError(f"Unexpected futures kline payload: {payload!r}")
            yield payload

    def _request_json(self, path: str, params: dict[str, object]) -> object:
        retrying = Retrying(
            stop=stop_after_attempt(self.retry_attempts),
            wait=wait_exponential(
                multiplier=1,
                min=self.backoff_min_seconds,
                max=self.backoff_max_seconds,
            ),
            retry=retry_if_exception_type((BinanceFuturesClientError, httpx.HTTPError)),
            reraise=True,
        )
        for attempt in retrying:
            with attempt:
                response = self._client.get(path, params=params)
                if response.status_code >= 400:
                    raise BinanceFuturesClientError(
                        f"Binance futures request failed with status {response.status_code}: {response.text}"
                    )
                return response.json()
        raise BinanceFuturesClientError("Binance futures request failed after retries")
