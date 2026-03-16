from __future__ import annotations

from datetime import datetime
from typing import Iterator, Optional

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.core.config import Settings, get_settings
from app.integrations.binance_common import BinanceResearchTimeframe
from app.utils.research_symbols import to_okx_index_inst_id, to_okx_swap_inst_id
from app.utils.time import ensure_utc


class OkxPerpClientError(Exception):
    pass


class OkxPerpClient:
    provider_name = "okx_swap"

    def __init__(self, settings: Optional[Settings] = None) -> None:
        app_settings = settings or get_settings()
        self.base_url = app_settings.okx_api_base_url
        self.timeout_seconds = app_settings.funding_basis_timeout_seconds
        self.max_rows_per_request = app_settings.okx_max_rows_per_request
        self.max_funding_rows = app_settings.okx_max_funding_rows_per_request
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
        yield from self._iter_candle_endpoint(
            "/api/v5/market/history-mark-price-candles",
            inst_id=to_okx_swap_inst_id(symbol),
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
        )

    def iter_index_price_klines(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> Iterator[list[list[object]]]:
        yield from self._iter_candle_endpoint(
            "/api/v5/market/history-index-candles",
            inst_id=to_okx_index_inst_id(symbol),
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
        )

    def iter_trade_klines(
        self,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> Iterator[list[list[object]]]:
        yield from self._iter_candle_endpoint(
            "/api/v5/market/history-candles",
            inst_id=to_okx_swap_inst_id(symbol),
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
        )

    def iter_funding_rates(
        self,
        symbol: str,
        start_at: datetime,
        end_at: datetime,
    ) -> Iterator[list[dict[str, object]]]:
        normalized_start = ensure_utc(start_at)
        normalized_end = ensure_utc(end_at)
        start_ms = int(normalized_start.timestamp() * 1000)
        cursor_ms = int(normalized_end.timestamp() * 1000) + 1

        while True:
            payload = self._request_json(
                "/api/v5/public/funding-rate-history",
                params={
                    "instId": to_okx_swap_inst_id(symbol),
                    "limit": str(self.max_funding_rows),
                    "after": str(cursor_ms),
                },
            )
            data = payload.get("data")
            if not isinstance(data, list):
                raise OkxPerpClientError(f"Unexpected OKX funding payload: {payload!r}")
            if not data:
                break

            page_rows: list[dict[str, object]] = []
            oldest_ms = None
            for row in data:
                funding_ms = int(str(row["fundingTime"]))
                oldest_ms = funding_ms if oldest_ms is None else min(oldest_ms, funding_ms)
                if funding_ms < start_ms or funding_ms > int(normalized_end.timestamp() * 1000):
                    continue
                page_rows.append(row)

            if page_rows:
                yield page_rows

            if oldest_ms is None or oldest_ms <= start_ms:
                break
            cursor_ms = oldest_ms

    def _iter_candle_endpoint(
        self,
        path: str,
        *,
        inst_id: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> Iterator[list[list[object]]]:
        timeframe_value = BinanceResearchTimeframe.from_code(timeframe)
        start_ms = int(ensure_utc(start_at).timestamp() * 1000)
        end_ms = int(ensure_utc(end_at).timestamp() * 1000)
        cursor_ms = end_ms + 1

        while True:
            payload = self._request_json(
                path,
                params={
                    "instId": inst_id,
                    "bar": self._to_okx_bar(timeframe_value),
                    "limit": str(self.max_rows_per_request),
                    "after": str(cursor_ms),
                },
            )
            data = payload.get("data")
            if not isinstance(data, list):
                raise OkxPerpClientError(f"Unexpected OKX candle payload: {payload!r}")
            if not data:
                break

            page_rows: list[list[object]] = []
            oldest_ms = None
            for row in data:
                if not isinstance(row, list) or not row:
                    continue
                ts_ms = int(str(row[0]))
                oldest_ms = ts_ms if oldest_ms is None else min(oldest_ms, ts_ms)
                if ts_ms < start_ms or ts_ms > end_ms:
                    continue
                page_rows.append(row)

            if page_rows:
                yield page_rows

            if oldest_ms is None or oldest_ms <= start_ms:
                break
            cursor_ms = oldest_ms

    def _request_json(self, path: str, params: dict[str, object]) -> dict[str, object]:
        retrying = Retrying(
            stop=stop_after_attempt(self.retry_attempts),
            wait=wait_exponential(
                multiplier=1,
                min=self.backoff_min_seconds,
                max=self.backoff_max_seconds,
            ),
            retry=retry_if_exception_type((OkxPerpClientError, httpx.HTTPError)),
            reraise=True,
        )
        for attempt in retrying:
            with attempt:
                response = self._client.get(path, params=params)
                if response.status_code >= 400:
                    raise OkxPerpClientError(f"OKX request failed with status {response.status_code}: {response.text}")
                payload = response.json()
                if payload.get("code") != "0":
                    raise OkxPerpClientError(f"OKX response error: {payload!r}")
                return payload
        raise OkxPerpClientError("OKX request failed after retries")

    @staticmethod
    def _to_okx_bar(timeframe: BinanceResearchTimeframe) -> str:
        mapping = {
            BinanceResearchTimeframe.FIVE_MINUTES: "5m",
            BinanceResearchTimeframe.FIFTEEN_MINUTES: "15m",
            BinanceResearchTimeframe.ONE_HOUR: "1H",
        }
        return mapping[timeframe]
