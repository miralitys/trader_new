from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.core.config import get_settings
from app.exchanges.base import ExchangeAdapter, NormalizedCandle


class CoinbaseAdapter(ExchangeAdapter):
    granularity_map = {"5m": 300, "15m": 900, "1h": 3600}
    max_candles_per_request = 300

    def __init__(self) -> None:
        settings = get_settings()
        self.base_url = settings.coinbase_api_base_url.rstrip("/")
        self.timeout_seconds = settings.coinbase_timeout_seconds

    def fetch_candles(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> list[NormalizedCandle]:
        symbol = symbol.upper()
        granularity = self.granularity_map[timeframe]
        windows = self._chunk_windows(start, end, granularity)
        candles: dict[datetime, NormalizedCandle] = {}
        for window_start, window_end in windows:
            for candle in self._fetch_chunk(symbol, granularity, window_start, window_end):
                candles[candle.open_time] = candle
        return [candles[key] for key in sorted(candles)]

    def _chunk_windows(
        self,
        start: datetime,
        end: datetime,
        granularity: int,
    ) -> list[tuple[datetime, datetime]]:
        max_span = granularity * self.max_candles_per_request
        windows: list[tuple[datetime, datetime]] = []
        cursor = start
        while cursor < end:
            window_end = min(cursor + timedelta(seconds=max_span), end)
            windows.append((cursor, window_end))
            cursor = window_end
        return windows

    @retry(
        retry=retry_if_exception_type((httpx.HTTPError, RuntimeError)),
        wait=wait_exponential(multiplier=1, min=1, max=20),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def _fetch_chunk(
        self,
        symbol: str,
        granularity: int,
        start: datetime,
        end: datetime,
    ) -> list[NormalizedCandle]:
        url = f"{self.base_url}/products/{symbol}/candles"
        params = {
            "granularity": granularity,
            "start": start.astimezone(timezone.utc).isoformat(),
            "end": end.astimezone(timezone.utc).isoformat(),
        }
        headers = {"Accept": "application/json", "User-Agent": "trader-mvp"}
        with httpx.Client(timeout=self.timeout_seconds, headers=headers) as client:
            response = client.get(url, params=params)
            if response.status_code in {429, 500, 502, 503, 504}:
                raise RuntimeError(f"Coinbase transient error {response.status_code}")
            response.raise_for_status()
            payload = response.json()
        return self._normalize_response(payload, granularity)

    def _normalize_response(self, payload: list[list[float]], granularity: int) -> list[NormalizedCandle]:
        candles: list[NormalizedCandle] = []
        for item in payload:
            timestamp, low, high, open_price, close_price, volume = item[:6]
            open_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            candles.append(
                NormalizedCandle(
                    open_time=open_time,
                    close_time=open_time + timedelta(seconds=granularity),
                    open=float(open_price),
                    high=float(high),
                    low=float(low),
                    close=float(close_price),
                    volume=float(volume),
                )
            )
        candles.sort(key=lambda candle: candle.open_time)
        return candles
