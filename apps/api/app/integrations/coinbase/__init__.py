"""Coinbase market data integration."""

from app.integrations.coinbase.client import CoinbaseClient
from app.integrations.coinbase.schemas import CoinbaseTimeframe, NormalizedCandle, normalize_coinbase_candles

CoinbaseIntegration = CoinbaseClient

__all__ = [
    "CoinbaseClient",
    "CoinbaseIntegration",
    "CoinbaseTimeframe",
    "NormalizedCandle",
    "normalize_coinbase_candles",
]
