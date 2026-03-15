from __future__ import annotations

SUPPORTED_EXCHANGE_CODES = {"coinbase", "binance_us"}

_EXCHANGE_CODE_ALIASES = {
    "coinbase": "coinbase",
    "binance_us": "binance_us",
    "binanceus": "binance_us",
    "binance.us": "binance_us",
}


def normalize_exchange_code(value: str) -> str:
    normalized = value.strip().lower()
    if not normalized:
        raise ValueError("Exchange code must not be empty")

    try:
        return _EXCHANGE_CODE_ALIASES[normalized]
    except KeyError as exc:
        raise ValueError(f"Unsupported exchange_code: {normalized}") from exc
