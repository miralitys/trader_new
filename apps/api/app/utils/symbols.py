from __future__ import annotations

from app.core.config import get_settings


def supported_symbol_codes() -> tuple[str, ...]:
    return tuple(symbol.upper() for symbol in get_settings().default_symbol_list)


def normalize_supported_symbol(value: str) -> str:
    normalized = value.strip().upper()
    if not normalized:
        raise ValueError("Symbol must not be empty")

    if normalized not in supported_symbol_codes():
        supported = ", ".join(supported_symbol_codes())
        raise ValueError(f"Unsupported symbol: {normalized}. Supported symbols: {supported}")

    return normalized


def compact_supported_symbols(values: list[str]) -> list[str]:
    normalized: list[str] = []
    for value in values:
        symbol = normalize_supported_symbol(value)
        if symbol not in normalized:
            normalized.append(symbol)
    return normalized
