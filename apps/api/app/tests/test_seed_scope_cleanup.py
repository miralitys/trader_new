from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.db.seed import (
    _sanitize_strategy_config_payload,
    _strategy_run_out_of_scope,
)
from app.strategies.base import BaseStrategyConfig


def test_sanitize_strategy_config_payload_keeps_only_supported_symbols() -> None:
    payload = {
        "symbols": ["BTC-USDT", "ARB-USDT", "sol-usdt", "ETH-USDT", "BTC-USD"],
        "exchange_code": "coinbase",
        "timeframes": ["5m"],
    }

    sanitized = _sanitize_strategy_config_payload(payload, {"BTC-USDT", "ETH-USDT", "SOL-USDT"})

    assert sanitized["symbols"] == ["BTC-USDT", "SOL-USDT", "ETH-USDT"]
    assert sanitized["exchange_code"] == "binance_us"
    assert sanitized["timeframes"] == ["5m"]


def test_strategy_run_out_of_scope_rejects_unsupported_symbols_or_exchange() -> None:
    allowed_symbols = {"BTC-USDT", "ETH-USDT", "SOL-USDT"}

    assert _strategy_run_out_of_scope(
        SimpleNamespace(
            metadata_json={"exchange_code": "binance_us"},
            symbols_json=["BTC-USDT", "ARB-USDT"],
        ),
        allowed_exchange_code="binance_us",
        allowed_symbols=allowed_symbols,
    )

    assert _strategy_run_out_of_scope(
        SimpleNamespace(
            metadata_json={"exchange_code": "coinbase"},
            symbols_json=["BTC-USDT"],
        ),
        allowed_exchange_code="binance_us",
        allowed_symbols=allowed_symbols,
    )


def test_base_strategy_config_rejects_unsupported_symbols() -> None:
    with pytest.raises(ValueError, match="Unsupported symbol"):
        BaseStrategyConfig(symbols=["BTC-USDT", "ARB-USDT"])


def test_base_strategy_config_accepts_new_supported_symbols() -> None:
    config = BaseStrategyConfig(
        symbols=["ICP-USDT", "GALA-USDT", "AXS-USDT", "ONDO-USDT", "IOTA-USDT", "FIL-USDT"]
    )

    assert config.symbols == ["ICP-USDT", "GALA-USDT", "AXS-USDT", "ONDO-USDT", "IOTA-USDT", "FIL-USDT"]
