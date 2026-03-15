from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator

from app.integrations.binance_us import BinanceUSTimeframe
from app.utils.exchanges import normalize_exchange_code


class PaperRunStartRequest(BaseModel):
    strategy_code: str
    symbols: list[str]
    timeframes: list[str]
    exchange_code: str = "binance_us"
    initial_balance: Decimal = Field(default=Decimal("10000"), gt=0)
    currency: str = "USD"
    fee: Decimal = Field(default=Decimal("0.001"), ge=0)
    slippage: Decimal = Field(default=Decimal("0.0005"), ge=0)
    strategy_config_override: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("strategy_code", "currency")
    @classmethod
    def validate_non_empty_string(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Value must not be empty")
        return normalized

    @field_validator("exchange_code")
    @classmethod
    def validate_exchange_code(cls, value: str) -> str:
        return normalize_exchange_code(value)

    @field_validator("symbols")
    @classmethod
    def validate_symbols(cls, value: list[str]) -> list[str]:
        normalized = [symbol.strip() for symbol in value if symbol.strip()]
        if not normalized:
            raise ValueError("At least one symbol is required")
        return normalized

    @field_validator("timeframes")
    @classmethod
    def validate_timeframes(cls, value: list[str]) -> list[str]:
        normalized = [timeframe.strip() for timeframe in value if timeframe.strip()]
        if not normalized:
            raise ValueError("At least one timeframe is required")
        for timeframe in normalized:
            BinanceUSTimeframe.from_code(timeframe)
        return normalized


class PaperRunResponse(BaseModel):
    run_id: int
    strategy_code: str
    status: str
    symbols: list[str]
    timeframes: list[str]
    exchange_code: str
    account_balance: Decimal
    currency: str
    last_processed_candle_at: Optional[datetime] = None
    processed_candles: int = 0
    signals_created: int = 0
    orders_created: int = 0
    trades_created: int = 0
    error_text: Optional[str] = None
