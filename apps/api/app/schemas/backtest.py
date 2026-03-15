from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from app.integrations.binance_us import BinanceUSTimeframe
from app.utils.exchanges import normalize_exchange_code
from app.utils.symbols import normalize_supported_symbol


class BacktestRequest(BaseModel):
    strategy_code: str
    symbol: str
    timeframe: str
    start_at: datetime
    end_at: datetime
    exchange_code: str = "binance_us"
    initial_capital: Decimal = Field(default=Decimal("10000"), gt=0)
    fee: Decimal = Field(default=Decimal("0.001"), ge=0)
    slippage: Decimal = Field(default=Decimal("0.0005"), ge=0)
    position_size_pct: Decimal = Field(default=Decimal("1"), gt=0, le=1)
    strategy_config_override: dict[str, Any] = Field(default_factory=dict)

    @field_validator("timeframe")
    @classmethod
    def validate_timeframe(cls, value: str) -> str:
        BinanceUSTimeframe.from_code(value)
        return value

    @field_validator("strategy_code")
    @classmethod
    def validate_non_empty_string(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Value must not be empty")
        return normalized

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str) -> str:
        return normalize_supported_symbol(value)

    @field_validator("exchange_code")
    @classmethod
    def validate_exchange_code(cls, value: str) -> str:
        return normalize_exchange_code(value)

    @model_validator(mode="after")
    def validate_range(self) -> "BacktestRequest":
        if self.end_at <= self.start_at:
            raise ValueError("end_at must be greater than start_at")
        return self


class BacktestStopRequest(BaseModel):
    reason: str = "manual_stop"


class BacktestCandle(BaseModel):
    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal


class BacktestTrade(BaseModel):
    side: str = "long"
    entry_time: datetime
    exit_time: datetime
    entry_price: Decimal
    exit_price: Decimal
    qty: Decimal
    gross_pnl: Decimal
    pnl: Decimal
    pnl_pct: Decimal
    fees: Decimal
    slippage: Decimal
    exit_reason: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class EquityPoint(BaseModel):
    timestamp: datetime
    equity: Decimal
    cash: Decimal
    close_price: Decimal
    position_qty: Decimal = Decimal("0")


class BacktestMetrics(BaseModel):
    total_return_pct: Decimal = Decimal("0")
    max_drawdown_pct: Decimal = Decimal("0")
    win_rate_pct: Decimal = Decimal("0")
    profit_factor: Decimal = Decimal("0")
    expectancy: Decimal = Decimal("0")
    gross_expectancy: Decimal = Decimal("0")
    net_expectancy: Decimal = Decimal("0")
    avg_winner: Decimal = Decimal("0")
    avg_loser: Decimal = Decimal("0")
    total_trades: int = 0


class BacktestResponse(BaseModel):
    run_id: Optional[int] = None
    strategy_code: str
    symbol: str
    timeframe: str
    exchange_code: str = "binance_us"
    status: str
    initial_capital: Decimal
    final_equity: Decimal
    started_at: datetime
    completed_at: Optional[datetime] = None
    params: dict[str, Any] = Field(default_factory=dict)
    metrics: BacktestMetrics
    equity_curve: list[EquityPoint] = Field(default_factory=list)
    trades: list[BacktestTrade] = Field(default_factory=list)
    diagnostics: dict[str, Any] = Field(default_factory=dict)
    error_text: Optional[str] = None
