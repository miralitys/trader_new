from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class DashboardStrategyCard(BaseModel):
    id: int
    key: str
    name: str
    status: str
    pnl: float = 0.0
    win_rate: float = 0.0
    number_of_trades: int = 0
    max_drawdown: float = 0.0
    profit_factor: float = 0.0
    expectancy: float = 0.0
    open_positions: int = 0
    last_signal_time: datetime | None = None
    last_processed_candle: datetime | None = None
    symbols: list[str] = Field(default_factory=list)
    timeframes: list[str] = Field(default_factory=list)


class DashboardRead(BaseModel):
    generated_at: datetime
    strategies: list[DashboardStrategyCard]
    sync_status: dict[str, Any]
    recent_backtests: list[dict[str, Any]] = Field(default_factory=list)
