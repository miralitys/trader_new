from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.domain.schemas.common import ORMModel


class BacktestRunRequest(BaseModel):
    strategy_id: int
    symbols: list[str]
    timeframes: list[str]
    start: datetime
    end: datetime
    initial_capital: float = 10000.0
    position_sizing: float = 0.2
    fee_bps: float = 10.0
    slippage_bps: float = 5.0
    config_overrides: dict[str, Any] = Field(default_factory=dict)


class BacktestRunRead(ORMModel):
    id: int
    strategy_id: int
    status: str
    params: dict[str, Any]
    started_at: datetime | None = None
    ended_at: datetime | None = None
    error_message: str | None = None
    created_at: datetime


class BacktestResultRead(BaseModel):
    run: BacktestRunRead
    summary: dict[str, Any] = Field(default_factory=dict)
    equity_curve: list[dict[str, Any]] = Field(default_factory=list)
    trades: list[dict[str, Any]] = Field(default_factory=list)
