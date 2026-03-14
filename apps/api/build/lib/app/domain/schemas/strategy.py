from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.domain.schemas.common import ORMModel


class StrategyConfigUpdate(BaseModel):
    settings: dict[str, Any] = Field(default_factory=dict)
    risk_settings: dict[str, Any] = Field(default_factory=dict)
    symbols: list[str] = Field(default_factory=list)
    timeframes: list[str] = Field(default_factory=list)
    paper_account_id: int | None = None


class StrategyModeChangeRequest(BaseModel):
    mode: str = "paper_trading"


class StrategyRunRead(ORMModel):
    id: int
    symbol_id: int
    timeframe_id: int
    mode: str
    status: str
    last_processed_candle: datetime | None = None


class SignalRead(ORMModel):
    id: int
    strategy_id: int
    symbol_id: int
    timeframe_id: int
    candle_time: datetime
    action: str
    side: str
    strength: float
    payload: dict[str, Any]


class PositionRead(ORMModel):
    id: int
    strategy_id: int
    symbol_id: int
    timeframe_id: int
    mode: str
    status: str
    entry_price: float
    quantity: float
    stop_loss: float | None = None
    take_profit: float | None = None
    opened_at: datetime
    closed_at: datetime | None = None
    unrealized_pnl: float
    realized_pnl: float


class TradeRead(ORMModel):
    id: int
    strategy_id: int
    position_id: int | None = None
    symbol_id: int
    timeframe_id: int
    entry_time: datetime
    exit_time: datetime
    entry_price: float
    exit_price: float
    quantity: float
    gross_pnl: float
    net_pnl: float
    fee: float
    slippage: float


class StrategyListItem(ORMModel):
    id: int
    key: str
    name: str
    description: str
    status: str
    is_enabled: bool
    last_signal_at: datetime | None = None
    last_processed_candle_at: datetime | None = None
    metrics: dict[str, Any] = Field(default_factory=dict)
    open_positions: int = 0


class StrategyDetail(StrategyListItem):
    config: dict[str, Any] = Field(default_factory=dict)
    runs: list[StrategyRunRead] = Field(default_factory=list)
    signals: list[SignalRead] = Field(default_factory=list)
    positions: list[PositionRead] = Field(default_factory=list)
    trades: list[TradeRead] = Field(default_factory=list)
    logs: list[dict[str, Any]] = Field(default_factory=list)
    equity_curve: list[dict[str, Any]] = Field(default_factory=list)
