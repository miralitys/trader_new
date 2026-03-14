from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.domain.enums import SignalAction, SignalSide


class BaseStrategyConfig(BaseModel):
    warmup_candles: int = 50
    position_size_pct: float = 0.2
    stop_loss_pct: float = 0.03
    take_profit_pct: float = 0.06
    fee_bps: float = 10.0
    slippage_bps: float = 5.0


@dataclass(slots=True)
class CandleInput:
    open_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(slots=True)
class PositionView:
    entry_price: float
    quantity: float
    stop_loss: float | None = None
    take_profit: float | None = None
    opened_at: datetime | None = None


@dataclass(slots=True)
class StrategyContext:
    strategy_id: int
    strategy_key: str
    symbol: str
    timeframe: str
    cash: float
    run_mode: str
    config: BaseStrategyConfig
    risk_settings: dict[str, Any] = field(default_factory=dict)
    position: PositionView | None = None

    @property
    def has_open_position(self) -> bool:
        return self.position is not None


@dataclass(slots=True)
class SignalDecision:
    action: SignalAction
    side: SignalSide = SignalSide.LONG
    strength: float = 0.0
    reason: str = ""
    stop_loss: float | None = None
    take_profit: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RiskDecision:
    approved: bool
    position_value: float = 0.0
    reason: str = ""


@dataclass(slots=True)
class ExecutionResult:
    action: SignalAction
    price: float
    quantity: float
    fee: float
    slippage: float
    notional: float
    notes: dict[str, Any] = field(default_factory=dict)
