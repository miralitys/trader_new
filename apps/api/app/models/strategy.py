from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import DateTime, ForeignKey, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import (
    AppModel,
    CreatedAtMixin,
    PERCENT_NUMERIC,
    PRICE_NUMERIC,
    QUANTITY_NUMERIC,
    SIGNAL_STRENGTH_NUMERIC,
    TimestampMixin,
)
from app.models.enums import OrderStatus, OrderType, PositionStatus, Side, SignalType
from app.models.enums import StrategyRunMode, StrategyRunStatus, pg_enum


class StrategyConfig(AppModel, TimestampMixin):
    __tablename__ = "strategy_configs"

    strategy_id: Mapped[int] = mapped_column(ForeignKey("strategies.id"), nullable=False, index=True)
    config_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)


class StrategyRun(AppModel, CreatedAtMixin):
    __tablename__ = "strategy_runs"

    strategy_id: Mapped[int] = mapped_column(ForeignKey("strategies.id"), nullable=False, index=True)
    mode: Mapped[StrategyRunMode] = mapped_column(
        pg_enum(StrategyRunMode, "strategy_run_mode_enum"),
        nullable=False,
        default=StrategyRunMode.PAPER,
    )
    status: Mapped[StrategyRunStatus] = mapped_column(
        pg_enum(StrategyRunStatus, "strategy_run_status_enum"),
        nullable=False,
        default=StrategyRunStatus.CREATED,
    )
    symbols_json: Mapped[list[str]] = mapped_column(JSONB, default=list, nullable=False)
    timeframes_json: Mapped[list[str]] = mapped_column(JSONB, default=list, nullable=False)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    stopped_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_processed_candle_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)


class Signal(AppModel, CreatedAtMixin):
    __tablename__ = "signals"

    strategy_run_id: Mapped[int] = mapped_column(ForeignKey("strategy_runs.id"), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    timeframe: Mapped[str] = mapped_column(Text, nullable=False)
    signal_type: Mapped[SignalType] = mapped_column(
        pg_enum(SignalType, "signal_type_enum"),
        nullable=False,
        default=SignalType.HOLD,
    )
    signal_strength: Mapped[float] = mapped_column(SIGNAL_STRENGTH_NUMERIC, nullable=False, default=0)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
    candle_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Order(AppModel, TimestampMixin):
    __tablename__ = "orders"

    strategy_run_id: Mapped[int] = mapped_column(ForeignKey("strategy_runs.id"), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    side: Mapped[Side] = mapped_column(pg_enum(Side, "side_enum"), nullable=False, default=Side.LONG)
    order_type: Mapped[OrderType] = mapped_column(
        pg_enum(OrderType, "order_type_enum"),
        nullable=False,
        default=OrderType.SIMULATED,
    )
    qty: Mapped[float] = mapped_column(QUANTITY_NUMERIC, nullable=False)
    price: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False)
    status: Mapped[OrderStatus] = mapped_column(
        pg_enum(OrderStatus, "order_status_enum"),
        nullable=False,
        default=OrderStatus.NEW,
    )
    linked_signal_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("signals.id"),
        nullable=True,
        index=True,
    )


class Position(AppModel):
    __tablename__ = "positions"

    strategy_run_id: Mapped[int] = mapped_column(ForeignKey("strategy_runs.id"), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    side: Mapped[Side] = mapped_column(pg_enum(Side, "side_enum"), nullable=False, default=Side.LONG)
    qty: Mapped[float] = mapped_column(QUANTITY_NUMERIC, nullable=False)
    avg_entry_price: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False)
    stop_price: Mapped[Optional[float]] = mapped_column(PRICE_NUMERIC, nullable=True)
    take_profit_price: Mapped[Optional[float]] = mapped_column(PRICE_NUMERIC, nullable=True)
    status: Mapped[PositionStatus] = mapped_column(
        pg_enum(PositionStatus, "position_status_enum"),
        nullable=False,
        default=PositionStatus.OPEN,
    )
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    closed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class Trade(AppModel):
    __tablename__ = "trades"

    strategy_run_id: Mapped[int] = mapped_column(ForeignKey("strategy_runs.id"), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    entry_price: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False)
    exit_price: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False)
    qty: Mapped[float] = mapped_column(QUANTITY_NUMERIC, nullable=False)
    pnl: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False)
    pnl_pct: Mapped[float] = mapped_column(PERCENT_NUMERIC, nullable=False)
    fees: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False, default=0)
    slippage: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False, default=0)
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    closed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
