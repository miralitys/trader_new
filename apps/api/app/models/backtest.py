from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import DateTime, ForeignKey, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import AppModel, CreatedAtMixin, PERCENT_NUMERIC, PRICE_NUMERIC, TimestampMixin
from app.models.enums import BacktestStatus, pg_enum


class BacktestRun(AppModel, TimestampMixin):
    __tablename__ = "backtest_runs"

    strategy_id: Mapped[int] = mapped_column(ForeignKey("strategies.id"), nullable=False, index=True)
    status: Mapped[BacktestStatus] = mapped_column(
        pg_enum(BacktestStatus, "backtest_status_enum"),
        nullable=False,
        default=BacktestStatus.QUEUED,
    )
    params_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    error_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class BacktestResult(AppModel, CreatedAtMixin):
    __tablename__ = "backtest_results"

    backtest_run_id: Mapped[int] = mapped_column(
        ForeignKey("backtest_runs.id"),
        nullable=False,
        unique=True,
        index=True,
    )
    total_return_pct: Mapped[float] = mapped_column(PERCENT_NUMERIC, nullable=False, default=0)
    max_drawdown_pct: Mapped[float] = mapped_column(PERCENT_NUMERIC, nullable=False, default=0)
    win_rate_pct: Mapped[float] = mapped_column(PERCENT_NUMERIC, nullable=False, default=0)
    profit_factor: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False, default=0)
    expectancy: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False, default=0)
    total_trades: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    avg_winner: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False, default=0)
    avg_loser: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False, default=0)
    equity_curve_json: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, default=list, nullable=False)
    summary_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
