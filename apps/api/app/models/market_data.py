from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import AppModel, CreatedAtMixin, PRICE_NUMERIC, QUANTITY_NUMERIC


class Candle(AppModel, CreatedAtMixin):
    __tablename__ = "candles"
    __table_args__ = (
        UniqueConstraint(
            "exchange_id",
            "symbol_id",
            "timeframe",
            "open_time",
            name="uq_candles_exchange_symbol_timeframe_open_time",
        ),
        Index("ix_candles_symbol_timeframe_open_time", "symbol_id", "timeframe", "open_time"),
    )

    exchange_id: Mapped[int] = mapped_column(ForeignKey("exchanges.id"), nullable=False, index=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey("symbols.id"), nullable=False, index=True)
    timeframe: Mapped[str] = mapped_column(ForeignKey("timeframes.code"), nullable=False)
    open_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    open: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False)
    high: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False)
    low: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False)
    close: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False)
    volume: Mapped[float] = mapped_column(QUANTITY_NUMERIC, nullable=False)
