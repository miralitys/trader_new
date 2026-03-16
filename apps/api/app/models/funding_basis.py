from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import AppModel, CreatedAtMixin, PERCENT_NUMERIC, PRICE_NUMERIC, QUANTITY_NUMERIC


class SpotPrice(AppModel, CreatedAtMixin):
    __tablename__ = "spot_prices"
    __table_args__ = (
        UniqueConstraint("exchange", "symbol", "ts", name="uq_spot_prices_exchange_symbol_ts"),
        Index("ix_spot_prices_symbol_ts", "symbol", "ts"),
        Index("ix_spot_prices_exchange_symbol_ts", "exchange", "symbol", "ts"),
    )

    exchange: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    bid: Mapped[Optional[float]] = mapped_column(PRICE_NUMERIC, nullable=True)
    ask: Mapped[Optional[float]] = mapped_column(PRICE_NUMERIC, nullable=True)
    mid: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False)
    close: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False)
    volume: Mapped[float] = mapped_column(QUANTITY_NUMERIC, nullable=False, default=0)


class PerpPrice(AppModel, CreatedAtMixin):
    __tablename__ = "perp_prices"
    __table_args__ = (
        UniqueConstraint("exchange", "symbol", "ts", name="uq_perp_prices_exchange_symbol_ts"),
        Index("ix_perp_prices_symbol_ts", "symbol", "ts"),
        Index("ix_perp_prices_exchange_symbol_ts", "exchange", "symbol", "ts"),
    )

    exchange: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    mark_price: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False)
    index_price: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False)
    bid: Mapped[Optional[float]] = mapped_column(PRICE_NUMERIC, nullable=True)
    ask: Mapped[Optional[float]] = mapped_column(PRICE_NUMERIC, nullable=True)
    mid: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False)
    open_interest: Mapped[Optional[float]] = mapped_column(QUANTITY_NUMERIC, nullable=True)
    volume: Mapped[Optional[float]] = mapped_column(QUANTITY_NUMERIC, nullable=True)


class FundingRate(AppModel, CreatedAtMixin):
    __tablename__ = "funding_rates"
    __table_args__ = (
        UniqueConstraint("exchange", "symbol", "funding_time", name="uq_funding_rates_exchange_symbol_time"),
        Index("ix_funding_rates_symbol_time", "symbol", "funding_time"),
        Index("ix_funding_rates_exchange_symbol_time", "exchange", "symbol", "funding_time"),
    )

    exchange: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    funding_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    funding_rate: Mapped[float] = mapped_column(PERCENT_NUMERIC, nullable=False)
    realized_funding_rate: Mapped[Optional[float]] = mapped_column(PERCENT_NUMERIC, nullable=True)


class FeeSchedule(AppModel, CreatedAtMixin):
    __tablename__ = "fee_schedules"
    __table_args__ = (
        UniqueConstraint("venue", "product_type", "effective_from", name="uq_fee_schedules_venue_product_effective"),
        Index("ix_fee_schedules_venue_product_effective", "venue", "product_type", "effective_from"),
    )

    venue: Mapped[str] = mapped_column(String(64), nullable=False)
    product_type: Mapped[str] = mapped_column(String(16), nullable=False)
    maker_fee_pct: Mapped[float] = mapped_column(PERCENT_NUMERIC, nullable=False)
    taker_fee_pct: Mapped[float] = mapped_column(PERCENT_NUMERIC, nullable=False)
    effective_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
