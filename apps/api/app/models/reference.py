from __future__ import annotations

from typing import Optional

from sqlalchemy import Boolean, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import AppModel, TimestampMixin


class Exchange(AppModel, TimestampMixin):
    __tablename__ = "exchanges"

    code: Mapped[str] = mapped_column(String(32), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    symbols: Mapped[list["Symbol"]] = relationship(back_populates="exchange")


class Symbol(AppModel, TimestampMixin):
    __tablename__ = "symbols"
    __table_args__ = (UniqueConstraint("exchange_id", "code", name="uq_symbols_exchange_code"),)

    exchange_id: Mapped[int] = mapped_column(ForeignKey("exchanges.id"), nullable=False, index=True)
    code: Mapped[str] = mapped_column(String(32), nullable=False)
    base_asset: Mapped[str] = mapped_column(String(16), nullable=False)
    quote_asset: Mapped[str] = mapped_column(String(16), nullable=False)
    price_precision: Mapped[int] = mapped_column(Integer, default=2, nullable=False)
    qty_precision: Mapped[int] = mapped_column(Integer, default=8, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    exchange: Mapped["Exchange"] = relationship(back_populates="symbols")


class Timeframe(AppModel, TimestampMixin):
    __tablename__ = "timeframes"

    code: Mapped[str] = mapped_column(String(16), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(64), nullable=False)
    duration_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class Strategy(AppModel, TimestampMixin):
    __tablename__ = "strategies"

    code: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
