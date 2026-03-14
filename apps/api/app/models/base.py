from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, Numeric, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


PRICE_NUMERIC = Numeric(28, 10)
QUANTITY_NUMERIC = Numeric(28, 10)
PERCENT_NUMERIC = Numeric(12, 6)
SIGNAL_STRENGTH_NUMERIC = Numeric(12, 6)


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        server_default=func.now(),
        onupdate=utc_now,
        nullable=False,
    )


class CreatedAtMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        server_default=func.now(),
        nullable=False,
    )


class IdentifierMixin:
    id: Mapped[int] = mapped_column(Integer, primary_key=True)


class AppModel(Base, IdentifierMixin):
    __abstract__ = True
