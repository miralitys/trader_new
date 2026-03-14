from __future__ import annotations

from sqlalchemy import Boolean, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import AppModel, PRICE_NUMERIC, TimestampMixin


class PaperAccount(AppModel, TimestampMixin):
    __tablename__ = "paper_accounts"

    strategy_id: Mapped[int] = mapped_column(ForeignKey("strategies.id"), nullable=False, unique=True, index=True)
    balance: Mapped[float] = mapped_column(PRICE_NUMERIC, nullable=False, default=10000)
    currency: Mapped[str] = mapped_column(String(16), nullable=False, default="USD")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
