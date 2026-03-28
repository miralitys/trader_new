from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, Index, Numeric, UniqueConstraint
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


FEATURE_NUMERIC = Numeric(18, 8)


class MarketFeature(AppModel, CreatedAtMixin):
    __tablename__ = "market_features"
    __table_args__ = (
        UniqueConstraint(
            "exchange_id",
            "symbol_id",
            "timeframe",
            "open_time",
            name="uq_market_features_exchange_symbol_timeframe_open_time",
        ),
        Index("ix_market_features_symbol_timeframe_open_time", "symbol_id", "timeframe", "open_time"),
    )

    exchange_id: Mapped[int] = mapped_column(ForeignKey("exchanges.id"), nullable=False, index=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey("symbols.id"), nullable=False, index=True)
    timeframe: Mapped[str] = mapped_column(ForeignKey("timeframes.code"), nullable=False)
    open_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    ret_1: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)
    ret_3: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)
    ret_12: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)
    ret_48: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)

    range_pct: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)
    atr_pct: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)
    realized_vol_20: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)

    body_pct: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)
    upper_wick_pct: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)
    lower_wick_pct: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)
    distance_to_high_20_pct: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)
    distance_to_low_20_pct: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)

    ema20_dist_pct: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)
    ema50_dist_pct: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)
    ema200_dist_pct: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)
    ema20_slope_pct: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)
    ema50_slope_pct: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)
    ema200_slope_pct: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)

    relative_volume_20: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)
    volume_zscore_20: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)

    compression_ratio_12: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)
    expansion_ratio_12: Mapped[Optional[float]] = mapped_column(FEATURE_NUMERIC, nullable=True)
