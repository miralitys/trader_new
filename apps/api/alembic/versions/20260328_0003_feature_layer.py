"""feature layer schema

Revision ID: 20260328_0003
Revises: 20260315_0002
Create Date: 2026-03-28 00:00:03
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260328_0003"
down_revision = "20260315_0002"
branch_labels = None
depends_on = None


sync_job_status_enum = postgresql.ENUM(
    "queued",
    "running",
    "completed",
    "failed",
    name="sync_job_status_enum",
    create_type=False,
)


def upgrade() -> None:
    op.create_table(
        "market_features",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("exchange_id", sa.Integer(), sa.ForeignKey("exchanges.id"), nullable=False),
        sa.Column("symbol_id", sa.Integer(), sa.ForeignKey("symbols.id"), nullable=False),
        sa.Column("timeframe", sa.String(length=16), sa.ForeignKey("timeframes.code"), nullable=False),
        sa.Column("open_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ret_1", sa.Numeric(18, 8), nullable=True),
        sa.Column("ret_3", sa.Numeric(18, 8), nullable=True),
        sa.Column("ret_12", sa.Numeric(18, 8), nullable=True),
        sa.Column("ret_48", sa.Numeric(18, 8), nullable=True),
        sa.Column("range_pct", sa.Numeric(18, 8), nullable=True),
        sa.Column("atr_pct", sa.Numeric(18, 8), nullable=True),
        sa.Column("realized_vol_20", sa.Numeric(18, 8), nullable=True),
        sa.Column("body_pct", sa.Numeric(18, 8), nullable=True),
        sa.Column("upper_wick_pct", sa.Numeric(18, 8), nullable=True),
        sa.Column("lower_wick_pct", sa.Numeric(18, 8), nullable=True),
        sa.Column("distance_to_high_20_pct", sa.Numeric(18, 8), nullable=True),
        sa.Column("distance_to_low_20_pct", sa.Numeric(18, 8), nullable=True),
        sa.Column("ema20_dist_pct", sa.Numeric(18, 8), nullable=True),
        sa.Column("ema50_dist_pct", sa.Numeric(18, 8), nullable=True),
        sa.Column("ema200_dist_pct", sa.Numeric(18, 8), nullable=True),
        sa.Column("ema20_slope_pct", sa.Numeric(18, 8), nullable=True),
        sa.Column("ema50_slope_pct", sa.Numeric(18, 8), nullable=True),
        sa.Column("ema200_slope_pct", sa.Numeric(18, 8), nullable=True),
        sa.Column("relative_volume_20", sa.Numeric(18, 8), nullable=True),
        sa.Column("volume_zscore_20", sa.Numeric(18, 8), nullable=True),
        sa.Column("compression_ratio_12", sa.Numeric(18, 8), nullable=True),
        sa.Column("expansion_ratio_12", sa.Numeric(18, 8), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint(
            "exchange_id",
            "symbol_id",
            "timeframe",
            "open_time",
            name="uq_market_features_exchange_symbol_timeframe_open_time",
        ),
    )
    op.create_index("ix_market_features_symbol_timeframe_open_time", "market_features", ["symbol_id", "timeframe", "open_time"])

    op.create_table(
        "feature_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("exchange", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("timeframe", sa.Text(), nullable=False),
        sa.Column("lookback_days", sa.Integer(), nullable=False, server_default="730"),
        sa.Column("start_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("end_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sync_job_status_enum, nullable=False, server_default="queued"),
        sa.Column("source_candle_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("feature_rows_upserted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("computed_start_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("computed_end_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_feature_runs_symbol_timeframe_updated_at", "feature_runs", ["symbol", "timeframe", "updated_at"])


def downgrade() -> None:
    op.drop_index("ix_feature_runs_symbol_timeframe_updated_at", table_name="feature_runs")
    op.drop_table("feature_runs")

    op.drop_index("ix_market_features_symbol_timeframe_open_time", table_name="market_features")
    op.drop_table("market_features")
