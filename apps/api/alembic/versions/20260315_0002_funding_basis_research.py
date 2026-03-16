"""funding basis research schema

Revision ID: 20260315_0002
Revises: 20260314_0001
Create Date: 2026-03-15 00:00:02
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260315_0002"
down_revision = "20260314_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "spot_prices",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("exchange", sa.String(length=64), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("bid", sa.Numeric(28, 10), nullable=True),
        sa.Column("ask", sa.Numeric(28, 10), nullable=True),
        sa.Column("mid", sa.Numeric(28, 10), nullable=False),
        sa.Column("close", sa.Numeric(28, 10), nullable=False),
        sa.Column("volume", sa.Numeric(28, 10), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("exchange", "symbol", "ts", name="uq_spot_prices_exchange_symbol_ts"),
    )
    op.create_index("ix_spot_prices_symbol_ts", "spot_prices", ["symbol", "ts"])
    op.create_index("ix_spot_prices_exchange_symbol_ts", "spot_prices", ["exchange", "symbol", "ts"])

    op.create_table(
        "perp_prices",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("exchange", sa.String(length=64), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("mark_price", sa.Numeric(28, 10), nullable=False),
        sa.Column("index_price", sa.Numeric(28, 10), nullable=False),
        sa.Column("bid", sa.Numeric(28, 10), nullable=True),
        sa.Column("ask", sa.Numeric(28, 10), nullable=True),
        sa.Column("mid", sa.Numeric(28, 10), nullable=False),
        sa.Column("open_interest", sa.Numeric(28, 10), nullable=True),
        sa.Column("volume", sa.Numeric(28, 10), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("exchange", "symbol", "ts", name="uq_perp_prices_exchange_symbol_ts"),
    )
    op.create_index("ix_perp_prices_symbol_ts", "perp_prices", ["symbol", "ts"])
    op.create_index("ix_perp_prices_exchange_symbol_ts", "perp_prices", ["exchange", "symbol", "ts"])

    op.create_table(
        "funding_rates",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("exchange", sa.String(length=64), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("funding_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("funding_rate", sa.Numeric(12, 6), nullable=False),
        sa.Column("realized_funding_rate", sa.Numeric(12, 6), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("exchange", "symbol", "funding_time", name="uq_funding_rates_exchange_symbol_time"),
    )
    op.create_index("ix_funding_rates_symbol_time", "funding_rates", ["symbol", "funding_time"])
    op.create_index("ix_funding_rates_exchange_symbol_time", "funding_rates", ["exchange", "symbol", "funding_time"])

    op.create_table(
        "fee_schedules",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("venue", sa.String(length=64), nullable=False),
        sa.Column("product_type", sa.String(length=16), nullable=False),
        sa.Column("maker_fee_pct", sa.Numeric(12, 6), nullable=False),
        sa.Column("taker_fee_pct", sa.Numeric(12, 6), nullable=False),
        sa.Column("effective_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("venue", "product_type", "effective_from", name="uq_fee_schedules_venue_product_effective"),
    )
    op.create_index(
        "ix_fee_schedules_venue_product_effective",
        "fee_schedules",
        ["venue", "product_type", "effective_from"],
    )


def downgrade() -> None:
    op.drop_index("ix_fee_schedules_venue_product_effective", table_name="fee_schedules")
    op.drop_table("fee_schedules")

    op.drop_index("ix_funding_rates_exchange_symbol_time", table_name="funding_rates")
    op.drop_index("ix_funding_rates_symbol_time", table_name="funding_rates")
    op.drop_table("funding_rates")

    op.drop_index("ix_perp_prices_exchange_symbol_ts", table_name="perp_prices")
    op.drop_index("ix_perp_prices_symbol_ts", table_name="perp_prices")
    op.drop_table("perp_prices")

    op.drop_index("ix_spot_prices_exchange_symbol_ts", table_name="spot_prices")
    op.drop_index("ix_spot_prices_symbol_ts", table_name="spot_prices")
    op.drop_table("spot_prices")
