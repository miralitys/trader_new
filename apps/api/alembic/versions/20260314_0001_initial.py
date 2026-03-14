"""initial schema

Revision ID: 20260314_0001
Revises:
Create Date: 2026-03-14 00:00:01
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260314_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()

    enum_types = [
        postgresql.ENUM(
            "paper",
            "backtest",
            "live_prep",
            name="strategy_run_mode_enum",
            create_type=False,
        ),
        postgresql.ENUM(
            "created",
            "running",
            "stopped",
            "failed",
            "completed",
            name="strategy_run_status_enum",
            create_type=False,
        ),
        postgresql.ENUM("enter", "exit", "hold", name="signal_type_enum", create_type=False),
        postgresql.ENUM("long", name="side_enum", create_type=False),
        postgresql.ENUM("market", "limit", "simulated", name="order_type_enum", create_type=False),
        postgresql.ENUM(
            "new",
            "open",
            "filled",
            "cancelled",
            "rejected",
            name="order_status_enum",
            create_type=False,
        ),
        postgresql.ENUM("open", "closed", name="position_status_enum", create_type=False),
        postgresql.ENUM(
            "queued",
            "running",
            "completed",
            "failed",
            name="backtest_status_enum",
            create_type=False,
        ),
        postgresql.ENUM(
            "queued",
            "running",
            "completed",
            "failed",
            name="sync_job_status_enum",
            create_type=False,
        ),
        postgresql.ENUM(
            "debug",
            "info",
            "warning",
            "error",
            name="app_log_level_enum",
            create_type=False,
        ),
    ]

    for enum_type in enum_types:
        enum_type.create(bind, checkfirst=True)

    op.create_table(
        "exchanges",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("code", sa.String(length=32), nullable=False, unique=True),
        sa.Column("name", sa.String(length=128), nullable=False, unique=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "timeframes",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("code", sa.String(length=16), nullable=False, unique=True),
        sa.Column("name", sa.String(length=64), nullable=False),
        sa.Column("duration_seconds", sa.Integer(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "strategies",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("code", sa.String(length=64), nullable=False, unique=True),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "symbols",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("exchange_id", sa.Integer(), sa.ForeignKey("exchanges.id"), nullable=False),
        sa.Column("code", sa.String(length=32), nullable=False),
        sa.Column("base_asset", sa.String(length=16), nullable=False),
        sa.Column("quote_asset", sa.String(length=16), nullable=False),
        sa.Column("price_precision", sa.Integer(), nullable=False, server_default="2"),
        sa.Column("qty_precision", sa.Integer(), nullable=False, server_default="8"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("exchange_id", "code", name="uq_symbols_exchange_code"),
    )
    op.create_index("ix_symbols_exchange_id", "symbols", ["exchange_id"])

    op.create_table(
        "candles",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("exchange_id", sa.Integer(), sa.ForeignKey("exchanges.id"), nullable=False),
        sa.Column("symbol_id", sa.Integer(), sa.ForeignKey("symbols.id"), nullable=False),
        sa.Column("timeframe", sa.String(length=16), sa.ForeignKey("timeframes.code"), nullable=False),
        sa.Column("open_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open", sa.Numeric(28, 10), nullable=False),
        sa.Column("high", sa.Numeric(28, 10), nullable=False),
        sa.Column("low", sa.Numeric(28, 10), nullable=False),
        sa.Column("close", sa.Numeric(28, 10), nullable=False),
        sa.Column("volume", sa.Numeric(28, 10), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint(
            "exchange_id",
            "symbol_id",
            "timeframe",
            "open_time",
            name="uq_candles_exchange_symbol_timeframe_open_time",
        ),
    )
    op.create_index("ix_candles_exchange_id", "candles", ["exchange_id"])
    op.create_index("ix_candles_symbol_id", "candles", ["symbol_id"])
    op.create_index("ix_candles_symbol_timeframe_open_time", "candles", ["symbol_id", "timeframe", "open_time"])

    op.create_table(
        "strategy_configs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("strategy_id", sa.Integer(), sa.ForeignKey("strategies.id"), nullable=False),
        sa.Column(
            "config_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_strategy_configs_strategy_id", "strategy_configs", ["strategy_id"])

    op.create_table(
        "strategy_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("strategy_id", sa.Integer(), sa.ForeignKey("strategies.id"), nullable=False),
        sa.Column("mode", enum_types[0], nullable=False, server_default="paper"),
        sa.Column("status", enum_types[1], nullable=False, server_default="created"),
        sa.Column(
            "symbols_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "timeframes_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("stopped_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_processed_candle_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "metadata_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_strategy_runs_strategy_id", "strategy_runs", ["strategy_id"])

    op.create_table(
        "signals",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("strategy_run_id", sa.Integer(), sa.ForeignKey("strategy_runs.id"), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("timeframe", sa.Text(), nullable=False),
        sa.Column("signal_type", enum_types[2], nullable=False, server_default="hold"),
        sa.Column("signal_strength", sa.Numeric(12, 6), nullable=False, server_default="0"),
        sa.Column(
            "payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("candle_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_signals_strategy_run_id", "signals", ["strategy_run_id"])

    op.create_table(
        "orders",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("strategy_run_id", sa.Integer(), sa.ForeignKey("strategy_runs.id"), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("side", enum_types[3], nullable=False, server_default="long"),
        sa.Column("order_type", enum_types[4], nullable=False, server_default="simulated"),
        sa.Column("qty", sa.Numeric(28, 10), nullable=False),
        sa.Column("price", sa.Numeric(28, 10), nullable=False),
        sa.Column("status", enum_types[5], nullable=False, server_default="new"),
        sa.Column("linked_signal_id", sa.Integer(), sa.ForeignKey("signals.id"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_orders_strategy_run_id", "orders", ["strategy_run_id"])
    op.create_index("ix_orders_linked_signal_id", "orders", ["linked_signal_id"])

    op.create_table(
        "positions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("strategy_run_id", sa.Integer(), sa.ForeignKey("strategy_runs.id"), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("side", enum_types[3], nullable=False, server_default="long"),
        sa.Column("qty", sa.Numeric(28, 10), nullable=False),
        sa.Column("avg_entry_price", sa.Numeric(28, 10), nullable=False),
        sa.Column("stop_price", sa.Numeric(28, 10), nullable=True),
        sa.Column("take_profit_price", sa.Numeric(28, 10), nullable=True),
        sa.Column("status", enum_types[6], nullable=False, server_default="open"),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_positions_strategy_run_id", "positions", ["strategy_run_id"])

    op.create_table(
        "trades",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("strategy_run_id", sa.Integer(), sa.ForeignKey("strategy_runs.id"), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("entry_price", sa.Numeric(28, 10), nullable=False),
        sa.Column("exit_price", sa.Numeric(28, 10), nullable=False),
        sa.Column("qty", sa.Numeric(28, 10), nullable=False),
        sa.Column("pnl", sa.Numeric(28, 10), nullable=False),
        sa.Column("pnl_pct", sa.Numeric(12, 6), nullable=False),
        sa.Column("fees", sa.Numeric(28, 10), nullable=False, server_default="0"),
        sa.Column("slippage", sa.Numeric(28, 10), nullable=False, server_default="0"),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "metadata_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.create_index("ix_trades_strategy_run_id", "trades", ["strategy_run_id"])

    op.create_table(
        "backtest_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("strategy_id", sa.Integer(), sa.ForeignKey("strategies.id"), nullable=False),
        sa.Column("status", enum_types[7], nullable=False, server_default="queued"),
        sa.Column(
            "params_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_backtest_runs_strategy_id", "backtest_runs", ["strategy_id"])

    op.create_table(
        "backtest_results",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("backtest_run_id", sa.Integer(), sa.ForeignKey("backtest_runs.id"), nullable=False, unique=True),
        sa.Column("total_return_pct", sa.Numeric(12, 6), nullable=False, server_default="0"),
        sa.Column("max_drawdown_pct", sa.Numeric(12, 6), nullable=False, server_default="0"),
        sa.Column("win_rate_pct", sa.Numeric(12, 6), nullable=False, server_default="0"),
        sa.Column("profit_factor", sa.Numeric(28, 10), nullable=False, server_default="0"),
        sa.Column("expectancy", sa.Numeric(28, 10), nullable=False, server_default="0"),
        sa.Column("total_trades", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("avg_winner", sa.Numeric(28, 10), nullable=False, server_default="0"),
        sa.Column("avg_loser", sa.Numeric(28, 10), nullable=False, server_default="0"),
        sa.Column(
            "equity_curve_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "summary_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_backtest_results_backtest_run_id", "backtest_results", ["backtest_run_id"], unique=True)

    op.create_table(
        "sync_jobs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("exchange", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("timeframe", sa.Text(), nullable=False),
        sa.Column("start_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("end_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", enum_types[8], nullable=False, server_default="queued"),
        sa.Column("rows_inserted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "app_logs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("scope", sa.Text(), nullable=False),
        sa.Column("level", enum_types[9], nullable=False, server_default="info"),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column(
            "payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "paper_accounts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("strategy_id", sa.Integer(), sa.ForeignKey("strategies.id"), nullable=False, unique=True),
        sa.Column("balance", sa.Numeric(28, 10), nullable=False, server_default="10000"),
        sa.Column("currency", sa.String(length=16), nullable=False, server_default="USD"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_paper_accounts_strategy_id", "paper_accounts", ["strategy_id"], unique=True)


def downgrade() -> None:
    bind = op.get_bind()

    op.drop_index("ix_paper_accounts_strategy_id", table_name="paper_accounts")
    op.drop_table("paper_accounts")
    op.drop_table("app_logs")
    op.drop_table("sync_jobs")
    op.drop_index("ix_backtest_results_backtest_run_id", table_name="backtest_results")
    op.drop_table("backtest_results")
    op.drop_index("ix_backtest_runs_strategy_id", table_name="backtest_runs")
    op.drop_table("backtest_runs")
    op.drop_index("ix_trades_strategy_run_id", table_name="trades")
    op.drop_table("trades")
    op.drop_index("ix_positions_strategy_run_id", table_name="positions")
    op.drop_table("positions")
    op.drop_index("ix_orders_linked_signal_id", table_name="orders")
    op.drop_index("ix_orders_strategy_run_id", table_name="orders")
    op.drop_table("orders")
    op.drop_index("ix_signals_strategy_run_id", table_name="signals")
    op.drop_table("signals")
    op.drop_index("ix_strategy_runs_strategy_id", table_name="strategy_runs")
    op.drop_table("strategy_runs")
    op.drop_index("ix_strategy_configs_strategy_id", table_name="strategy_configs")
    op.drop_table("strategy_configs")
    op.drop_index("ix_candles_symbol_timeframe_open_time", table_name="candles")
    op.drop_index("ix_candles_symbol_id", table_name="candles")
    op.drop_index("ix_candles_exchange_id", table_name="candles")
    op.drop_table("candles")
    op.drop_index("ix_symbols_exchange_id", table_name="symbols")
    op.drop_table("symbols")
    op.drop_table("strategies")
    op.drop_table("timeframes")
    op.drop_table("exchanges")

    for enum_type in reversed(
        [
            postgresql.ENUM(
                "paper",
                "backtest",
                "live_prep",
                name="strategy_run_mode_enum",
                create_type=False,
            ),
            postgresql.ENUM(
                "created",
                "running",
                "stopped",
                "failed",
                "completed",
                name="strategy_run_status_enum",
                create_type=False,
            ),
            postgresql.ENUM("enter", "exit", "hold", name="signal_type_enum", create_type=False),
            postgresql.ENUM("long", name="side_enum", create_type=False),
            postgresql.ENUM("market", "limit", "simulated", name="order_type_enum", create_type=False),
            postgresql.ENUM(
                "new",
                "open",
                "filled",
                "cancelled",
                "rejected",
                name="order_status_enum",
                create_type=False,
            ),
            postgresql.ENUM("open", "closed", name="position_status_enum", create_type=False),
            postgresql.ENUM(
                "queued",
                "running",
                "completed",
                "failed",
                name="backtest_status_enum",
                create_type=False,
            ),
            postgresql.ENUM(
                "queued",
                "running",
                "completed",
                "failed",
                name="sync_job_status_enum",
                create_type=False,
            ),
            postgresql.ENUM(
                "debug",
                "info",
                "warning",
                "error",
                name="app_log_level_enum",
                create_type=False,
            ),
        ]
    ):
        enum_type.drop(bind, checkfirst=True)
