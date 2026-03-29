"""Add pattern scan background runs."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260329_0006"
down_revision = "20260328_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "pattern_scan_runs",
        sa.Column("exchange", sa.Text(), nullable=False),
        sa.Column("symbols_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("timeframes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("lookback_days", sa.Integer(), nullable=False, server_default="730"),
        sa.Column("forward_bars", sa.Integer(), nullable=False, server_default="12"),
        sa.Column("fee_pct", sa.Text(), nullable=False, server_default="0.001"),
        sa.Column("slippage_pct", sa.Text(), nullable=False, server_default="0.0005"),
        sa.Column("max_bars_per_series", sa.Integer(), nullable=False, server_default="5000"),
        sa.Column("status", postgresql.ENUM("queued", "running", "completed", "failed", name="sync_job_status_enum", create_type=False), nullable=False, server_default="queued"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("progress_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("report_summary_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("report_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_pattern_scan_runs_updated_at", "pattern_scan_runs", ["updated_at"])


def downgrade() -> None:
    op.drop_index("ix_pattern_scan_runs_updated_at", table_name="pattern_scan_runs")
    op.drop_table("pattern_scan_runs")
