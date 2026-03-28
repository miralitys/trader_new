"""validation runs background schema

Revision ID: 20260328_0004
Revises: 20260328_0003
Create Date: 2026-03-28 00:00:04
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260328_0004"
down_revision = "20260328_0003"
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
        "validation_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("exchange", sa.Text(), nullable=False),
        sa.Column("symbols_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("timeframes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("lookback_days", sa.Integer(), nullable=False, server_default="730"),
        sa.Column("sample_limit", sa.Integer(), nullable=False, server_default="5"),
        sa.Column("perform_resync", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("resync_days", sa.Integer(), nullable=False, server_default="14"),
        sa.Column("status", sync_job_status_enum, nullable=False, server_default="queued"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("report_summary_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("report_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_validation_runs_updated_at", "validation_runs", ["updated_at"])


def downgrade() -> None:
    op.drop_index("ix_validation_runs_updated_at", table_name="validation_runs")
    op.drop_table("validation_runs")
