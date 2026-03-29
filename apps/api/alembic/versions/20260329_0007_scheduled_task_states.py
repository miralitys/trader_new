"""add scheduled task states

Revision ID: 20260329_0007
Revises: 20260329_0006
Create Date: 2026-03-29 02:40:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260329_0007"
down_revision = "20260329_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "scheduled_task_states",
        sa.Column("task_key", sa.Text(), nullable=False),
        sa.Column("timezone_name", sa.Text(), nullable=False),
        sa.Column("schedule_local_hour", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("schedule_local_minute", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("lookback_days", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("last_scheduled_date", sa.Text(), nullable=True),
        sa.Column("last_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_status", sa.Text(), nullable=True),
        sa.Column("last_error_text", sa.Text(), nullable=True),
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("task_key", name="uq_scheduled_task_states_task_key"),
    )
    op.create_index("ix_scheduled_task_states_task_key", "scheduled_task_states", ["task_key"], unique=True)


def downgrade() -> None:
    op.drop_index("ix_scheduled_task_states_task_key", table_name="scheduled_task_states")
    op.drop_table("scheduled_task_states")
