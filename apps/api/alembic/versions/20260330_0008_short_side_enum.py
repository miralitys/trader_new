"""Add short support to side enum.

Revision ID: 20260330_0008
Revises: 20260329_0007
Create Date: 2026-03-30 11:15:00
"""

from __future__ import annotations

from alembic import op


revision = "20260330_0008"
down_revision = "20260329_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TYPE side_enum ADD VALUE IF NOT EXISTS 'short'")


def downgrade() -> None:
    # PostgreSQL enum value removal is intentionally left as a no-op.
    pass
