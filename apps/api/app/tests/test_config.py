from __future__ import annotations

from app.core.config import Settings


def test_settings_normalize_render_postgres_url() -> None:
    settings = Settings(DATABASE_URL="postgresql://user:pass@host:5432/dbname")

    assert settings.database_url == "postgresql+psycopg://user:pass@host:5432/dbname"


def test_settings_normalize_legacy_postgres_url() -> None:
    settings = Settings(DATABASE_URL="postgres://user:pass@host:5432/dbname")

    assert settings.database_url == "postgresql+psycopg://user:pass@host:5432/dbname"
