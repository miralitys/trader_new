from __future__ import annotations

from app.core.config import Settings


def test_settings_normalize_render_postgres_url() -> None:
    settings = Settings(DATABASE_URL="postgresql://user:pass@host:5432/dbname")

    assert settings.database_url == "postgresql+psycopg://user:pass@host:5432/dbname"


def test_settings_normalize_legacy_postgres_url() -> None:
    settings = Settings(DATABASE_URL="postgres://user:pass@host:5432/dbname")

    assert settings.database_url == "postgresql+psycopg://user:pass@host:5432/dbname"


def test_settings_include_funding_basis_defaults() -> None:
    settings = Settings()

    assert settings.binance_spot_api_base_url == "https://api.binance.com"
    assert settings.binance_futures_api_base_url == "https://fapi.binance.com"
    assert settings.okx_api_base_url == "https://www.okx.com"
    assert settings.funding_basis_archive_base_url == "https://data.binance.vision/data"
    assert settings.funding_basis_archive_fallback_enabled is True
    assert settings.funding_basis_incremental_overlap_bars == 3
    assert settings.default_symbol_list == [
        "BTC-USDT",
        "ETH-USDT",
        "SOL-USDT",
        "BNB-USDT",
        "ADA-USDT",
        "ALPINE-USDT",
        "XRP-USDT",
        "1INCH-USDT",
    ]
