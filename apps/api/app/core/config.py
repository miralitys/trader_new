from __future__ import annotations

from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = Field(default="Trader Backend", alias="APP_NAME")
    app_version: str = Field(default="0.1.0", alias="APP_VERSION")
    api_v1_prefix: str = Field(default="/api", alias="API_V1_PREFIX")
    environment: str = Field(default="local", alias="ENVIRONMENT")
    debug: bool = Field(default=False, alias="DEBUG")
    database_url: str = Field(
        default="postgresql+psycopg://postgres:postgres@postgres:5432/trader",
        alias="DATABASE_URL",
    )
    redis_url: str = Field(default="redis://redis:6379/0", alias="REDIS_URL")
    secret_key: str = Field(default="replace-me", alias="SECRET_KEY")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    binance_us_api_base_url: str = Field(default="https://api.binance.us", alias="BINANCE_US_API_BASE_URL")
    binance_us_timeout_seconds: int = Field(default=20, alias="BINANCE_US_TIMEOUT_SECONDS")
    binance_us_max_candles_per_request: int = Field(default=1000, alias="BINANCE_US_MAX_CANDLES_PER_REQUEST")
    binance_us_retry_attempts: int = Field(default=5, alias="BINANCE_US_RETRY_ATTEMPTS")
    binance_us_backoff_min_seconds: float = Field(default=1.0, alias="BINANCE_US_BACKOFF_MIN_SECONDS")
    binance_us_backoff_max_seconds: float = Field(default=8.0, alias="BINANCE_US_BACKOFF_MAX_SECONDS")
    binance_spot_api_base_url: str = Field(default="https://api.binance.com", alias="BINANCE_SPOT_API_BASE_URL")
    binance_futures_api_base_url: str = Field(default="https://fapi.binance.com", alias="BINANCE_FUTURES_API_BASE_URL")
    binance_spot_max_rows_per_request: int = Field(default=1000, alias="BINANCE_SPOT_MAX_ROWS_PER_REQUEST")
    binance_futures_max_rows_per_request: int = Field(default=1000, alias="BINANCE_FUTURES_MAX_ROWS_PER_REQUEST")
    binance_futures_open_interest_max_rows_per_request: int = Field(
        default=500,
        alias="BINANCE_FUTURES_OPEN_INTEREST_MAX_ROWS_PER_REQUEST",
    )
    binance_futures_max_funding_rows_per_request: int = Field(
        default=1000,
        alias="BINANCE_FUTURES_MAX_FUNDING_ROWS_PER_REQUEST",
    )
    funding_basis_timeout_seconds: int = Field(default=20, alias="FUNDING_BASIS_TIMEOUT_SECONDS")
    funding_basis_retry_attempts: int = Field(default=5, alias="FUNDING_BASIS_RETRY_ATTEMPTS")
    funding_basis_backoff_min_seconds: float = Field(default=1.0, alias="FUNDING_BASIS_BACKOFF_MIN_SECONDS")
    funding_basis_backoff_max_seconds: float = Field(default=8.0, alias="FUNDING_BASIS_BACKOFF_MAX_SECONDS")
    okx_api_base_url: str = Field(default="https://www.okx.com", alias="OKX_API_BASE_URL")
    okx_max_rows_per_request: int = Field(default=100, alias="OKX_MAX_ROWS_PER_REQUEST")
    okx_max_funding_rows_per_request: int = Field(default=100, alias="OKX_MAX_FUNDING_ROWS_PER_REQUEST")
    funding_basis_archive_base_url: str = Field(
        default="https://data.binance.vision/data",
        alias="FUNDING_BASIS_ARCHIVE_BASE_URL",
    )
    funding_basis_archive_fallback_enabled: bool = Field(
        default=True,
        alias="FUNDING_BASIS_ARCHIVE_FALLBACK_ENABLED",
    )
    funding_basis_archive_prefer_archive: bool = Field(
        default=False,
        alias="FUNDING_BASIS_ARCHIVE_PREFER_ARCHIVE",
    )
    funding_basis_incremental_overlap_bars: int = Field(
        default=3,
        alias="FUNDING_BASIS_INCREMENTAL_OVERLAP_BARS",
    )
    funding_basis_incremental_funding_overlap_intervals: int = Field(
        default=1,
        alias="FUNDING_BASIS_INCREMENTAL_FUNDING_OVERLAP_INTERVALS",
    )
    funding_basis_default_backfill_days: int = Field(default=30, alias="FUNDING_BASIS_DEFAULT_BACKFILL_DAYS")
    market_data_incremental_overlap_candles: int = Field(
        default=3,
        alias="MARKET_DATA_INCREMENTAL_OVERLAP_CANDLES",
    )
    worker_poll_seconds: int = Field(default=15, alias="WORKER_POLL_SECONDS")
    worker_max_candles_per_stream: int = Field(default=100, alias="WORKER_MAX_CANDLES_PER_STREAM")
    backtest_stale_after_seconds: int = Field(default=14400, alias="BACKTEST_STALE_AFTER_SECONDS")
    backtest_progress_interval_bars: int = Field(default=500, alias="BACKTEST_PROGRESS_INTERVAL_BARS")
    backtest_stop_check_interval_bars: int = Field(default=100, alias="BACKTEST_STOP_CHECK_INTERVAL_BARS")
    default_symbols: str = Field(
        default="BTC-USDT,ETH-USDT,SOL-USDT,BNB-USDT,ADA-USDT,ALPINE-USDT,XRP-USDT,1INCH-USDT",
        alias="DEFAULT_SYMBOLS",
    )
    default_timeframes: str = Field(default="5m,15m,1h", alias="DEFAULT_TIMEFRAMES")
    allowed_origins: list[str] = Field(default=["*"], alias="ALLOWED_ORIGINS")

    @field_validator("database_url", mode="before")
    @classmethod
    def normalize_database_url(cls, value: str) -> str:
        if value.startswith("postgresql://"):
            return value.replace("postgresql://", "postgresql+psycopg://", 1)
        if value.startswith("postgres://"):
            return value.replace("postgres://", "postgresql+psycopg://", 1)
        return value

    @property
    def default_symbol_list(self) -> list[str]:
        return [item.strip() for item in self.default_symbols.split(",") if item.strip()]

    @property
    def default_timeframe_list(self) -> list[str]:
        return [item.strip() for item in self.default_timeframes.split(",") if item.strip()]

    @property
    def binance_us_base_url(self) -> str:
        return self.binance_us_api_base_url


@lru_cache
def get_settings() -> Settings:
    return Settings()
