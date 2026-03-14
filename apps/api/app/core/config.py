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
    coinbase_api_base_url: str = Field(
        default="https://api.exchange.coinbase.com",
        alias="COINBASE_API_BASE_URL",
    )
    coinbase_timeout_seconds: int = Field(default=20, alias="COINBASE_TIMEOUT_SECONDS")
    coinbase_max_candles_per_request: int = Field(default=300, alias="COINBASE_MAX_CANDLES_PER_REQUEST")
    coinbase_retry_attempts: int = Field(default=5, alias="COINBASE_RETRY_ATTEMPTS")
    coinbase_backoff_min_seconds: float = Field(default=1.0, alias="COINBASE_BACKOFF_MIN_SECONDS")
    coinbase_backoff_max_seconds: float = Field(default=8.0, alias="COINBASE_BACKOFF_MAX_SECONDS")
    market_data_incremental_overlap_candles: int = Field(
        default=3,
        alias="MARKET_DATA_INCREMENTAL_OVERLAP_CANDLES",
    )
    worker_poll_seconds: int = Field(default=15, alias="WORKER_POLL_SECONDS")
    worker_max_candles_per_stream: int = Field(default=100, alias="WORKER_MAX_CANDLES_PER_STREAM")
    default_symbols: str = Field(default="BTC-USD,ETH-USD,SOL-USD", alias="DEFAULT_SYMBOLS")
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
    def coinbase_base_url(self) -> str:
        return self.coinbase_api_base_url


@lru_cache
def get_settings() -> Settings:
    return Settings()
