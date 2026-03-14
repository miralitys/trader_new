from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

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
    default_symbols: str = Field(default="BTC-USD,ETH-USD,SOL-USD", alias="DEFAULT_SYMBOLS")
    default_timeframes: str = Field(default="5m,15m,1h", alias="DEFAULT_TIMEFRAMES")

    @property
    def default_symbol_list(self) -> list[str]:
        return [item.strip() for item in self.default_symbols.split(",") if item.strip()]

    @property
    def default_timeframe_list(self) -> list[str]:
        return [item.strip() for item in self.default_timeframes.split(",") if item.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
