from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Type

from pydantic import BaseModel, Field, field_validator

from app.schemas.strategy import StrategyDescriptor
from app.utils.symbols import compact_supported_symbols


class BaseStrategyConfig(BaseModel):
    enabled: bool = True
    symbols: list[str] = Field(default_factory=list)
    timeframes: list[str] = Field(default_factory=list)
    position_size_pct: float = 0.1
    stop_loss_pct: float = 0.03
    take_profit_pct: float = 0.06

    @field_validator("symbols")
    @classmethod
    def validate_symbols(cls, value: list[str]) -> list[str]:
        return compact_supported_symbols(value)


class StrategyContext(BaseModel):
    symbol: str
    timeframe: str
    timestamp: datetime | None = None
    mode: str = "paper"
    metadata: dict[str, Any] = Field(default_factory=dict)


class StrategySignal(BaseModel):
    action: str = "hold"
    side: str = "long"
    reason: str = "strategy_placeholder"
    confidence: float = 0.0
    metadata: dict[str, Any] = Field(default_factory=dict)


class BaseStrategy:
    key = "base_strategy"
    name = "BaseStrategy"
    description = "Base strategy interface placeholder."
    spot_only = True
    long_only = True
    status = "scaffold"
    debug_counter_keys: tuple[str, ...] = ()
    config_model: Type[BaseStrategyConfig] = BaseStrategyConfig

    def descriptor(self) -> StrategyDescriptor:
        return StrategyDescriptor(
            key=self.key,
            name=self.name,
            description=self.description,
            spot_only=self.spot_only,
            long_only=self.long_only,
            status=self.status,
        )

    def default_config(self) -> dict[str, Any]:
        return self.config_model().model_dump()

    def parse_config(
        self,
        config: BaseStrategyConfig | Mapping[str, Any] | None = None,
    ) -> BaseStrategyConfig:
        if config is None:
            return self.config_model()
        if isinstance(config, self.config_model):
            return config
        if isinstance(config, BaseStrategyConfig):
            return self.config_model(**config.model_dump())
        return self.config_model(**dict(config))

    def required_preroll_days(
        self,
        timeframe: str,
        strategy_config: BaseStrategyConfig | None = None,
    ) -> int:
        return 0

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        return StrategySignal(reason=f"{self.key}_signal_not_implemented")

    def risk_management(self, context: StrategyContext) -> dict[str, Any]:
        return {
            "strategy_key": self.key,
            "status": "placeholder",
            "long_only": self.long_only,
            "spot_only": self.spot_only,
        }

    def simulate_execution(self, context: StrategyContext) -> dict[str, Any]:
        return {
            "strategy_key": self.key,
            "status": "placeholder",
            "mode": context.mode,
        }
