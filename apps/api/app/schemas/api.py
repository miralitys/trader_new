from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal, Optional

from pydantic import Field, field_validator, model_validator

from app.integrations.binance_us import BinanceUSTimeframe
from app.schemas.common import APIModel
from app.utils.exchanges import normalize_exchange_code

SyncMode = Literal["initial", "incremental", "manual"]


class StrategyConfigUpdateRequest(APIModel):
    config: dict[str, Any] = Field(default_factory=dict)


class StrategyPaperStartRequest(APIModel):
    symbols: list[str]
    timeframes: list[str]
    exchange_code: str = "binance_us"
    initial_balance: Decimal = Field(default=Decimal("10000"), gt=0)
    currency: str = "USD"
    fee: Decimal = Field(default=Decimal("0.001"), ge=0)
    slippage: Decimal = Field(default=Decimal("0.0005"), ge=0)
    strategy_config_override: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("currency")
    @classmethod
    def validate_non_empty_string(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Value must not be empty")
        return normalized

    @field_validator("exchange_code")
    @classmethod
    def validate_exchange_code(cls, value: str) -> str:
        return normalize_exchange_code(value)

    @field_validator("symbols")
    @classmethod
    def validate_symbols(cls, value: list[str]) -> list[str]:
        normalized = [symbol.strip() for symbol in value if symbol.strip()]
        if not normalized:
            raise ValueError("At least one symbol is required")
        return normalized

    @field_validator("timeframes")
    @classmethod
    def validate_timeframes(cls, value: list[str]) -> list[str]:
        normalized = [timeframe.strip() for timeframe in value if timeframe.strip()]
        if not normalized:
            raise ValueError("At least one timeframe is required")
        for timeframe in normalized:
            BinanceUSTimeframe.from_code(timeframe)
        return normalized


class StrategyPaperStopRequest(APIModel):
    reason: str = "manual_stop"


class StrategySummaryResponse(APIModel):
    code: str
    name: str
    description: str
    spot_only: bool = True
    long_only: bool = True
    has_saved_config: bool = False
    active_paper_run_id: Optional[int] = None
    active_paper_status: Optional[str] = None


class StrategyDetailResponse(StrategySummaryResponse):
    default_config: dict[str, Any] = Field(default_factory=dict)
    effective_config: dict[str, Any] = Field(default_factory=dict)
    config_schema: dict[str, Any] = Field(default_factory=dict)
    config_source: str = "default"


class StrategyConfigResponse(APIModel):
    strategy_code: str
    source: str
    config: dict[str, Any] = Field(default_factory=dict)
    default_config: dict[str, Any] = Field(default_factory=dict)
    config_schema: dict[str, Any] = Field(default_factory=dict)
    updated_at: Optional[datetime] = None


class StrategyRunSummaryResponse(APIModel):
    id: int
    strategy_code: str
    strategy_name: str
    mode: str
    status: str
    symbols: list[str] = Field(default_factory=list)
    timeframes: list[str] = Field(default_factory=list)
    started_at: Optional[datetime] = None
    stopped_at: Optional[datetime] = None
    last_processed_candle_at: Optional[datetime] = None
    created_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)
    account_balance: Optional[Decimal] = None
    currency: Optional[str] = None
    open_positions_count: int = 0


class StrategyRunDetailResponse(StrategyRunSummaryResponse):
    pass


class BacktestListItemResponse(APIModel):
    id: int
    strategy_code: str
    strategy_name: str
    status: str
    symbol: str
    timeframe: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    initial_capital: Decimal = Decimal("0")
    final_equity: Decimal = Decimal("0")
    total_return_pct: Decimal = Decimal("0")
    max_drawdown_pct: Decimal = Decimal("0")
    win_rate_pct: Decimal = Decimal("0")
    total_trades: int = 0
    error_text: Optional[str] = None


class DataSyncRequest(APIModel):
    mode: SyncMode = "manual"
    exchange_code: str = "binance_us"
    symbol: str
    timeframe: str
    start_at: Optional[datetime] = None
    end_at: Optional[datetime] = None

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Symbol must not be empty")
        return normalized

    @field_validator("exchange_code")
    @classmethod
    def validate_exchange_code(cls, value: str) -> str:
        return normalize_exchange_code(value)

    @field_validator("timeframe")
    @classmethod
    def validate_timeframe(cls, value: str) -> str:
        BinanceUSTimeframe.from_code(value)
        return value

    @model_validator(mode="after")
    def validate_range(self) -> "DataSyncRequest":
        if self.mode in {"initial", "manual"}:
            if self.start_at is None or self.end_at is None:
                raise ValueError("start_at and end_at are required for initial and manual sync")
        if self.start_at is not None and self.end_at is not None and self.end_at <= self.start_at:
            raise ValueError("end_at must be greater than start_at")
        return self


class CandleCoverageResponse(APIModel):
    exchange_code: str
    symbol: str
    timeframe: str
    requested_start_at: Optional[datetime] = None
    requested_end_at: Optional[datetime] = None
    loaded_start_at: Optional[datetime] = None
    loaded_end_at: Optional[datetime] = None
    candle_count: int = 0
    expected_candle_count: int = 0
    missing_candle_count: int = 0
    completion_pct: Decimal = Decimal("0")


class DataSyncResponse(APIModel):
    job_id: int
    exchange: str
    symbol: str
    timeframe: str
    start_at: datetime
    end_at: datetime
    fetched_rows: int
    normalized_rows: int
    inserted_rows: int
    status: str
    coverage: Optional[CandleCoverageResponse] = None


class SyncJobResponse(APIModel):
    id: int
    exchange: str
    symbol: str
    timeframe: str
    start_at: Optional[datetime] = None
    end_at: Optional[datetime] = None
    status: str
    rows_inserted: int
    error_text: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    coverage: Optional[CandleCoverageResponse] = None


class CandleResponse(APIModel):
    id: int
    exchange_code: str
    symbol: str
    timeframe: str
    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    created_at: datetime


class SignalResponse(APIModel):
    id: int
    strategy_run_id: int
    strategy_code: Optional[str] = None
    symbol: str
    timeframe: str
    signal_type: str
    signal_strength: Decimal
    payload: dict[str, Any] = Field(default_factory=dict)
    candle_time: datetime
    created_at: datetime


class TradeResponse(APIModel):
    id: int
    strategy_run_id: int
    strategy_code: Optional[str] = None
    symbol: str
    entry_price: Decimal
    exit_price: Decimal
    qty: Decimal
    pnl: Decimal
    pnl_pct: Decimal
    fees: Decimal
    slippage: Decimal
    opened_at: datetime
    closed_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class PositionResponse(APIModel):
    id: int
    strategy_run_id: int
    strategy_code: Optional[str] = None
    symbol: str
    side: str
    qty: Decimal
    avg_entry_price: Decimal
    stop_price: Optional[Decimal] = None
    take_profit_price: Optional[Decimal] = None
    status: str
    opened_at: datetime
    closed_at: Optional[datetime] = None


class AppLogResponse(APIModel):
    id: int
    scope: str
    level: str
    message: str
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class DashboardRunStatus(APIModel):
    active_paper_runs: int = 0
    stopped_paper_runs: int = 0
    failed_paper_runs: int = 0
    recent_backtests: int = 0


class DashboardPerformanceSnapshot(APIModel):
    backtest_run_id: int
    strategy_code: str
    symbol: str
    timeframe: str
    total_return_pct: Decimal
    win_rate_pct: Decimal
    max_drawdown_pct: Decimal
    total_trades: int


class DashboardDataSyncStatus(APIModel):
    latest_job: Optional[SyncJobResponse] = None
    recent_jobs: list[SyncJobResponse] = Field(default_factory=list)


class DashboardSummaryResponse(APIModel):
    strategies: list[StrategySummaryResponse] = Field(default_factory=list)
    run_status: DashboardRunStatus = Field(default_factory=DashboardRunStatus)
    key_performance_metrics: list[DashboardPerformanceSnapshot] = Field(default_factory=list)
    open_positions_count: int = 0
    recent_trades: list[TradeResponse] = Field(default_factory=list)
    recent_backtests: list[BacktestListItemResponse] = Field(default_factory=list)
    data_sync_status: DashboardDataSyncStatus = Field(default_factory=DashboardDataSyncStatus)
