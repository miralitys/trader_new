from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import Field, field_validator

from app.schemas.common import APIModel
from app.integrations.binance_us import BinanceUSTimeframe
from app.utils.exchanges import normalize_exchange_code
from app.utils.symbols import compact_supported_symbols


class ResearchCoverageResponse(APIModel):
    symbol: str
    timeframe: str
    candle_count: int = 0
    loaded_start_at: datetime | None = None
    loaded_end_at: datetime | None = None
    completion_pct: Decimal = Decimal("0")
    ready_for_pattern_scan: bool = False


class PatternSummaryResponse(APIModel):
    pattern_code: str
    pattern_name: str
    symbol: str
    timeframe: str
    sample_size: int = 0
    win_rate_pct: Decimal = Decimal("0")
    avg_forward_return_pct: Decimal = Decimal("0")
    median_forward_return_pct: Decimal = Decimal("0")
    avg_net_return_pct: Decimal = Decimal("0")
    best_forward_return_pct: Decimal = Decimal("0")
    worst_forward_return_pct: Decimal = Decimal("0")
    verdict: str = "insufficient_sample"


class ResearchSummaryResponse(APIModel):
    generated_at: datetime
    exchange_code: str
    lookback_days: int
    forward_bars: int
    fee_pct: Decimal
    slippage_pct: Decimal
    max_bars_per_series: int
    coverage: list[ResearchCoverageResponse] = Field(default_factory=list)
    patterns: list[PatternSummaryResponse] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class PatternScanRequest(APIModel):
    exchange_code: str = "binance_us"
    symbols: list[str] = Field(default_factory=list)
    timeframes: list[str] = Field(default_factory=list)
    lookback_days: int = 730
    forward_bars: int = 12
    fee_pct: Decimal = Decimal("0.001")
    slippage_pct: Decimal = Decimal("0.0005")
    max_bars_per_series: int = 5000

    @field_validator("exchange_code")
    @classmethod
    def validate_exchange_code(cls, value: str) -> str:
        return normalize_exchange_code(value)

    @field_validator("symbols")
    @classmethod
    def validate_symbols(cls, value: list[str]) -> list[str]:
        normalized = compact_supported_symbols(value)
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


class PatternScanProgressResponse(APIModel):
    phase: str = "queued"
    processed_series: int = 0
    total_series: int = 0
    percent_complete: Decimal = Decimal("0")
    current_symbol: str | None = None
    current_timeframe: str | None = None


class PatternScanRunResponse(APIModel):
    id: int
    exchange: str
    symbols: list[str] = Field(default_factory=list)
    timeframes: list[str] = Field(default_factory=list)
    lookback_days: int
    forward_bars: int
    fee_pct: Decimal = Decimal("0.001")
    slippage_pct: Decimal = Decimal("0.0005")
    max_bars_per_series: int = 5000
    status: str
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_text: str | None = None
    progress: PatternScanProgressResponse | None = None
    report_summary: ResearchSummaryResponse | None = None
    report: ResearchSummaryResponse | None = None
    created_at: datetime
    updated_at: datetime
