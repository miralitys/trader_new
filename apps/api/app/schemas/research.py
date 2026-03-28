from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import Field

from app.schemas.common import APIModel


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
