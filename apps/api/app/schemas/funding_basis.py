from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field


class FundingBasisResearchConfig(BaseModel):
    min_funding_rate: Decimal = Field(default=Decimal("0.0001"))
    min_basis_pct: Decimal = Field(default=Decimal("0.0005"))
    notional_usd: Decimal = Field(default=Decimal("10000"))
    spot_fee_pct: Decimal = Field(default=Decimal("0.001"))
    perp_fee_pct: Decimal = Field(default=Decimal("0.0005"))
    slippage_pct: Decimal = Field(default=Decimal("0.0003"))
    max_snapshot_alignment_seconds: int = Field(default=600, ge=1)


class FundingBasisObservation(BaseModel):
    symbol: str
    funding_time: datetime
    spot_reference_price: Decimal
    perp_reference_price: Decimal
    basis_pct: Decimal
    funding_rate: Decimal
    expected_gross_funding_carry: Decimal
    entry_spot_fee: Decimal
    entry_perp_fee: Decimal
    exit_spot_fee: Decimal
    exit_perp_fee: Decimal
    total_slippage: Decimal
    expected_net_carry: Decimal


class FundingBasisAssetReport(BaseModel):
    symbol: str
    total_funding_observations: int
    aligned_observations: int
    insufficient_alignment_observations: int
    avg_funding_rate: Decimal
    funding_above_threshold_share: Decimal
    avg_basis_pct: Decimal
    avg_expected_gross_funding_carry: Decimal
    avg_expected_net_carry: Decimal
    looks_viable: bool
    screening_notes: list[str] = Field(default_factory=list)


class FundingBasisResearchReport(BaseModel):
    generated_at: datetime
    spot_exchange: str
    perp_exchange: str
    timeframe: str
    start_at: datetime
    end_at: datetime
    config: FundingBasisResearchConfig
    assets: list[FundingBasisAssetReport]
    observations: dict[str, list[FundingBasisObservation]]


class FundingBasisVenueComparison(BaseModel):
    symbol: str
    venue_reports: dict[str, FundingBasisAssetReport]
    viable_venues: list[str] = Field(default_factory=list)
    best_net_carry_venue: Optional[str] = None
    best_funding_rate_venue: Optional[str] = None
    best_basis_venue: Optional[str] = None


class FundingBasisVenueComparisonReport(BaseModel):
    generated_at: datetime
    spot_exchange: str
    perp_exchanges: list[str]
    timeframe: str
    start_at: datetime
    end_at: datetime
    config: FundingBasisResearchConfig
    reports: dict[str, FundingBasisResearchReport]
    comparisons: list[FundingBasisVenueComparison]


class FundingBasisIngestionResult(BaseModel):
    symbol: str
    timeframe: str
    spot_rows_inserted: int = 0
    perp_rows_inserted: int = 0
    funding_rows_inserted: int = 0
    spot_source: str = "rest"
    perp_source: str = "rest"
    funding_source: str = "rest"
    notes: list[str] = Field(default_factory=list)


class FundingBasisBatchIngestionResult(BaseModel):
    mode: str
    exchange_spot: str
    exchange_perp: str
    timeframe: str
    start_at: Optional[datetime] = None
    end_at: Optional[datetime] = None
    symbols: list[str]
    results: list[FundingBasisIngestionResult]
