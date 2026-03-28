from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, Field


StrategyName = Literal[
    "PerpPremiumMeanReversion",
    "CrossVenueBasisSpread",
    "FundingSpikeFade",
]


class MarketNeutralCostScenario(BaseModel):
    name: str
    spot_entry_fee_pct: Decimal = Field(default=Decimal("0"))
    spot_exit_fee_pct: Decimal = Field(default=Decimal("0"))
    perp_entry_fee_pct: Decimal = Field(default=Decimal("0"))
    perp_exit_fee_pct: Decimal = Field(default=Decimal("0"))
    spot_entry_slippage_pct: Decimal = Field(default=Decimal("0"))
    spot_exit_slippage_pct: Decimal = Field(default=Decimal("0"))
    perp_entry_slippage_pct: Decimal = Field(default=Decimal("0"))
    perp_exit_slippage_pct: Decimal = Field(default=Decimal("0"))


class MarketNeutralSweepConfig(BaseModel):
    notional_usd: Decimal = Field(default=Decimal("10000"))
    max_alignment_seconds: int = Field(default=600, ge=1)
    min_trades_for_viability: int = Field(default=3, ge=1)
    positive_share_threshold: Decimal = Field(default=Decimal("0.50"))
    cost_scenarios: list[MarketNeutralCostScenario] = Field(
        default_factory=lambda: [
            MarketNeutralCostScenario(
                name="taker_strict",
                spot_entry_fee_pct=Decimal("0.0010"),
                spot_exit_fee_pct=Decimal("0.0010"),
                perp_entry_fee_pct=Decimal("0.0005"),
                perp_exit_fee_pct=Decimal("0.0005"),
                spot_entry_slippage_pct=Decimal("0.0003"),
                spot_exit_slippage_pct=Decimal("0.0003"),
                perp_entry_slippage_pct=Decimal("0.0003"),
                perp_exit_slippage_pct=Decimal("0.0003"),
            ),
            MarketNeutralCostScenario(
                name="maker_taker",
                spot_entry_fee_pct=Decimal("0.0005"),
                spot_exit_fee_pct=Decimal("0.0010"),
                perp_entry_fee_pct=Decimal("0.0002"),
                perp_exit_fee_pct=Decimal("0.0005"),
                spot_entry_slippage_pct=Decimal("0.00005"),
                spot_exit_slippage_pct=Decimal("0.0001"),
                perp_entry_slippage_pct=Decimal("0.00005"),
                perp_exit_slippage_pct=Decimal("0.0001"),
            ),
            MarketNeutralCostScenario(
                name="maker_maker",
                spot_entry_fee_pct=Decimal("0.0005"),
                spot_exit_fee_pct=Decimal("0.0005"),
                perp_entry_fee_pct=Decimal("0.0002"),
                perp_exit_fee_pct=Decimal("0.0002"),
                spot_entry_slippage_pct=Decimal("0.00002"),
                spot_exit_slippage_pct=Decimal("0.00002"),
                perp_entry_slippage_pct=Decimal("0.00002"),
                perp_exit_slippage_pct=Decimal("0.00002"),
            ),
        ]
    )


class MarketNeutralSweepResult(BaseModel):
    strategy: StrategyName
    variant_name: str
    cost_scenario: str
    symbol: str
    primary_venue: str
    secondary_venue: Optional[str] = None
    trades: int
    wins: int
    losses: int
    win_rate: Decimal
    positive_share: Decimal
    total_net_pnl_usd: Decimal
    avg_net_pnl_usd: Decimal
    avg_spread_pnl_usd: Decimal
    avg_funding_pnl_usd: Decimal
    avg_cost_usd: Decimal
    avg_hold_bars: Decimal
    avg_hold_funding_intervals: Decimal
    looks_viable: bool
    screening_notes: list[str] = Field(default_factory=list)


class MarketNeutralSweepReport(BaseModel):
    generated_at: datetime
    timeframe: str
    start_at: datetime
    end_at: datetime
    config: MarketNeutralSweepConfig
    symbols: list[str]
    results: list[MarketNeutralSweepResult]
    best_result: Optional[MarketNeutralSweepResult] = None
    all_rejected: bool = True


class PerpPremiumWalkForwardWindow(BaseModel):
    label: str
    train_start_at: datetime
    train_end_at: datetime
    test_start_at: datetime
    test_end_at: datetime
    selected_primary_venue: Optional[str] = None
    selected_variant_name: Optional[str] = None
    train_result: Optional[MarketNeutralSweepResult] = None
    test_result: Optional[MarketNeutralSweepResult] = None
    notes: list[str] = Field(default_factory=list)


class PerpPremiumWalkForwardSummary(BaseModel):
    symbol: str
    cost_scenario: str
    windows: list[PerpPremiumWalkForwardWindow]
    evaluated_windows: int
    positive_windows: int
    total_test_net_pnl_usd: Decimal
    avg_test_net_pnl_usd: Decimal
    total_test_trades: int
    avg_test_win_rate: Decimal
    looks_stable: bool
    screening_notes: list[str] = Field(default_factory=list)
