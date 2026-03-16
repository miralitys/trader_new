from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional, Sequence

from app.schemas.funding_basis import (
    FundingBasisAssetReport,
    FundingBasisResearchConfig,
    FundingBasisResearchReport,
    FundingBasisVenueComparison,
    FundingBasisVenueComparisonReport,
)
from app.services.funding_basis_research_service import FundingBasisResearchService
from app.utils.research_symbols import normalize_research_symbol
from app.utils.time import ensure_utc, utc_now

ZERO = Decimal("0")


class FundingBasisComparisonService:
    def __init__(self, *, spot_exchange: str = "binance_spot") -> None:
        self.spot_exchange = spot_exchange

    def build_comparison_report(
        self,
        *,
        symbols: list[str],
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
        perp_exchanges: Sequence[str],
        config: Optional[FundingBasisResearchConfig] = None,
    ) -> FundingBasisVenueComparisonReport:
        effective_config = config or FundingBasisResearchConfig()
        reports: dict[str, FundingBasisResearchReport] = {}
        normalized_symbols = [normalize_research_symbol(symbol) for symbol in symbols]

        for venue in perp_exchanges:
            service = FundingBasisResearchService(
                spot_exchange=self.spot_exchange,
                perp_exchange=venue,
            )
            reports[venue] = service.build_report(
                symbols=normalized_symbols,
                timeframe=timeframe,
                start_at=start_at,
                end_at=end_at,
                config=effective_config,
            )

        comparisons = [
            self._compare_symbol(symbol=symbol, reports=reports)
            for symbol in normalized_symbols
        ]

        return FundingBasisVenueComparisonReport(
            generated_at=utc_now(),
            spot_exchange=self.spot_exchange,
            perp_exchanges=list(perp_exchanges),
            timeframe=timeframe,
            start_at=ensure_utc(start_at),
            end_at=ensure_utc(end_at),
            config=effective_config,
            reports=reports,
            comparisons=comparisons,
        )

    def _compare_symbol(
        self,
        *,
        symbol: str,
        reports: dict[str, FundingBasisResearchReport],
    ) -> FundingBasisVenueComparison:
        venue_reports: dict[str, FundingBasisAssetReport] = {}
        for venue, report in reports.items():
            asset = next((item for item in report.assets if item.symbol == symbol), None)
            if asset is not None:
                venue_reports[venue] = asset

        viable_venues = [venue for venue, asset in venue_reports.items() if asset.looks_viable]

        return FundingBasisVenueComparison(
            symbol=symbol,
            venue_reports=venue_reports,
            viable_venues=viable_venues,
            best_net_carry_venue=self._pick_best_venue(
                venue_reports,
                key=lambda asset: asset.avg_expected_net_carry,
            ),
            best_funding_rate_venue=self._pick_best_venue(
                venue_reports,
                key=lambda asset: asset.avg_funding_rate,
            ),
            best_basis_venue=self._pick_best_venue(
                venue_reports,
                key=lambda asset: asset.avg_basis_pct,
            ),
        )

    def _pick_best_venue(
        self,
        venue_reports: dict[str, FundingBasisAssetReport],
        *,
        key,
    ) -> Optional[str]:
        eligible = [(venue, asset) for venue, asset in venue_reports.items() if asset.aligned_observations > 0]
        if not eligible:
            return None
        eligible.sort(key=lambda item: (key(item[1]), item[0]), reverse=True)
        best_venue, best_asset = eligible[0]
        if key(best_asset) == ZERO and all(key(asset) == ZERO for _, asset in eligible):
            return None
        return best_venue
