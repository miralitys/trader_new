from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from app.schemas.funding_basis import (
    FundingBasisAssetReport,
    FundingBasisResearchConfig,
    FundingBasisResearchReport,
)
from app.services.funding_basis_comparison_service import FundingBasisComparisonService
from app.services.funding_basis_research_service import FundingBasisResearchService
from app.utils.research_symbols import (
    normalize_research_symbol,
    select_nearest_snapshot,
    to_okx_index_inst_id,
    to_okx_swap_inst_id,
)


def test_normalize_research_symbol_handles_spot_and_perp_formats() -> None:
    assert normalize_research_symbol("BTC-USDT") == "BTC-USDT"
    assert normalize_research_symbol("BTCUSDT") == "BTC-USDT"
    assert normalize_research_symbol("btcusdtperp") == "BTC-USDT"
    assert normalize_research_symbol("BTC-USDT-SWAP") == "BTC-USDT"
    assert to_okx_swap_inst_id("BTCUSDT") == "BTC-USDT-SWAP"
    assert to_okx_index_inst_id("BTCUSDT") == "BTC-USDT"


def test_select_nearest_snapshot_returns_closest_match_within_window() -> None:
    target = datetime(2026, 3, 15, 8, 0, tzinfo=timezone.utc)
    snapshots = [
        SimpleNamespace(ts=datetime(2026, 3, 15, 7, 55, tzinfo=timezone.utc), value="early"),
        SimpleNamespace(ts=datetime(2026, 3, 15, 8, 4, tzinfo=timezone.utc), value="late"),
    ]

    row, info = select_nearest_snapshot(
        target,
        snapshots,
        max_alignment_seconds=300,
        timestamp_getter=lambda item: item.ts,
    )

    assert row is not None
    assert row.value == "late"
    assert info.matched is True
    assert info.distance_seconds == 240


def test_funding_basis_report_computes_net_carry_and_screening() -> None:
    service = FundingBasisResearchService()
    config = FundingBasisResearchConfig(
        min_funding_rate=Decimal("0.0001"),
        min_basis_pct=Decimal("0.0002"),
        notional_usd=Decimal("10000"),
        spot_fee_pct=Decimal("0.0005"),
        perp_fee_pct=Decimal("0.0004"),
        slippage_pct=Decimal("0.0001"),
        max_snapshot_alignment_seconds=600,
    )
    funding_rows = [
        SimpleNamespace(
            exchange="binance_futures",
            symbol="BTC-USDT",
            funding_time=datetime(2026, 3, 15, 8, 0, tzinfo=timezone.utc),
            funding_rate=Decimal("0.0008"),
        ),
        SimpleNamespace(
            exchange="binance_futures",
            symbol="BTC-USDT",
            funding_time=datetime(2026, 3, 15, 16, 0, tzinfo=timezone.utc),
            funding_rate=Decimal("0.0006"),
        ),
    ]
    spot_rows = [
        SimpleNamespace(ts=datetime(2026, 3, 15, 7, 55, tzinfo=timezone.utc), mid=Decimal("50000"), close=Decimal("50000")),
        SimpleNamespace(ts=datetime(2026, 3, 15, 15, 58, tzinfo=timezone.utc), mid=Decimal("50500"), close=Decimal("50500")),
    ]
    perp_rows = [
        SimpleNamespace(ts=datetime(2026, 3, 15, 8, 2, tzinfo=timezone.utc), mid=Decimal("50100"), mark_price=Decimal("50100")),
        SimpleNamespace(ts=datetime(2026, 3, 15, 16, 1, tzinfo=timezone.utc), mid=Decimal("50650"), mark_price=Decimal("50650")),
    ]

    observations, summary = service.build_symbol_report_from_rows(
        symbol="BTC-USDT",
        funding_rows=funding_rows,
        spot_rows=spot_rows,
        perp_rows=perp_rows,
        config=config,
    )

    assert len(observations) == 2
    assert observations[0].basis_pct == Decimal("0.002000")
    assert observations[0].expected_gross_funding_carry == Decimal("8.000000")
    assert observations[0].expected_net_carry == Decimal("-14.000000")
    assert summary.total_funding_observations == 2
    assert summary.aligned_observations == 2
    assert summary.avg_funding_rate == Decimal("0.000700")
    assert summary.funding_above_threshold_share == Decimal("1.000000")
    assert summary.avg_basis_pct > Decimal("0.002")
    assert summary.looks_viable is False
    assert "avg_net_carry_non_positive" in summary.screening_notes


def test_funding_basis_comparison_report_picks_best_venue(monkeypatch) -> None:
    config = FundingBasisResearchConfig()

    venue_assets = {
        "binance_futures": FundingBasisAssetReport(
            symbol="BTC-USDT",
            total_funding_observations=10,
            aligned_observations=10,
            insufficient_alignment_observations=0,
            avg_funding_rate=Decimal("0.000200"),
            funding_above_threshold_share=Decimal("0.500000"),
            avg_basis_pct=Decimal("0.000300"),
            avg_expected_gross_funding_carry=Decimal("2.000000"),
            avg_expected_net_carry=Decimal("-1.000000"),
            looks_viable=False,
            screening_notes=["avg_net_carry_non_positive"],
        ),
        "okx_swap": FundingBasisAssetReport(
            symbol="BTC-USDT",
            total_funding_observations=10,
            aligned_observations=10,
            insufficient_alignment_observations=0,
            avg_funding_rate=Decimal("0.000400"),
            funding_above_threshold_share=Decimal("0.800000"),
            avg_basis_pct=Decimal("0.000600"),
            avg_expected_gross_funding_carry=Decimal("4.000000"),
            avg_expected_net_carry=Decimal("1.500000"),
            looks_viable=True,
            screening_notes=[],
        ),
    }

    def fake_build_report(self, *, symbols, timeframe, start_at, end_at, config):
        asset = venue_assets[self.perp_exchange]
        return FundingBasisResearchReport(
            generated_at=start_at,
            spot_exchange=self.spot_exchange,
            perp_exchange=self.perp_exchange,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
            config=config,
            assets=[asset],
            observations={"BTC-USDT": []},
        )

    monkeypatch.setattr(FundingBasisResearchService, "build_report", fake_build_report)

    service = FundingBasisComparisonService()
    report = service.build_comparison_report(
        symbols=["BTC-USDT"],
        timeframe="5m",
        start_at=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc),
        end_at=datetime(2026, 3, 2, 0, 0, tzinfo=timezone.utc),
        perp_exchanges=["binance_futures", "okx_swap"],
        config=config,
    )

    assert report.perp_exchanges == ["binance_futures", "okx_swap"]
    assert report.comparisons[0].symbol == "BTC-USDT"
    assert report.comparisons[0].best_net_carry_venue == "okx_swap"
    assert report.comparisons[0].best_funding_rate_venue == "okx_swap"
    assert report.comparisons[0].best_basis_venue == "okx_swap"
    assert report.comparisons[0].viable_venues == ["okx_swap"]
