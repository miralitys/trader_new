from __future__ import annotations

from datetime import datetime, timedelta, timezone

from contextlib import contextmanager

from app.schemas.market_neutral_research import (
    MarketNeutralCostScenario,
    MarketNeutralSweepConfig,
    MarketNeutralSweepResult,
)
from app.services.market_neutral_research_service import (
    BasisSnapshot,
    CrossVenueSnapshot,
    FundingEvent,
    MarketNeutralResearchService,
)


def test_perp_premium_mean_reversion_finds_profitable_reversion() -> None:
    service = MarketNeutralResearchService()
    config = MarketNeutralSweepConfig(min_trades_for_viability=1)
    cost = MarketNeutralCostScenario(name="zero")
    base_time = datetime(2026, 3, 15, 0, 0, tzinfo=timezone.utc)
    basis_values = [0.001, 0.0012, 0.0011, 0.0010, 0.0011, 0.0120, 0.0040, 0.0013]
    snapshots = [
        BasisSnapshot(ts=base_time + timedelta(minutes=5 * index), basis_pct=value, spot_price=100.0, perp_price=100.0)
        for index, value in enumerate(basis_values)
    ]

    result = service.simulate_perp_premium_mean_reversion(
        symbol="BTC-USDT",
        venue="okx_swap",
        snapshots=snapshots,
        funding_events=[],
        cost_scenario=cost,
        config=config,
        variant={
            "name": "unit",
            "lookback_bars": 5,
            "entry_z": 1.5,
            "exit_z": 0.5,
            "max_hold_bars": 3,
            "min_abs_basis_pct": 0.002,
            "min_expected_edge_usd": 0.0,
        },
    )

    assert result.trades == 1
    assert result.total_net_pnl_usd > 0
    assert result.looks_viable is True


def test_funding_spike_fade_finds_profitable_spike_fade() -> None:
    service = MarketNeutralResearchService()
    config = MarketNeutralSweepConfig(min_trades_for_viability=1)
    cost = MarketNeutralCostScenario(name="zero")
    base_time = datetime(2026, 3, 15, 0, 0, tzinfo=timezone.utc)
    snapshots = [
        BasisSnapshot(ts=base_time + timedelta(hours=8 * index), basis_pct=value, spot_price=100.0, perp_price=100.0)
        for index, value in enumerate([0.015, 0.003, 0.001])
    ]
    funding_events = [
        FundingEvent(ts=base_time + timedelta(hours=8 * index), rate=value)
        for index, value in enumerate([0.0012, 0.0002, 0.0001])
    ]

    result = service.simulate_funding_spike_fade(
        symbol="ACT-USDT",
        venue="okx_swap",
        snapshots=snapshots,
        funding_events=funding_events,
        cost_scenario=cost,
        config=config,
        variant={
            "name": "unit",
            "min_abs_funding_rate": 0.0005,
            "min_abs_basis_pct": 0.002,
            "hold_intervals": 1,
            "two_sided": False,
            "exit_on_normalization": True,
            "normalize_basis_pct": 0.002,
        },
    )

    assert result.trades == 1
    assert result.total_net_pnl_usd > 0
    assert result.looks_viable is True


def test_cross_venue_basis_spread_finds_profitable_convergence() -> None:
    service = MarketNeutralResearchService()
    config = MarketNeutralSweepConfig(min_trades_for_viability=1)
    cost = MarketNeutralCostScenario(name="zero")
    base_time = datetime(2026, 3, 15, 0, 0, tzinfo=timezone.utc)
    spread_values = [0.0004, 0.0005, 0.0003, 0.0004, 0.0005, 0.0100, 0.0030, 0.0006]
    snapshots = [
        CrossVenueSnapshot(
            ts=base_time + timedelta(minutes=5 * index),
            spread_pct=value,
            primary_basis_pct=value,
            secondary_basis_pct=0.0,
        )
        for index, value in enumerate(spread_values)
    ]

    result = service.simulate_cross_venue_basis_spread(
        symbol="MMT-USDT",
        primary_venue="okx_swap",
        secondary_venue="binance_futures",
        snapshots=snapshots,
        primary_funding_events=[],
        secondary_funding_events=[],
        cost_scenario=cost,
        config=config,
        variant={
            "name": "unit",
            "lookback_bars": 5,
            "entry_z": 1.5,
            "exit_z": 0.5,
            "max_hold_bars": 3,
            "min_abs_spread_pct": 0.001,
            "min_expected_edge_usd": 0.0,
        },
    )

    assert result.trades == 1
    assert result.total_net_pnl_usd > 0
    assert result.looks_viable is True


def test_perp_premium_walk_forward_selects_best_train_candidate(monkeypatch) -> None:
    service = MarketNeutralResearchService(repository=object())
    config = MarketNeutralSweepConfig(
        cost_scenarios=[
            MarketNeutralCostScenario(name="maker_taker"),
        ],
        min_trades_for_viability=1,
    )

    @contextmanager
    def fake_session_scope():
        yield None

    monkeypatch.setattr("app.services.market_neutral_research_service.session_scope", fake_session_scope)

    def fake_load_symbol_venue_data(*, repository, symbol, venue, start_at, end_at, max_alignment_seconds):
        return {
            "snapshots": [(start_at, end_at, venue)],
            "funding_events": [],
        }

    def fake_simulate(*, symbol, venue, snapshots, funding_events, cost_scenario, config, variant):
        start_at, end_at, snapshot_venue = snapshots[0]
        label = variant["name"]
        is_train = (end_at - start_at).days >= 14
        if is_train and snapshot_venue == "okx_swap" and label == "lookback48_z1.5_hold48":
            net = 100
        elif is_train and snapshot_venue == "binance_futures" and label == "lookback48_z2.0_hold24":
            net = 120
        elif not is_train and snapshot_venue == "binance_futures" and label == "lookback48_z2.0_hold24":
            net = 15
        else:
            net = -10
        looks_viable = net > 0
        return MarketNeutralSweepResult(
            strategy="PerpPremiumMeanReversion",
            variant_name=str(label),
            cost_scenario=cost_scenario.name,
            symbol=symbol,
            primary_venue=venue,
            secondary_venue=None,
            trades=5 if looks_viable else 2,
            wins=3 if looks_viable else 0,
            losses=2 if looks_viable else 2,
            win_rate=service._quantize(0.6 if looks_viable else 0.0),
            positive_share=service._quantize(0.6 if looks_viable else 0.0),
            total_net_pnl_usd=service._quantize(net),
            avg_net_pnl_usd=service._quantize(net / 5 if looks_viable else net / 2),
            avg_spread_pnl_usd=service._quantize(net / 5 if looks_viable else net / 2),
            avg_funding_pnl_usd=service._quantize(0),
            avg_cost_usd=service._quantize(1),
            avg_hold_bars=service._quantize(5),
            avg_hold_funding_intervals=service._quantize(1),
            looks_viable=looks_viable,
            screening_notes=[] if looks_viable else ["avg_net_non_positive"],
        )

    monkeypatch.setattr(service, "_load_symbol_venue_data", fake_load_symbol_venue_data)
    monkeypatch.setattr(service, "simulate_perp_premium_mean_reversion", fake_simulate)

    summaries = service.run_perp_premium_walk_forward(
        symbol="ACT-USDT",
        timeframe="5m",
        start_at=datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc),
        end_at=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc),
        train_days=14,
        test_days=7,
        config=config,
        cost_scenario_names=["maker_taker"],
    )

    assert len(summaries) == 1
    summary = summaries[0]
    assert summary.evaluated_windows == 2
    assert summary.positive_windows == 2
    assert summary.looks_stable is True
    assert all(window.selected_primary_venue == "binance_futures" for window in summary.windows)
    assert all(window.selected_variant_name == "lookback48_z2.0_hold24" for window in summary.windows)
