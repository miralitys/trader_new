from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from math import sqrt
from typing import Optional, Sequence

from app.db.session import session_scope
from app.models import FundingRate, PerpPrice, SpotPrice
from app.repositories.funding_basis_repository import FundingBasisRepository
from app.schemas.market_neutral_research import (
    MarketNeutralCostScenario,
    MarketNeutralSweepConfig,
    PerpPremiumWalkForwardSummary,
    PerpPremiumWalkForwardWindow,
    MarketNeutralSweepReport,
    MarketNeutralSweepResult,
)
from app.utils.research_symbols import SnapshotAlignmentResult, normalize_research_symbol
from app.utils.time import ensure_utc, utc_now

ZERO = Decimal("0")


@dataclass(frozen=True)
class BasisSnapshot:
    ts: datetime
    basis_pct: float
    spot_price: float
    perp_price: float


@dataclass(frozen=True)
class FundingEvent:
    ts: datetime
    rate: float


@dataclass(frozen=True)
class CrossVenueSnapshot:
    ts: datetime
    spread_pct: float
    primary_basis_pct: float
    secondary_basis_pct: float


@dataclass(frozen=True)
class SimulatedTrade:
    net_pnl_usd: float
    spread_pnl_usd: float
    funding_pnl_usd: float
    cost_usd: float
    hold_bars: int
    hold_funding_intervals: int


class MarketNeutralResearchService:
    SUPPORTED_PERP_VENUES = ("binance_futures", "okx_swap")

    def __init__(self, repository: Optional[FundingBasisRepository] = None) -> None:
        self.repository = repository
        self.spot_exchange = "binance_spot"

    def build_report(
        self,
        *,
        symbols: list[str],
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
        config: Optional[MarketNeutralSweepConfig] = None,
    ) -> MarketNeutralSweepReport:
        effective_config = config or MarketNeutralSweepConfig()
        normalized_symbols = [normalize_research_symbol(symbol) for symbol in symbols]
        results: list[MarketNeutralSweepResult] = []

        with session_scope() as session:
            repository = self.repository or FundingBasisRepository(session)
            single_venue_data = {
                (symbol, venue): self._load_symbol_venue_data(
                    repository=repository,
                    symbol=symbol,
                    venue=venue,
                    start_at=start_at,
                    end_at=end_at,
                    max_alignment_seconds=effective_config.max_alignment_seconds,
                )
                for symbol in normalized_symbols
                for venue in self.SUPPORTED_PERP_VENUES
            }
            cross_venue_data = {
                symbol: self._build_cross_venue_snapshots(
                    primary=single_venue_data[(symbol, "okx_swap")]["snapshots"],
                    secondary=single_venue_data[(symbol, "binance_futures")]["snapshots"],
                    max_alignment_seconds=effective_config.max_alignment_seconds,
                )
                for symbol in normalized_symbols
            }

        for cost_scenario in effective_config.cost_scenarios:
            for symbol in normalized_symbols:
                for venue in self.SUPPORTED_PERP_VENUES:
                    venue_payload = single_venue_data[(symbol, venue)]
                    for variant in self._perp_premium_variants():
                        results.append(
                            self.simulate_perp_premium_mean_reversion(
                                symbol=symbol,
                                venue=venue,
                                snapshots=venue_payload["snapshots"],
                                funding_events=venue_payload["funding_events"],
                                cost_scenario=cost_scenario,
                                config=effective_config,
                                variant=variant,
                            )
                        )
                    for variant in self._funding_spike_variants():
                        results.append(
                            self.simulate_funding_spike_fade(
                                symbol=symbol,
                                venue=venue,
                                snapshots=venue_payload["snapshots"],
                                funding_events=venue_payload["funding_events"],
                                cost_scenario=cost_scenario,
                                config=effective_config,
                                variant=variant,
                            )
                        )

                for variant in self._cross_venue_variants():
                    results.append(
                        self.simulate_cross_venue_basis_spread(
                            symbol=symbol,
                            primary_venue="okx_swap",
                            secondary_venue="binance_futures",
                            snapshots=cross_venue_data[symbol],
                            primary_funding_events=single_venue_data[(symbol, "okx_swap")]["funding_events"],
                            secondary_funding_events=single_venue_data[(symbol, "binance_futures")]["funding_events"],
                            cost_scenario=cost_scenario,
                            config=effective_config,
                            variant=variant,
                        )
                    )

        results.sort(
            key=lambda item: (
                item.looks_viable,
                item.total_net_pnl_usd,
                item.avg_net_pnl_usd,
                item.trades,
            ),
            reverse=True,
        )
        best_result = results[0] if results else None
        all_rejected = not any(result.looks_viable for result in results)

        return MarketNeutralSweepReport(
            generated_at=utc_now(),
            timeframe=timeframe,
            start_at=ensure_utc(start_at),
            end_at=ensure_utc(end_at),
            config=effective_config,
            symbols=normalized_symbols,
            results=results,
            best_result=best_result,
            all_rejected=all_rejected,
        )

    def run_perp_premium_walk_forward(
        self,
        *,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
        train_days: int = 14,
        test_days: int = 7,
        config: Optional[MarketNeutralSweepConfig] = None,
        cost_scenario_names: Optional[Sequence[str]] = None,
    ) -> list[PerpPremiumWalkForwardSummary]:
        effective_config = config or MarketNeutralSweepConfig()
        normalized_symbol = normalize_research_symbol(symbol)
        scenarios = [
            scenario
            for scenario in effective_config.cost_scenarios
            if cost_scenario_names is None or scenario.name in cost_scenario_names
        ]
        summaries: list[PerpPremiumWalkForwardSummary] = []
        train_span = timedelta(days=train_days)
        test_span = timedelta(days=test_days)

        with session_scope() as session:
            repository = self.repository or FundingBasisRepository(session)
            for scenario in scenarios:
                windows: list[PerpPremiumWalkForwardWindow] = []
                train_start = ensure_utc(start_at)
                index = 1
                while train_start + train_span + test_span <= ensure_utc(end_at):
                    train_end = train_start + train_span
                    test_start = train_end
                    test_end = min(test_start + test_span, ensure_utc(end_at))
                    best_train_result: Optional[MarketNeutralSweepResult] = None
                    best_variant: Optional[dict[str, float | int | str]] = None
                    best_venue: Optional[str] = None

                    for venue in self.SUPPORTED_PERP_VENUES:
                        train_payload = self._load_symbol_venue_data(
                            repository=repository,
                            symbol=normalized_symbol,
                            venue=venue,
                            start_at=train_start,
                            end_at=train_end,
                            max_alignment_seconds=effective_config.max_alignment_seconds,
                        )
                        for variant in self._perp_premium_variants():
                            train_result = self.simulate_perp_premium_mean_reversion(
                                symbol=normalized_symbol,
                                venue=venue,
                                snapshots=train_payload["snapshots"],
                                funding_events=train_payload["funding_events"],
                                cost_scenario=scenario,
                                config=effective_config,
                                variant=variant,
                            )
                            if self._is_better_result(train_result, best_train_result):
                                best_train_result = train_result
                                best_variant = variant
                                best_venue = venue

                    notes: list[str] = []
                    test_result: Optional[MarketNeutralSweepResult] = None
                    if best_train_result is None or best_variant is None or best_venue is None or best_train_result.trades == 0:
                        notes.append("no_train_candidate")
                    else:
                        test_payload = self._load_symbol_venue_data(
                            repository=repository,
                            symbol=normalized_symbol,
                            venue=best_venue,
                            start_at=test_start,
                            end_at=test_end,
                            max_alignment_seconds=effective_config.max_alignment_seconds,
                        )
                        test_result = self.simulate_perp_premium_mean_reversion(
                            symbol=normalized_symbol,
                            venue=best_venue,
                            snapshots=test_payload["snapshots"],
                            funding_events=test_payload["funding_events"],
                            cost_scenario=scenario,
                            config=effective_config,
                            variant=best_variant,
                        )
                        if test_result.trades == 0:
                            notes.append("no_test_trades")

                    windows.append(
                        PerpPremiumWalkForwardWindow(
                            label=f"wf_{index}",
                            train_start_at=train_start,
                            train_end_at=train_end,
                            test_start_at=test_start,
                            test_end_at=test_end,
                            selected_primary_venue=best_venue,
                            selected_variant_name=str(best_variant["name"]) if best_variant is not None else None,
                            train_result=best_train_result,
                            test_result=test_result,
                            notes=notes,
                        )
                    )
                    train_start = train_start + test_span
                    index += 1

                summaries.append(
                    self._summarize_walk_forward(
                        symbol=normalized_symbol,
                        cost_scenario=scenario.name,
                        windows=windows,
                    )
                )

        return summaries

    def simulate_perp_premium_mean_reversion(
        self,
        *,
        symbol: str,
        venue: str,
        snapshots: Sequence[BasisSnapshot],
        funding_events: Sequence[FundingEvent],
        cost_scenario: MarketNeutralCostScenario,
        config: MarketNeutralSweepConfig,
        variant: dict[str, float | int | str],
    ) -> MarketNeutralSweepResult:
        notes: list[str] = []
        if len(snapshots) <= int(variant["lookback_bars"]):
            notes.append("insufficient_snapshots")
            return self._empty_result(
                strategy="PerpPremiumMeanReversion",
                variant_name=str(variant["name"]),
                cost_scenario=cost_scenario.name,
                symbol=symbol,
                primary_venue=venue,
                secondary_venue=None,
                notes=notes,
            )

        cost_usd = self._spot_perp_roundtrip_cost(cost_scenario, float(config.notional_usd))
        trades: list[SimulatedTrade] = []
        cursor = int(variant["lookback_bars"])
        while cursor < len(snapshots) - 1:
            signal_snapshot = snapshots[cursor]
            window = snapshots[cursor - int(variant["lookback_bars"]) : cursor]
            mean_basis, std_basis = self._mean_std(snapshot.basis_pct for snapshot in window)
            if std_basis <= 0:
                cursor += 1
                continue
            zscore = (signal_snapshot.basis_pct - mean_basis) / std_basis
            if abs(zscore) < float(variant["entry_z"]):
                cursor += 1
                continue
            if abs(signal_snapshot.basis_pct) < float(variant["min_abs_basis_pct"]):
                cursor += 1
                continue

            entry_cursor = cursor + 1
            if entry_cursor >= len(snapshots):
                break

            expected_edge_usd = abs(signal_snapshot.basis_pct - mean_basis) * float(config.notional_usd)
            if expected_edge_usd <= cost_usd + float(variant["min_expected_edge_usd"]):
                cursor += 1
                continue

            entry = snapshots[entry_cursor]
            side = -1.0 if signal_snapshot.basis_pct > mean_basis else 1.0
            exit_cursor = min(entry_cursor + int(variant["max_hold_bars"]), len(snapshots) - 1)
            exit_basis = snapshots[exit_cursor].basis_pct
            for future_cursor in range(entry_cursor + 1, min(entry_cursor + int(variant["max_hold_bars"]), len(snapshots) - 1) + 1):
                future_basis = snapshots[future_cursor].basis_pct
                future_zscore = abs((future_basis - mean_basis) / std_basis)
                if future_zscore <= float(variant["exit_z"]):
                    exit_cursor = min(future_cursor + 1, len(snapshots) - 1)
                    exit_basis = future_basis
                    break

            funding_cash, funding_intervals = self._sum_spot_perp_funding(
                funding_events=funding_events,
                entry_time=entry.ts,
                exit_time=snapshots[exit_cursor].ts,
                perp_position_sign=side,
                notional_usd=float(config.notional_usd),
            )
            spread_pnl = side * (exit_basis - entry.basis_pct) * float(config.notional_usd)
            trades.append(
                SimulatedTrade(
                    net_pnl_usd=spread_pnl + funding_cash - cost_usd,
                    spread_pnl_usd=spread_pnl,
                    funding_pnl_usd=funding_cash,
                    cost_usd=cost_usd,
                    hold_bars=max(exit_cursor - entry_cursor, 0),
                    hold_funding_intervals=funding_intervals,
                )
            )
            cursor = exit_cursor + 1

        if not trades:
            notes.append("no_trades")
        return self._summarize_trades(
            strategy="PerpPremiumMeanReversion",
            variant_name=str(variant["name"]),
            cost_scenario=cost_scenario.name,
            symbol=symbol,
            primary_venue=venue,
            secondary_venue=None,
            trades=trades,
            config=config,
            notes=notes,
        )

    def simulate_funding_spike_fade(
        self,
        *,
        symbol: str,
        venue: str,
        snapshots: Sequence[BasisSnapshot],
        funding_events: Sequence[FundingEvent],
        cost_scenario: MarketNeutralCostScenario,
        config: MarketNeutralSweepConfig,
        variant: dict[str, float | int | str | bool],
    ) -> MarketNeutralSweepResult:
        notes: list[str] = []
        if len(funding_events) <= int(variant["hold_intervals"]):
            notes.append("insufficient_funding_events")
            return self._empty_result(
                strategy="FundingSpikeFade",
                variant_name=str(variant["name"]),
                cost_scenario=cost_scenario.name,
                symbol=symbol,
                primary_venue=venue,
                secondary_venue=None,
                notes=notes,
            )
        if not snapshots:
            notes.append("insufficient_snapshots")
            return self._empty_result(
                strategy="FundingSpikeFade",
                variant_name=str(variant["name"]),
                cost_scenario=cost_scenario.name,
                symbol=symbol,
                primary_venue=venue,
                secondary_venue=None,
                notes=notes,
            )

        aligned_events = self._align_funding_to_basis_snapshots(
            funding_events=funding_events,
            snapshots=snapshots,
            max_alignment_seconds=config.max_alignment_seconds,
        )
        if len(aligned_events) <= int(variant["hold_intervals"]):
            notes.append("insufficient_aligned_funding_events")
            return self._empty_result(
                strategy="FundingSpikeFade",
                variant_name=str(variant["name"]),
                cost_scenario=cost_scenario.name,
                symbol=symbol,
                primary_venue=venue,
                secondary_venue=None,
                notes=notes,
            )

        cost_usd = self._spot_perp_roundtrip_cost(cost_scenario, float(config.notional_usd))
        trades: list[SimulatedTrade] = []
        for index, aligned_event in enumerate(aligned_events):
            funding_rate = aligned_event["funding_rate"]
            entry_basis = aligned_event["basis_pct"]
            side: Optional[float] = None
            if funding_rate >= float(variant["min_abs_funding_rate"]) and entry_basis >= float(variant["min_abs_basis_pct"]):
                side = -1.0
            elif bool(variant["two_sided"]) and funding_rate <= -float(variant["min_abs_funding_rate"]) and entry_basis <= -float(variant["min_abs_basis_pct"]):
                side = 1.0
            if side is None:
                continue

            entry_snapshot_index = int(aligned_event["snapshot_index"]) + 1
            if entry_snapshot_index >= len(snapshots):
                continue

            exit_index = min(index + int(variant["hold_intervals"]), len(aligned_events) - 1)
            if bool(variant["exit_on_normalization"]):
                for future_index in range(index + 1, exit_index + 1):
                    future_basis = aligned_events[future_index]["basis_pct"]
                    if side == -1.0 and future_basis <= float(variant["normalize_basis_pct"]):
                        exit_index = future_index
                        break
                    if side == 1.0 and future_basis >= -float(variant["normalize_basis_pct"]):
                        exit_index = future_index
                        break

            exit_event = aligned_events[exit_index]
            exit_snapshot_index = min(int(exit_event["snapshot_index"]) + 1, len(snapshots) - 1)
            entry_basis = snapshots[entry_snapshot_index].basis_pct
            exit_basis = snapshots[exit_snapshot_index].basis_pct
            funding_cash, funding_intervals = self._sum_spot_perp_funding(
                funding_events=funding_events,
                entry_time=aligned_event["funding_time"],
                exit_time=exit_event["funding_time"],
                perp_position_sign=side,
                notional_usd=float(config.notional_usd),
            )
            spread_pnl = side * (exit_basis - entry_basis) * float(config.notional_usd)
            trades.append(
                SimulatedTrade(
                    net_pnl_usd=spread_pnl + funding_cash - cost_usd,
                    spread_pnl_usd=spread_pnl,
                    funding_pnl_usd=funding_cash,
                    cost_usd=cost_usd,
                    hold_bars=max(exit_snapshot_index - entry_snapshot_index, 0),
                    hold_funding_intervals=funding_intervals,
                )
            )

        if not trades:
            notes.append("no_trades")
        return self._summarize_trades(
            strategy="FundingSpikeFade",
            variant_name=str(variant["name"]),
            cost_scenario=cost_scenario.name,
            symbol=symbol,
            primary_venue=venue,
            secondary_venue=None,
            trades=trades,
            config=config,
            notes=notes,
        )

    def simulate_cross_venue_basis_spread(
        self,
        *,
        symbol: str,
        primary_venue: str,
        secondary_venue: str,
        snapshots: Sequence[CrossVenueSnapshot],
        primary_funding_events: Sequence[FundingEvent],
        secondary_funding_events: Sequence[FundingEvent],
        cost_scenario: MarketNeutralCostScenario,
        config: MarketNeutralSweepConfig,
        variant: dict[str, float | int | str],
    ) -> MarketNeutralSweepResult:
        notes: list[str] = []
        if len(snapshots) <= int(variant["lookback_bars"]):
            notes.append("insufficient_snapshots")
            return self._empty_result(
                strategy="CrossVenueBasisSpread",
                variant_name=str(variant["name"]),
                cost_scenario=cost_scenario.name,
                symbol=symbol,
                primary_venue=primary_venue,
                secondary_venue=secondary_venue,
                notes=notes,
            )

        cost_usd = self._perp_perp_roundtrip_cost(cost_scenario, float(config.notional_usd))
        trades: list[SimulatedTrade] = []
        cursor = int(variant["lookback_bars"])
        while cursor < len(snapshots) - 1:
            signal_snapshot = snapshots[cursor]
            window = snapshots[cursor - int(variant["lookback_bars"]) : cursor]
            mean_spread, std_spread = self._mean_std(snapshot.spread_pct for snapshot in window)
            if std_spread <= 0:
                cursor += 1
                continue
            zscore = (signal_snapshot.spread_pct - mean_spread) / std_spread
            if abs(zscore) < float(variant["entry_z"]):
                cursor += 1
                continue
            if abs(signal_snapshot.spread_pct) < float(variant["min_abs_spread_pct"]):
                cursor += 1
                continue

            entry_cursor = cursor + 1
            if entry_cursor >= len(snapshots):
                break

            expected_edge_usd = abs(signal_snapshot.spread_pct - mean_spread) * float(config.notional_usd)
            if expected_edge_usd <= cost_usd + float(variant["min_expected_edge_usd"]):
                cursor += 1
                continue

            entry = snapshots[entry_cursor]
            side = -1.0 if signal_snapshot.spread_pct > mean_spread else 1.0
            exit_cursor = min(entry_cursor + int(variant["max_hold_bars"]), len(snapshots) - 1)
            exit_spread = snapshots[exit_cursor].spread_pct
            for future_cursor in range(entry_cursor + 1, min(entry_cursor + int(variant["max_hold_bars"]), len(snapshots) - 1) + 1):
                future_spread = snapshots[future_cursor].spread_pct
                future_zscore = abs((future_spread - mean_spread) / std_spread)
                if future_zscore <= float(variant["exit_z"]):
                    exit_cursor = min(future_cursor + 1, len(snapshots) - 1)
                    exit_spread = future_spread
                    break

            funding_cash, funding_intervals = self._sum_cross_venue_funding(
                primary_funding_events=primary_funding_events,
                secondary_funding_events=secondary_funding_events,
                entry_time=entry.ts,
                exit_time=snapshots[exit_cursor].ts,
                cross_venue_position_sign=side,
                notional_usd=float(config.notional_usd),
            )
            spread_pnl = side * (exit_spread - entry.spread_pct) * float(config.notional_usd)
            trades.append(
                SimulatedTrade(
                    net_pnl_usd=spread_pnl + funding_cash - cost_usd,
                    spread_pnl_usd=spread_pnl,
                    funding_pnl_usd=funding_cash,
                    cost_usd=cost_usd,
                    hold_bars=max(exit_cursor - entry_cursor, 0),
                    hold_funding_intervals=funding_intervals,
                )
            )
            cursor = exit_cursor + 1

        if not trades:
            notes.append("no_trades")
        return self._summarize_trades(
            strategy="CrossVenueBasisSpread",
            variant_name=str(variant["name"]),
            cost_scenario=cost_scenario.name,
            symbol=symbol,
            primary_venue=primary_venue,
            secondary_venue=secondary_venue,
            trades=trades,
            config=config,
            notes=notes,
        )

    def _load_symbol_venue_data(
        self,
        *,
        repository: FundingBasisRepository,
        symbol: str,
        venue: str,
        start_at: datetime,
        end_at: datetime,
        max_alignment_seconds: int,
    ) -> dict[str, Sequence[BasisSnapshot] | Sequence[FundingEvent]]:
        alignment_delta = timedelta(seconds=max_alignment_seconds)
        spot_rows = repository.list_spot_prices(
            self.spot_exchange,
            symbol,
            start_at - alignment_delta,
            end_at + alignment_delta,
        )
        perp_rows = repository.list_perp_prices(
            venue,
            symbol,
            start_at - alignment_delta,
            end_at + alignment_delta,
        )
        funding_rows = repository.list_funding_rates(
            venue,
            symbol,
            start_at - alignment_delta,
            end_at + alignment_delta,
        )

        snapshots: list[BasisSnapshot] = []
        for perp_row in perp_rows:
            spot_row, alignment = self._select_previous_snapshot(
                perp_row.ts,
                spot_rows,
                max_alignment_seconds=max_alignment_seconds,
                timestamp_getter=lambda item: item.ts,
            )
            if not alignment.matched or spot_row is None:
                continue
            spot_price = float(spot_row.mid or spot_row.close)
            perp_price = float(perp_row.mid or perp_row.mark_price)
            if spot_price <= 0:
                continue
            snapshots.append(
                BasisSnapshot(
                    ts=perp_row.ts,
                    basis_pct=(perp_price - spot_price) / spot_price,
                    spot_price=spot_price,
                    perp_price=perp_price,
                )
            )

        funding_events = [
            FundingEvent(ts=funding_row.funding_time, rate=float(funding_row.funding_rate))
            for funding_row in funding_rows
        ]
        return {"snapshots": snapshots, "funding_events": funding_events}

    def _build_cross_venue_snapshots(
        self,
        *,
        primary: Sequence[BasisSnapshot],
        secondary: Sequence[BasisSnapshot],
        max_alignment_seconds: int,
    ) -> list[CrossVenueSnapshot]:
        snapshots: list[CrossVenueSnapshot] = []
        for primary_snapshot in primary:
            secondary_snapshot, alignment = self._select_previous_snapshot(
                primary_snapshot.ts,
                secondary,
                max_alignment_seconds=max_alignment_seconds,
                timestamp_getter=lambda item: item.ts,
            )
            if not alignment.matched or secondary_snapshot is None:
                continue
            snapshots.append(
                CrossVenueSnapshot(
                    ts=primary_snapshot.ts,
                    spread_pct=primary_snapshot.basis_pct - secondary_snapshot.basis_pct,
                    primary_basis_pct=primary_snapshot.basis_pct,
                    secondary_basis_pct=secondary_snapshot.basis_pct,
                )
            )
        return snapshots

    def _align_funding_to_basis_snapshots(
        self,
        *,
        funding_events: Sequence[FundingEvent],
        snapshots: Sequence[BasisSnapshot],
        max_alignment_seconds: int,
    ) -> list[dict[str, float | int | datetime]]:
        aligned: list[dict[str, float | int | datetime]] = []
        for funding_event in funding_events:
            snapshot, alignment = self._select_previous_snapshot(
                funding_event.ts,
                snapshots,
                max_alignment_seconds=max_alignment_seconds,
                timestamp_getter=lambda item: item.ts,
            )
            if not alignment.matched or snapshot is None:
                continue
            snapshot_index = next(
                (index for index, candidate in enumerate(snapshots) if candidate.ts == snapshot.ts),
                None,
            )
            if snapshot_index is None:
                continue
            aligned.append(
                {
                    "funding_time": funding_event.ts,
                    "funding_rate": funding_event.rate,
                    "basis_pct": snapshot.basis_pct,
                    "snapshot_index": snapshot_index,
                }
            )
        return aligned

    def _sum_spot_perp_funding(
        self,
        *,
        funding_events: Sequence[FundingEvent],
        entry_time: datetime,
        exit_time: datetime,
        perp_position_sign: float,
        notional_usd: float,
    ) -> tuple[float, int]:
        relevant_events = [event for event in funding_events if entry_time < event.ts <= exit_time]
        funding_cash = sum(-perp_position_sign * event.rate * notional_usd for event in relevant_events)
        return funding_cash, len(relevant_events)

    def _sum_cross_venue_funding(
        self,
        *,
        primary_funding_events: Sequence[FundingEvent],
        secondary_funding_events: Sequence[FundingEvent],
        entry_time: datetime,
        exit_time: datetime,
        cross_venue_position_sign: float,
        notional_usd: float,
    ) -> tuple[float, int]:
        primary_events = [event for event in primary_funding_events if entry_time < event.ts <= exit_time]
        secondary_events = [event for event in secondary_funding_events if entry_time < event.ts <= exit_time]
        funding_cash = sum(
            -cross_venue_position_sign * event.rate * notional_usd for event in primary_events
        ) + sum(
            cross_venue_position_sign * event.rate * notional_usd for event in secondary_events
        )
        return funding_cash, max(len(primary_events), len(secondary_events))

    def _spot_perp_roundtrip_cost(self, scenario: MarketNeutralCostScenario, notional_usd: float) -> float:
        spot_cost_pct = float(
            scenario.spot_entry_fee_pct
            + scenario.spot_exit_fee_pct
            + scenario.spot_entry_slippage_pct
            + scenario.spot_exit_slippage_pct
        )
        perp_cost_pct = float(
            scenario.perp_entry_fee_pct
            + scenario.perp_exit_fee_pct
            + scenario.perp_entry_slippage_pct
            + scenario.perp_exit_slippage_pct
        )
        return notional_usd * (spot_cost_pct + perp_cost_pct)

    def _perp_perp_roundtrip_cost(self, scenario: MarketNeutralCostScenario, notional_usd: float) -> float:
        perp_leg_cost_pct = float(
            scenario.perp_entry_fee_pct
            + scenario.perp_exit_fee_pct
            + scenario.perp_entry_slippage_pct
            + scenario.perp_exit_slippage_pct
        )
        return notional_usd * perp_leg_cost_pct * 2.0

    def _summarize_trades(
        self,
        *,
        strategy: str,
        variant_name: str,
        cost_scenario: str,
        symbol: str,
        primary_venue: str,
        secondary_venue: Optional[str],
        trades: Sequence[SimulatedTrade],
        config: MarketNeutralSweepConfig,
        notes: list[str],
    ) -> MarketNeutralSweepResult:
        trade_count = len(trades)
        wins = sum(1 for trade in trades if trade.net_pnl_usd > 0)
        losses = sum(1 for trade in trades if trade.net_pnl_usd <= 0)
        total_net = sum(trade.net_pnl_usd for trade in trades)
        avg_net = total_net / trade_count if trade_count else 0.0
        avg_spread = sum(trade.spread_pnl_usd for trade in trades) / trade_count if trade_count else 0.0
        avg_funding = sum(trade.funding_pnl_usd for trade in trades) / trade_count if trade_count else 0.0
        avg_cost = sum(trade.cost_usd for trade in trades) / trade_count if trade_count else 0.0
        avg_hold_bars = sum(trade.hold_bars for trade in trades) / trade_count if trade_count else 0.0
        avg_hold_funding = (
            sum(trade.hold_funding_intervals for trade in trades) / trade_count if trade_count else 0.0
        )
        win_rate = wins / trade_count if trade_count else 0.0
        positive_share = win_rate

        looks_viable = (
            trade_count >= config.min_trades_for_viability
            and total_net > 0
            and avg_net > 0
            and Decimal(str(positive_share)) >= config.positive_share_threshold
        )
        screening_notes = list(notes)
        if trade_count < config.min_trades_for_viability:
            screening_notes.append("insufficient_trade_count")
        if total_net <= 0:
            screening_notes.append("total_net_non_positive")
        if avg_net <= 0:
            screening_notes.append("avg_net_non_positive")
        if Decimal(str(positive_share)) < config.positive_share_threshold:
            screening_notes.append("positive_share_below_threshold")

        return MarketNeutralSweepResult(
            strategy=strategy,  # type: ignore[arg-type]
            variant_name=variant_name,
            cost_scenario=cost_scenario,
            symbol=symbol,
            primary_venue=primary_venue,
            secondary_venue=secondary_venue,
            trades=trade_count,
            wins=wins,
            losses=losses,
            win_rate=self._quantize(win_rate),
            positive_share=self._quantize(positive_share),
            total_net_pnl_usd=self._quantize(total_net),
            avg_net_pnl_usd=self._quantize(avg_net),
            avg_spread_pnl_usd=self._quantize(avg_spread),
            avg_funding_pnl_usd=self._quantize(avg_funding),
            avg_cost_usd=self._quantize(avg_cost),
            avg_hold_bars=self._quantize(avg_hold_bars),
            avg_hold_funding_intervals=self._quantize(avg_hold_funding),
            looks_viable=looks_viable,
            screening_notes=screening_notes,
        )

    def _empty_result(
        self,
        *,
        strategy: str,
        variant_name: str,
        cost_scenario: str,
        symbol: str,
        primary_venue: str,
        secondary_venue: Optional[str],
        notes: list[str],
    ) -> MarketNeutralSweepResult:
        return MarketNeutralSweepResult(
            strategy=strategy,  # type: ignore[arg-type]
            variant_name=variant_name,
            cost_scenario=cost_scenario,
            symbol=symbol,
            primary_venue=primary_venue,
            secondary_venue=secondary_venue,
            trades=0,
            wins=0,
            losses=0,
            win_rate=self._quantize(0.0),
            positive_share=self._quantize(0.0),
            total_net_pnl_usd=self._quantize(0.0),
            avg_net_pnl_usd=self._quantize(0.0),
            avg_spread_pnl_usd=self._quantize(0.0),
            avg_funding_pnl_usd=self._quantize(0.0),
            avg_cost_usd=self._quantize(0.0),
            avg_hold_bars=self._quantize(0.0),
            avg_hold_funding_intervals=self._quantize(0.0),
            looks_viable=False,
            screening_notes=notes + ["insufficient_data"],
        )

    def _perp_premium_variants(self) -> list[dict[str, float | int | str]]:
        variants: list[dict[str, float | int | str]] = []
        for lookback_bars in (48, 96):
            for entry_z in (1.5, 2.0):
                for max_hold_bars in (24, 48):
                    name = f"lookback{lookback_bars}_z{entry_z}_hold{max_hold_bars}"
                    variants.append(
                        {
                            "name": name,
                            "lookback_bars": lookback_bars,
                            "entry_z": entry_z,
                            "exit_z": 0.5,
                            "max_hold_bars": max_hold_bars,
                            "min_abs_basis_pct": 0.0005,
                            "min_expected_edge_usd": 0.0,
                        }
                    )
        return variants

    def _funding_spike_variants(self) -> list[dict[str, float | int | str | bool]]:
        variants: list[dict[str, float | int | str | bool]] = []
        for min_abs_funding_rate in (0.0003, 0.0005, 0.0010):
            for hold_intervals in (1, 2, 3):
                for two_sided in (False, True):
                    name = (
                        f"funding{min_abs_funding_rate:.4f}_hold{hold_intervals}_"
                        f"{'two_sided' if two_sided else 'positive_only'}"
                    )
                    variants.append(
                        {
                            "name": name,
                            "min_abs_funding_rate": min_abs_funding_rate,
                            "min_abs_basis_pct": 0.0005,
                            "hold_intervals": hold_intervals,
                            "two_sided": two_sided,
                            "exit_on_normalization": True,
                            "normalize_basis_pct": 0.0002,
                        }
                    )
        return variants

    def _cross_venue_variants(self) -> list[dict[str, float | int | str]]:
        variants: list[dict[str, float | int | str]] = []
        for lookback_bars in (48, 96):
            for entry_z in (1.5, 2.0):
                for max_hold_bars in (24, 48):
                    name = f"spread{lookback_bars}_z{entry_z}_hold{max_hold_bars}"
                    variants.append(
                        {
                            "name": name,
                            "lookback_bars": lookback_bars,
                            "entry_z": entry_z,
                            "exit_z": 0.5,
                            "max_hold_bars": max_hold_bars,
                            "min_abs_spread_pct": 0.0003,
                            "min_expected_edge_usd": 0.0,
                        }
                    )
        return variants

    def _mean_std(self, values: Sequence[float] | Sequence[Decimal] | list[float]) -> tuple[float, float]:
        data = [float(value) for value in values]
        if not data:
            return 0.0, 0.0
        mean_value = sum(data) / len(data)
        variance = sum((item - mean_value) ** 2 for item in data) / len(data)
        return mean_value, sqrt(variance)

    def _is_better_result(
        self,
        candidate: MarketNeutralSweepResult,
        current: Optional[MarketNeutralSweepResult],
    ) -> bool:
        if current is None:
            return True
        return (
            candidate.looks_viable,
            candidate.total_net_pnl_usd,
            candidate.avg_net_pnl_usd,
            candidate.win_rate,
            candidate.trades,
        ) > (
            current.looks_viable,
            current.total_net_pnl_usd,
            current.avg_net_pnl_usd,
            current.win_rate,
            current.trades,
        )

    def _summarize_walk_forward(
        self,
        *,
        symbol: str,
        cost_scenario: str,
        windows: Sequence[PerpPremiumWalkForwardWindow],
    ) -> PerpPremiumWalkForwardSummary:
        evaluated = [window for window in windows if window.test_result is not None]
        positive = [
            window for window in evaluated if window.test_result is not None and window.test_result.total_net_pnl_usd > ZERO
        ]
        total_test_net = sum(
            (window.test_result.total_net_pnl_usd for window in evaluated if window.test_result is not None),
            ZERO,
        )
        total_test_trades = sum(
            window.test_result.trades for window in evaluated if window.test_result is not None
        )
        avg_test_net = total_test_net / Decimal(len(evaluated)) if evaluated else ZERO
        avg_test_win_rate = (
            sum((window.test_result.win_rate for window in evaluated if window.test_result is not None), ZERO)
            / Decimal(len(evaluated))
            if evaluated
            else ZERO
        )
        notes: list[str] = []
        looks_stable = True
        if not evaluated:
            looks_stable = False
            notes.append("no_evaluated_windows")
        if total_test_net <= ZERO:
            looks_stable = False
            notes.append("total_test_net_non_positive")
        if evaluated and Decimal(len(positive)) / Decimal(len(evaluated)) < Decimal("0.5"):
            looks_stable = False
            notes.append("positive_window_share_below_half")
        if total_test_trades == 0:
            looks_stable = False
            notes.append("no_test_trades")

        return PerpPremiumWalkForwardSummary(
            symbol=symbol,
            cost_scenario=cost_scenario,
            windows=list(windows),
            evaluated_windows=len(evaluated),
            positive_windows=len(positive),
            total_test_net_pnl_usd=total_test_net,
            avg_test_net_pnl_usd=self._quantize(float(avg_test_net)) if evaluated else self._quantize(0.0),
            total_test_trades=total_test_trades,
            avg_test_win_rate=self._quantize(float(avg_test_win_rate)) if evaluated else self._quantize(0.0),
            looks_stable=looks_stable,
            screening_notes=notes,
        )

    def _select_previous_snapshot(
        self,
        target_time: datetime,
        snapshots,
        *,
        max_alignment_seconds: int,
        timestamp_getter,
    ):
        if not snapshots:
            return None, SnapshotAlignmentResult(matched=False, distance_seconds=None)
        target = ensure_utc(target_time)
        timestamps = [ensure_utc(timestamp_getter(item)) for item in snapshots]
        cursor = bisect_right(timestamps, target) - 1
        if cursor < 0:
            return None, SnapshotAlignmentResult(matched=False, distance_seconds=None)
        candidate = snapshots[cursor]
        distance_seconds = int((target - ensure_utc(timestamp_getter(candidate))).total_seconds())
        if distance_seconds > max_alignment_seconds:
            return None, SnapshotAlignmentResult(matched=False, distance_seconds=distance_seconds)
        return candidate, SnapshotAlignmentResult(matched=True, distance_seconds=distance_seconds)

    def _quantize(self, value: float) -> Decimal:
        return Decimal(str(value)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
