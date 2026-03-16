from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable, Optional, Sequence

from app.db.session import session_scope
from app.models import FundingRate, PerpPrice, SpotPrice
from app.repositories.funding_basis_repository import FundingBasisRepository
from app.schemas.funding_basis import (
    FundingBasisAssetReport,
    FundingBasisObservation,
    FundingBasisResearchConfig,
    FundingBasisResearchReport,
)
from app.utils.research_symbols import normalize_research_symbol, select_nearest_snapshot
from app.utils.time import ensure_utc, utc_now

ZERO = Decimal("0")
ONE_HUNDRED = Decimal("100")


class FundingBasisResearchService:
    SUPPORTED_PERP_VENUES = {"binance_futures", "okx_swap"}

    def __init__(
        self,
        repository: Optional[FundingBasisRepository] = None,
        *,
        spot_exchange: str = "binance_spot",
        perp_exchange: str = "binance_futures",
    ) -> None:
        if perp_exchange not in self.SUPPORTED_PERP_VENUES:
            raise ValueError(f"Unsupported perp exchange: {perp_exchange}")
        self.repository = repository
        self.spot_exchange = spot_exchange
        self.perp_exchange = perp_exchange

    def build_report(
        self,
        *,
        symbols: list[str],
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
        config: Optional[FundingBasisResearchConfig] = None,
    ) -> FundingBasisResearchReport:
        effective_config = config or FundingBasisResearchConfig()
        assets: list[FundingBasisAssetReport] = []
        observations: dict[str, list[FundingBasisObservation]] = {}

        with session_scope() as session:
            repository = self.repository or FundingBasisRepository(session)
            for symbol in symbols:
                normalized_symbol = normalize_research_symbol(symbol)
                observation_rows = self._build_observations_for_symbol(
                    repository=repository,
                    symbol=normalized_symbol,
                    start_at=start_at,
                    end_at=end_at,
                    config=effective_config,
                )
                observations[normalized_symbol] = observation_rows
                assets.append(
                    self._summarize_symbol(
                        symbol=normalized_symbol,
                        observations=observation_rows,
                        total_funding_observations=len(
                            repository.list_funding_rates(
                                exchange=self.perp_exchange,
                                symbol=normalized_symbol,
                                start_at=start_at,
                                end_at=end_at,
                            )
                        ),
                        config=effective_config,
                    )
                )

        return FundingBasisResearchReport(
            generated_at=utc_now(),
            spot_exchange=self.spot_exchange,
            perp_exchange=self.perp_exchange,
            timeframe=timeframe,
            start_at=ensure_utc(start_at),
            end_at=ensure_utc(end_at),
            config=effective_config,
            assets=assets,
            observations=observations,
        )

    def build_symbol_report_from_rows(
        self,
        *,
        symbol: str,
        funding_rows: Sequence[FundingRate],
        spot_rows: Sequence[SpotPrice],
        perp_rows: Sequence[PerpPrice],
        config: FundingBasisResearchConfig,
    ) -> tuple[list[FundingBasisObservation], FundingBasisAssetReport]:
        observations = self._align_observations(
            symbol=symbol,
            funding_rows=funding_rows,
            spot_rows=spot_rows,
            perp_rows=perp_rows,
            config=config,
        )
        summary = self._summarize_symbol(
            symbol=symbol,
            observations=observations,
            total_funding_observations=len(funding_rows),
            config=config,
        )
        return observations, summary

    def _build_observations_for_symbol(
        self,
        repository: FundingBasisRepository,
        symbol: str,
        start_at: datetime,
        end_at: datetime,
        config: FundingBasisResearchConfig,
    ) -> list[FundingBasisObservation]:
        alignment_delta = timedelta(seconds=config.max_snapshot_alignment_seconds)
        funding_rows = repository.list_funding_rates(self.perp_exchange, symbol, start_at, end_at)
        spot_rows = repository.list_spot_prices(
            self.spot_exchange,
            symbol,
            start_at - alignment_delta,
            end_at + alignment_delta,
        )
        perp_rows = repository.list_perp_prices(
            self.perp_exchange,
            symbol,
            start_at - alignment_delta,
            end_at + alignment_delta,
        )
        return self._align_observations(
            symbol=symbol,
            funding_rows=funding_rows,
            spot_rows=spot_rows,
            perp_rows=perp_rows,
            config=config,
        )

    def _align_observations(
        self,
        *,
        symbol: str,
        funding_rows: Sequence[FundingRate],
        spot_rows: Sequence[SpotPrice],
        perp_rows: Sequence[PerpPrice],
        config: FundingBasisResearchConfig,
    ) -> list[FundingBasisObservation]:
        observations: list[FundingBasisObservation] = []
        for funding_row in funding_rows:
            spot_row, spot_alignment = select_nearest_snapshot(
                funding_row.funding_time,
                spot_rows,
                max_alignment_seconds=config.max_snapshot_alignment_seconds,
                timestamp_getter=lambda item: item.ts,
            )
            perp_row, perp_alignment = select_nearest_snapshot(
                funding_row.funding_time,
                perp_rows,
                max_alignment_seconds=config.max_snapshot_alignment_seconds,
                timestamp_getter=lambda item: item.ts,
            )
            if not spot_alignment.matched or not perp_alignment.matched or spot_row is None or perp_row is None:
                continue

            spot_reference = Decimal(str(spot_row.mid or spot_row.close))
            perp_reference = Decimal(str(perp_row.mid or perp_row.mark_price))
            if spot_reference <= ZERO:
                continue

            basis_pct = (perp_reference - spot_reference) / spot_reference
            gross_funding_carry = config.notional_usd * Decimal(str(funding_row.funding_rate))
            entry_spot_fee = config.notional_usd * config.spot_fee_pct
            entry_perp_fee = config.notional_usd * config.perp_fee_pct
            exit_spot_fee = config.notional_usd * config.spot_fee_pct
            exit_perp_fee = config.notional_usd * config.perp_fee_pct
            total_slippage = config.notional_usd * config.slippage_pct * Decimal("4")
            expected_net = (
                gross_funding_carry
                - entry_spot_fee
                - entry_perp_fee
                - exit_spot_fee
                - exit_perp_fee
                - total_slippage
            )
            observations.append(
                FundingBasisObservation(
                    symbol=symbol,
                    funding_time=funding_row.funding_time,
                    spot_reference_price=spot_reference,
                    perp_reference_price=perp_reference,
                    basis_pct=self._quantize(basis_pct),
                    funding_rate=self._quantize(Decimal(str(funding_row.funding_rate))),
                    expected_gross_funding_carry=self._quantize(gross_funding_carry),
                    entry_spot_fee=self._quantize(entry_spot_fee),
                    entry_perp_fee=self._quantize(entry_perp_fee),
                    exit_spot_fee=self._quantize(exit_spot_fee),
                    exit_perp_fee=self._quantize(exit_perp_fee),
                    total_slippage=self._quantize(total_slippage),
                    expected_net_carry=self._quantize(expected_net),
                )
            )
        return observations

    def _summarize_symbol(
        self,
        *,
        symbol: str,
        observations: Sequence[FundingBasisObservation],
        total_funding_observations: int,
        config: FundingBasisResearchConfig,
    ) -> FundingBasisAssetReport:
        aligned_observations = len(observations)
        insufficient_alignment_observations = max(total_funding_observations - aligned_observations, 0)
        avg_funding_rate = self._mean(observation.funding_rate for observation in observations)
        avg_basis_pct = self._mean(observation.basis_pct for observation in observations)
        avg_gross = self._mean(observation.expected_gross_funding_carry for observation in observations)
        avg_net = self._mean(observation.expected_net_carry for observation in observations)
        threshold_hits = sum(1 for observation in observations if observation.funding_rate >= config.min_funding_rate)
        threshold_share = ZERO
        if aligned_observations:
            threshold_share = Decimal(threshold_hits) / Decimal(aligned_observations)

        notes: list[str] = []
        looks_viable = True
        if aligned_observations == 0:
            looks_viable = False
            notes.append("insufficient_aligned_observations")
        if avg_funding_rate < config.min_funding_rate:
            looks_viable = False
            notes.append("avg_funding_rate_below_threshold")
        if avg_basis_pct < config.min_basis_pct:
            looks_viable = False
            notes.append("avg_basis_pct_below_threshold")
        if avg_net <= ZERO:
            looks_viable = False
            notes.append("avg_net_carry_non_positive")

        return FundingBasisAssetReport(
            symbol=symbol,
            total_funding_observations=total_funding_observations,
            aligned_observations=aligned_observations,
            insufficient_alignment_observations=insufficient_alignment_observations,
            avg_funding_rate=self._quantize(avg_funding_rate),
            funding_above_threshold_share=self._quantize(threshold_share),
            avg_basis_pct=self._quantize(avg_basis_pct),
            avg_expected_gross_funding_carry=self._quantize(avg_gross),
            avg_expected_net_carry=self._quantize(avg_net),
            looks_viable=looks_viable,
            screening_notes=notes,
        )

    def _mean(self, values: Iterable[Decimal]) -> Decimal:
        items = list(values)
        if not items:
            return ZERO
        return sum(items, ZERO) / Decimal(len(items))

    def _quantize(self, value: Decimal) -> Decimal:
        return value.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
