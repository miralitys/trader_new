from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Iterable, Optional, Sequence

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.models import FeeSchedule, FundingRate, PerpPrice, SpotPrice
from app.repositories.base import BaseRepository


@dataclass(frozen=True)
class SpotPriceRow:
    exchange: str
    symbol: str
    ts: datetime
    bid: Optional[Decimal]
    ask: Optional[Decimal]
    mid: Decimal
    close: Decimal
    volume: Decimal


@dataclass(frozen=True)
class PerpPriceRow:
    exchange: str
    symbol: str
    ts: datetime
    mark_price: Decimal
    index_price: Decimal
    bid: Optional[Decimal]
    ask: Optional[Decimal]
    mid: Decimal
    open_interest: Optional[Decimal]
    volume: Optional[Decimal]


@dataclass(frozen=True)
class FundingRateRow:
    exchange: str
    symbol: str
    funding_time: datetime
    funding_rate: Decimal
    realized_funding_rate: Optional[Decimal]


@dataclass(frozen=True)
class FeeScheduleRow:
    venue: str
    product_type: str
    maker_fee_pct: Decimal
    taker_fee_pct: Decimal
    effective_from: datetime


class FundingBasisRepository(BaseRepository):
    SPOT_UPSERT_CHUNK_SIZE = 1000
    PERP_UPSERT_CHUNK_SIZE = 1000
    FUNDING_UPSERT_CHUNK_SIZE = 2000

    def upsert_spot_prices(self, rows: Iterable[SpotPriceRow]) -> int:
        payload = [
            {
                "exchange": row.exchange,
                "symbol": row.symbol,
                "ts": row.ts,
                "bid": row.bid,
                "ask": row.ask,
                "mid": row.mid,
                "close": row.close,
                "volume": row.volume,
            }
            for row in rows
        ]
        if not payload:
            return 0
        return self._upsert_spot_price_payload(payload)

    def upsert_perp_prices(self, rows: Iterable[PerpPriceRow]) -> int:
        payload = [
            {
                "exchange": row.exchange,
                "symbol": row.symbol,
                "ts": row.ts,
                "mark_price": row.mark_price,
                "index_price": row.index_price,
                "bid": row.bid,
                "ask": row.ask,
                "mid": row.mid,
                "open_interest": row.open_interest,
                "volume": row.volume,
            }
            for row in rows
        ]
        if not payload:
            return 0
        return self._upsert_perp_price_payload(payload)

    def upsert_funding_rates(self, rows: Iterable[FundingRateRow]) -> int:
        payload = [
            {
                "exchange": row.exchange,
                "symbol": row.symbol,
                "funding_time": row.funding_time,
                "funding_rate": row.funding_rate,
                "realized_funding_rate": row.realized_funding_rate,
            }
            for row in rows
        ]
        if not payload:
            return 0
        return self._upsert_funding_rate_payload(payload)

    def upsert_fee_schedules(self, rows: Iterable[FeeScheduleRow]) -> int:
        payload = [
            {
                "venue": row.venue,
                "product_type": row.product_type,
                "maker_fee_pct": row.maker_fee_pct,
                "taker_fee_pct": row.taker_fee_pct,
                "effective_from": row.effective_from,
            }
            for row in rows
        ]
        if not payload:
            return 0
        stmt = insert(FeeSchedule).values(payload)
        stmt = stmt.on_conflict_do_update(
            index_elements=["venue", "product_type", "effective_from"],
            set_={
                "maker_fee_pct": stmt.excluded.maker_fee_pct,
                "taker_fee_pct": stmt.excluded.taker_fee_pct,
            },
        ).returning(FeeSchedule.id)
        return len(list(self.session.execute(stmt).scalars()))

    def _upsert_spot_price_payload(self, payload: Sequence[dict[str, object]]) -> int:
        total = 0
        for chunk in self._chunk_payload(payload, self.SPOT_UPSERT_CHUNK_SIZE):
            stmt = insert(SpotPrice).values(list(chunk))
            stmt = stmt.on_conflict_do_update(
                index_elements=["exchange", "symbol", "ts"],
                set_={
                    "bid": stmt.excluded.bid,
                    "ask": stmt.excluded.ask,
                    "mid": stmt.excluded.mid,
                    "close": stmt.excluded.close,
                    "volume": stmt.excluded.volume,
                },
            ).returning(SpotPrice.id)
            total += len(list(self.session.execute(stmt).scalars()))
        return total

    def _upsert_perp_price_payload(self, payload: Sequence[dict[str, object]]) -> int:
        total = 0
        for chunk in self._chunk_payload(payload, self.PERP_UPSERT_CHUNK_SIZE):
            stmt = insert(PerpPrice).values(list(chunk))
            stmt = stmt.on_conflict_do_update(
                index_elements=["exchange", "symbol", "ts"],
                set_={
                    "mark_price": stmt.excluded.mark_price,
                    "index_price": stmt.excluded.index_price,
                    "bid": stmt.excluded.bid,
                    "ask": stmt.excluded.ask,
                    "mid": stmt.excluded.mid,
                    "open_interest": stmt.excluded.open_interest,
                    "volume": stmt.excluded.volume,
                },
            ).returning(PerpPrice.id)
            total += len(list(self.session.execute(stmt).scalars()))
        return total

    def _upsert_funding_rate_payload(self, payload: Sequence[dict[str, object]]) -> int:
        total = 0
        for chunk in self._chunk_payload(payload, self.FUNDING_UPSERT_CHUNK_SIZE):
            stmt = insert(FundingRate).values(list(chunk))
            stmt = stmt.on_conflict_do_update(
                index_elements=["exchange", "symbol", "funding_time"],
                set_={
                    "funding_rate": stmt.excluded.funding_rate,
                    "realized_funding_rate": stmt.excluded.realized_funding_rate,
                },
            ).returning(FundingRate.id)
            total += len(list(self.session.execute(stmt).scalars()))
        return total

    @staticmethod
    def _chunk_payload(
        payload: Sequence[dict[str, object]],
        chunk_size: int,
    ) -> Iterable[Sequence[dict[str, object]]]:
        for index in range(0, len(payload), chunk_size):
            yield payload[index : index + chunk_size]

    def get_last_spot_ts(self, exchange: str, symbol: str) -> Optional[datetime]:
        stmt = (
            select(SpotPrice.ts)
            .where(SpotPrice.exchange == exchange, SpotPrice.symbol == symbol)
            .order_by(SpotPrice.ts.desc())
            .limit(1)
        )
        return self.session.scalar(stmt)

    def get_last_perp_ts(self, exchange: str, symbol: str) -> Optional[datetime]:
        stmt = (
            select(PerpPrice.ts)
            .where(PerpPrice.exchange == exchange, PerpPrice.symbol == symbol)
            .order_by(PerpPrice.ts.desc())
            .limit(1)
        )
        return self.session.scalar(stmt)

    def get_last_funding_time(self, exchange: str, symbol: str) -> Optional[datetime]:
        stmt = (
            select(FundingRate.funding_time)
            .where(FundingRate.exchange == exchange, FundingRate.symbol == symbol)
            .order_by(FundingRate.funding_time.desc())
            .limit(1)
        )
        return self.session.scalar(stmt)

    def list_spot_prices(
        self,
        exchange: str,
        symbol: str,
        start_at: datetime,
        end_at: datetime,
    ) -> list[SpotPrice]:
        stmt = (
            select(SpotPrice)
            .where(
                SpotPrice.exchange == exchange,
                SpotPrice.symbol == symbol,
                SpotPrice.ts >= start_at,
                SpotPrice.ts <= end_at,
            )
            .order_by(SpotPrice.ts.asc())
        )
        return list(self.session.scalars(stmt))

    def list_perp_prices(
        self,
        exchange: str,
        symbol: str,
        start_at: datetime,
        end_at: datetime,
    ) -> list[PerpPrice]:
        stmt = (
            select(PerpPrice)
            .where(
                PerpPrice.exchange == exchange,
                PerpPrice.symbol == symbol,
                PerpPrice.ts >= start_at,
                PerpPrice.ts <= end_at,
            )
            .order_by(PerpPrice.ts.asc())
        )
        return list(self.session.scalars(stmt))

    def list_funding_rates(
        self,
        exchange: str,
        symbol: str,
        start_at: datetime,
        end_at: datetime,
    ) -> list[FundingRate]:
        stmt = (
            select(FundingRate)
            .where(
                FundingRate.exchange == exchange,
                FundingRate.symbol == symbol,
                FundingRate.funding_time >= start_at,
                FundingRate.funding_time <= end_at,
            )
            .order_by(FundingRate.funding_time.asc())
        )
        return list(self.session.scalars(stmt))
