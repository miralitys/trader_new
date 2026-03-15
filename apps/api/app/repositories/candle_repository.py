from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable, Optional

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from app.core.logging import get_logger
from app.integrations.binance_us import BinanceUSTimeframe, NormalizedCandle
from app.models import Candle, Exchange, Symbol, Timeframe
from app.repositories.base import BaseRepository
from app.utils.time import ensure_utc

logger = get_logger(__name__)


@dataclass(frozen=True)
class CandleCoverageSummary:
    exchange_code: str
    symbol_code: str
    timeframe: str
    requested_start_at: Optional[datetime]
    requested_end_at: Optional[datetime]
    actual_start_at: Optional[datetime]
    actual_end_at: Optional[datetime]
    candle_count: int
    expected_candle_count: int
    missing_candle_count: int
    completion_pct: Decimal


def estimate_expected_candle_count(
    timeframe: str,
    start_at: Optional[datetime],
    end_at: Optional[datetime],
) -> int:
    if start_at is None or end_at is None:
        return 0

    timeframe_value = BinanceUSTimeframe.from_code(timeframe)
    normalized_start = int(ensure_utc(start_at).timestamp())
    normalized_end = int(ensure_utc(end_at).timestamp())
    if normalized_end < normalized_start:
        return 0

    step_seconds = timeframe_value.granularity_seconds
    aligned_start = ((normalized_start + step_seconds - 1) // step_seconds) * step_seconds
    aligned_end = (normalized_end // step_seconds) * step_seconds
    if aligned_end < aligned_start:
        return 0

    return ((aligned_end - aligned_start) // step_seconds) + 1


def prepare_candle_upsert_rows(
    exchange_id: int,
    symbol_id: int,
    timeframe: str,
    candles: Iterable[NormalizedCandle],
) -> list[dict[str, object]]:
    deduped_rows: dict[datetime, dict[str, object]] = {}

    for candle in candles:
        deduped_rows[candle.open_time] = {
            "exchange_id": exchange_id,
            "symbol_id": symbol_id,
            "timeframe": timeframe,
            "open_time": candle.open_time,
            "open": candle.open,
            "high": candle.high,
            "low": candle.low,
            "close": candle.close,
            "volume": candle.volume,
        }

    return [deduped_rows[key] for key in sorted(deduped_rows)]


class CandleRepository(BaseRepository):
    def get_exchange(self, code: str) -> Optional[Exchange]:
        return self.session.scalar(select(Exchange).where(Exchange.code == code))

    def get_symbol(self, exchange_id: int, symbol_code: str) -> Optional[Symbol]:
        return self.session.scalar(
            select(Symbol).where(Symbol.exchange_id == exchange_id, Symbol.code == symbol_code)
        )

    def ensure_exchange(self, code: str, name: Optional[str] = None) -> Exchange:
        exchange = self.session.scalar(select(Exchange).where(Exchange.code == code))
        if exchange is not None:
            return exchange

        stmt = (
            insert(Exchange)
            .values(
                code=code,
                name=name or code.title(),
                description=f"{code.title()} exchange reference record.",
                is_active=True,
            )
            .on_conflict_do_nothing(index_elements=["code"])
            .returning(Exchange.id)
        )
        inserted_id = self.session.scalar(stmt)
        if inserted_id is not None:
            return self.session.get(Exchange, inserted_id)

        exchange = self.session.scalar(select(Exchange).where(Exchange.code == code))
        if exchange is None:
            raise ValueError(f"Exchange {code} could not be resolved")
        return exchange

    def ensure_timeframe(self, timeframe: str) -> Timeframe:
        timeframe_row = self.session.scalar(select(Timeframe).where(Timeframe.code == timeframe))
        if timeframe_row is not None:
            return timeframe_row

        timeframe_value = BinanceUSTimeframe.from_code(timeframe)
        stmt = (
            insert(Timeframe)
            .values(
                code=timeframe_value.value,
                name=timeframe_value.display_name,
                duration_seconds=timeframe_value.granularity_seconds,
                is_active=True,
            )
            .on_conflict_do_nothing(index_elements=["code"])
            .returning(Timeframe.id)
        )
        inserted_id = self.session.scalar(stmt)
        if inserted_id is not None:
            return self.session.get(Timeframe, inserted_id)

        timeframe_row = self.session.scalar(select(Timeframe).where(Timeframe.code == timeframe))
        if timeframe_row is None:
            raise ValueError(f"Timeframe {timeframe} could not be resolved")
        return timeframe_row

    def ensure_symbol(self, exchange_id: int, symbol_code: str) -> Symbol:
        symbol = self.session.scalar(
            select(Symbol).where(Symbol.exchange_id == exchange_id, Symbol.code == symbol_code)
        )
        if symbol is not None:
            return symbol

        if "-" not in symbol_code:
            raise ValueError(f"Symbol {symbol_code} must use BASE-QUOTE format")

        base_asset, quote_asset = symbol_code.split("-", 1)
        stmt = (
            insert(Symbol)
            .values(
                exchange_id=exchange_id,
                code=symbol_code,
                base_asset=base_asset,
                quote_asset=quote_asset,
                price_precision=2,
                qty_precision=8,
                is_active=True,
            )
            .on_conflict_do_nothing(index_elements=["exchange_id", "code"])
            .returning(Symbol.id)
        )
        inserted_id = self.session.scalar(stmt)
        if inserted_id is not None:
            return self.session.get(Symbol, inserted_id)

        symbol = self.session.scalar(
            select(Symbol).where(Symbol.exchange_id == exchange_id, Symbol.code == symbol_code)
        )
        if symbol is None:
            raise ValueError(f"Symbol {symbol_code} could not be resolved")
        return symbol

    def get_last_candle_open_time(
        self,
        exchange_id: int,
        symbol_id: int,
        timeframe: str,
    ) -> Optional[datetime]:
        stmt = select(func.max(Candle.open_time)).where(
            Candle.exchange_id == exchange_id,
            Candle.symbol_id == symbol_id,
            Candle.timeframe == timeframe,
        )
        return self.session.scalar(stmt)

    def list_candles(
        self,
        exchange_code: str,
        symbol_code: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
        limit: Optional[int] = None,
    ) -> list[Candle]:
        exchange = self.get_exchange(exchange_code)
        if exchange is None:
            return []

        symbol = self.get_symbol(exchange.id, symbol_code)
        if symbol is None:
            return []

        stmt = (
            select(Candle)
            .where(
                Candle.exchange_id == exchange.id,
                Candle.symbol_id == symbol.id,
                Candle.timeframe == timeframe,
                Candle.open_time >= start_at,
                Candle.open_time <= end_at,
            )
            .order_by(Candle.open_time.asc())
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        return list(self.session.scalars(stmt))

    def list_candles_after(
        self,
        exchange_code: str,
        symbol_code: str,
        timeframe: str,
        after_time: Optional[datetime],
        limit: Optional[int] = None,
    ) -> list[Candle]:
        exchange = self.get_exchange(exchange_code)
        if exchange is None:
            return []

        symbol = self.get_symbol(exchange.id, symbol_code)
        if symbol is None:
            return []

        stmt = (
            select(Candle)
            .where(
                Candle.exchange_id == exchange.id,
                Candle.symbol_id == symbol.id,
                Candle.timeframe == timeframe,
            )
            .order_by(Candle.open_time.asc())
        )
        if after_time is not None:
            stmt = stmt.where(Candle.open_time > after_time)
        if limit is not None:
            stmt = stmt.limit(limit)
        return list(self.session.scalars(stmt))

    def list_recent_candles(
        self,
        exchange_code: str,
        symbol_code: str,
        timeframe: str,
        end_at: Optional[datetime],
        limit: int,
    ) -> list[Candle]:
        if limit <= 0:
            return []

        exchange = self.get_exchange(exchange_code)
        if exchange is None:
            return []

        symbol = self.get_symbol(exchange.id, symbol_code)
        if symbol is None:
            return []

        stmt = (
            select(Candle)
            .where(
                Candle.exchange_id == exchange.id,
                Candle.symbol_id == symbol.id,
                Candle.timeframe == timeframe,
            )
            .order_by(Candle.open_time.desc())
            .limit(limit)
        )
        if end_at is not None:
            stmt = stmt.where(Candle.open_time <= end_at)
        return list(reversed(list(self.session.scalars(stmt))))

    def get_candle_coverage(
        self,
        exchange_code: str,
        symbol_code: str,
        timeframe: str,
        start_at: Optional[datetime] = None,
        end_at: Optional[datetime] = None,
    ) -> CandleCoverageSummary:
        expected_candle_count = estimate_expected_candle_count(timeframe, start_at, end_at)

        exchange = self.get_exchange(exchange_code)
        if exchange is None:
            return CandleCoverageSummary(
                exchange_code=exchange_code,
                symbol_code=symbol_code,
                timeframe=timeframe,
                requested_start_at=start_at,
                requested_end_at=end_at,
                actual_start_at=None,
                actual_end_at=None,
                candle_count=0,
                expected_candle_count=expected_candle_count,
                missing_candle_count=expected_candle_count,
                completion_pct=Decimal("0"),
            )

        symbol = self.get_symbol(exchange.id, symbol_code)
        if symbol is None:
            return CandleCoverageSummary(
                exchange_code=exchange_code,
                symbol_code=symbol_code,
                timeframe=timeframe,
                requested_start_at=start_at,
                requested_end_at=end_at,
                actual_start_at=None,
                actual_end_at=None,
                candle_count=0,
                expected_candle_count=expected_candle_count,
                missing_candle_count=expected_candle_count,
                completion_pct=Decimal("0"),
            )

        stmt = select(
            func.count(Candle.id),
            func.min(Candle.open_time),
            func.max(Candle.open_time),
        ).where(
            Candle.exchange_id == exchange.id,
            Candle.symbol_id == symbol.id,
            Candle.timeframe == timeframe,
        )
        if start_at is not None:
            stmt = stmt.where(Candle.open_time >= start_at)
        if end_at is not None:
            stmt = stmt.where(Candle.open_time <= end_at)

        candle_count, actual_start_at, actual_end_at = self.session.execute(stmt).one()
        candle_count = int(candle_count or 0)
        missing_candle_count = max(expected_candle_count - candle_count, 0)
        completion_pct = Decimal("0")
        if expected_candle_count > 0:
            completion_pct = (
                (Decimal(candle_count) * Decimal("100")) / Decimal(expected_candle_count)
            ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        return CandleCoverageSummary(
            exchange_code=exchange_code,
            symbol_code=symbol_code,
            timeframe=timeframe,
            requested_start_at=start_at,
            requested_end_at=end_at,
            actual_start_at=actual_start_at,
            actual_end_at=actual_end_at,
            candle_count=candle_count,
            expected_candle_count=expected_candle_count,
            missing_candle_count=missing_candle_count,
            completion_pct=completion_pct,
        )

    def upsert_candles(
        self,
        exchange_id: int,
        symbol_id: int,
        timeframe: str,
        candles: Iterable[NormalizedCandle],
    ) -> int:
        rows = prepare_candle_upsert_rows(
            exchange_id=exchange_id,
            symbol_id=symbol_id,
            timeframe=timeframe,
            candles=candles,
        )
        if not rows:
            return 0

        stmt = (
            insert(Candle)
            .values(rows)
            .on_conflict_do_nothing(
                index_elements=["exchange_id", "symbol_id", "timeframe", "open_time"]
            )
            .returning(Candle.id)
        )
        inserted_ids = list(self.session.execute(stmt).scalars())
        inserted_count = len(inserted_ids)
        logger.info(
            "Upserted candle batch",
            extra={
                "exchange_id": exchange_id,
                "symbol_id": symbol_id,
                "timeframe": timeframe,
                "received_rows": len(rows),
                "inserted_rows": inserted_count,
            },
        )
        return inserted_count
