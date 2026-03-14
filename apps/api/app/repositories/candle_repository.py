from __future__ import annotations

from datetime import datetime
from typing import Iterable, Optional

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from app.core.logging import get_logger
from app.integrations.coinbase.schemas import CoinbaseTimeframe, NormalizedCandle
from app.models import Candle, Exchange, Symbol, Timeframe
from app.repositories.base import BaseRepository

logger = get_logger(__name__)


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

        timeframe_value = CoinbaseTimeframe.from_code(timeframe)
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
