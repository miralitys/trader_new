from __future__ import annotations

from datetime import datetime

from sqlalchemy import Select, desc, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.domain.models import Candle, Exchange, Symbol, SyncJob, Timeframe
from app.exchanges.base import NormalizedCandle


class MarketDataRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def get_exchange_by_slug(self, slug: str) -> Exchange | None:
        return self.db.scalar(select(Exchange).where(Exchange.slug == slug))

    def list_symbols(self, exchange_id: int | None = None) -> list[Symbol]:
        query: Select[tuple[Symbol]] = select(Symbol).order_by(Symbol.symbol)
        if exchange_id is not None:
            query = query.where(Symbol.exchange_id == exchange_id)
        return list(self.db.scalars(query))

    def get_symbol(self, exchange_id: int, symbol: str) -> Symbol | None:
        return self.db.scalar(
            select(Symbol).where(Symbol.exchange_id == exchange_id, Symbol.symbol == symbol.upper())
        )

    def get_symbol_by_id(self, symbol_id: int) -> Symbol | None:
        return self.db.get(Symbol, symbol_id)

    def list_timeframes(self) -> list[Timeframe]:
        return list(self.db.scalars(select(Timeframe).order_by(Timeframe.seconds)))

    def get_timeframe(self, code: str) -> Timeframe | None:
        return self.db.scalar(select(Timeframe).where(Timeframe.code == code))

    def get_timeframe_by_id(self, timeframe_id: int) -> Timeframe | None:
        return self.db.get(Timeframe, timeframe_id)

    def latest_candle_time(self, exchange_id: int, symbol_id: int, timeframe_id: int) -> datetime | None:
        return self.db.scalar(
            select(func.max(Candle.open_time)).where(
                Candle.exchange_id == exchange_id,
                Candle.symbol_id == symbol_id,
                Candle.timeframe_id == timeframe_id,
            )
        )

    def list_candles(
        self,
        symbol_id: int,
        timeframe_id: int,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> list[Candle]:
        query: Select[tuple[Candle]] = select(Candle).where(
            Candle.symbol_id == symbol_id,
            Candle.timeframe_id == timeframe_id,
        )
        if start is not None:
            query = query.where(Candle.open_time >= start)
        if end is not None:
            query = query.where(Candle.open_time <= end)
        query = query.order_by(Candle.open_time.asc())
        if limit is not None:
            query = query.limit(limit)
        return list(self.db.scalars(query))

    def list_recent_window(self, symbol_id: int, timeframe_id: int, end: datetime, limit: int) -> list[Candle]:
        query = (
            select(Candle)
            .where(Candle.symbol_id == symbol_id, Candle.timeframe_id == timeframe_id, Candle.open_time <= end)
            .order_by(Candle.open_time.desc())
            .limit(limit)
        )
        candles = list(self.db.scalars(query))
        candles.reverse()
        return candles

    def list_new_candles(
        self,
        symbol_id: int,
        timeframe_id: int,
        after: datetime | None,
    ) -> list[Candle]:
        query = select(Candle).where(Candle.symbol_id == symbol_id, Candle.timeframe_id == timeframe_id)
        if after is not None:
            query = query.where(Candle.open_time > after)
        query = query.order_by(Candle.open_time.asc())
        return list(self.db.scalars(query))

    def upsert_candles(
        self,
        exchange_id: int,
        symbol_id: int,
        timeframe_id: int,
        candles: list[NormalizedCandle],
    ) -> int:
        if not candles:
            return 0
        values = [
            {
                "exchange_id": exchange_id,
                "symbol_id": symbol_id,
                "timeframe_id": timeframe_id,
                "open_time": candle.open_time,
                "close_time": candle.close_time,
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": candle.volume,
                "trade_count": candle.trade_count,
                "source": "coinbase",
            }
            for candle in candles
        ]
        statement = insert(Candle).values(values)
        statement = statement.on_conflict_do_nothing(
            index_elements=["exchange_id", "symbol_id", "timeframe_id", "open_time"]
        )
        result = self.db.execute(statement)
        return result.rowcount or 0

    def create_sync_job(
        self,
        exchange_id: int,
        symbol_id: int,
        timeframe_id: int,
        job_type: str,
        start: datetime | None,
        end: datetime | None,
        metadata_json: dict | None = None,
    ) -> SyncJob:
        job = SyncJob(
            exchange_id=exchange_id,
            symbol_id=symbol_id,
            timeframe_id=timeframe_id,
            job_type=job_type,
            requested_start=start,
            requested_end=end,
            metadata_json=metadata_json or {},
        )
        self.db.add(job)
        self.db.flush()
        return job

    def update_sync_job(self, job: SyncJob, **fields: object) -> SyncJob:
        for key, value in fields.items():
            setattr(job, key, value)
        self.db.add(job)
        self.db.flush()
        return job

    def list_sync_jobs(self, limit: int = 50) -> list[SyncJob]:
        return list(self.db.scalars(select(SyncJob).order_by(desc(SyncJob.created_at)).limit(limit)))

    def queue_depth(self) -> int:
        return int(
            self.db.scalar(
                select(func.count(SyncJob.id)).where(SyncJob.status.in_(["queued", "running"]))
            )
            or 0
        )

    def last_sync_at(self) -> datetime | None:
        return self.db.scalar(select(func.max(SyncJob.finished_at)))

    def coverage_summary(self) -> list[dict]:
        query = (
            select(
                Symbol.symbol,
                Timeframe.code,
                func.min(Candle.open_time),
                func.max(Candle.open_time),
                func.count(Candle.id),
            )
            .join(Symbol, Candle.symbol_id == Symbol.id)
            .join(Timeframe, Candle.timeframe_id == Timeframe.id)
            .group_by(Symbol.symbol, Timeframe.code)
            .order_by(Symbol.symbol, Timeframe.seconds)
        )
        rows = self.db.execute(query).all()
        return [
            {
                "symbol": row[0],
                "timeframe": row[1],
                "first_candle": row[2],
                "last_candle": row[3],
                "candle_count": int(row[4]),
            }
            for row in rows
        ]
