from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from app.core.celery_app import celery_app
from app.domain.enums import SyncJobStatus
from app.domain.schemas.data import DataStatusRead
from app.exchanges.coinbase import CoinbaseAdapter
from app.repositories.market_data import MarketDataRepository
from app.repositories.trading import TradingRepository


class MarketDataService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.repo = MarketDataRepository(db)
        self.log_repo = TradingRepository(db)
        self.adapter = CoinbaseAdapter()

    def schedule_sync_jobs(
        self,
        *,
        symbols: list[str],
        timeframes: list[str],
        start: datetime | None,
        end: datetime | None,
        full_resync: bool,
    ) -> list[dict]:
        exchange = self.repo.get_exchange_by_slug("coinbase")
        if exchange is None:
            raise ValueError("coinbase exchange not seeded")

        jobs: list[dict] = []
        for symbol_code in symbols:
            symbol = self.repo.get_symbol(exchange.id, symbol_code)
            if symbol is None:
                continue
            for timeframe_code in timeframes:
                timeframe = self.repo.get_timeframe(timeframe_code)
                if timeframe is None:
                    continue
                job = self.repo.create_sync_job(
                    exchange_id=exchange.id,
                    symbol_id=symbol.id,
                    timeframe_id=timeframe.id,
                    job_type="full_resync" if full_resync else "sync",
                    start=start,
                    end=end,
                    metadata_json={"symbol": symbol_code, "timeframe": timeframe_code},
                )
                jobs.append({"id": job.id, "symbol": symbol_code, "timeframe": timeframe_code})
                celery_app.send_task(
                    "app.background.tasks.sync_symbol_timeframe",
                    args=[
                        job.id,
                        symbol_code,
                        timeframe_code,
                        start.isoformat() if start else None,
                        end.isoformat() if end else None,
                        full_resync,
                    ],
                )
        self.db.commit()
        return jobs

    def sync_history(
        self,
        *,
        job_id: int,
        symbol_code: str,
        timeframe_code: str,
        start: datetime | None,
        end: datetime | None,
        full_resync: bool = False,
    ) -> dict:
        exchange = self.repo.get_exchange_by_slug("coinbase")
        if exchange is None:
            raise ValueError("coinbase exchange not seeded")
        symbol = self.repo.get_symbol(exchange.id, symbol_code)
        timeframe = self.repo.get_timeframe(timeframe_code)
        if symbol is None or timeframe is None:
            raise ValueError(f"Unknown symbol/timeframe {symbol_code}/{timeframe_code}")

        job = next((item for item in self.repo.list_sync_jobs(limit=500) if item.id == job_id), None)
        if job is None:
            raise ValueError(f"sync job {job_id} not found")

        self.repo.update_sync_job(job, status=SyncJobStatus.RUNNING.value, started_at=datetime.now(timezone.utc))
        self.db.commit()

        sync_end = end or datetime.now(timezone.utc)
        latest = None if full_resync else self.repo.latest_candle_time(exchange.id, symbol.id, timeframe.id)
        sync_start = start or (
            latest + timedelta(seconds=timeframe.seconds)
            if latest is not None
            else sync_end - timedelta(days=30)
        )

        if sync_start >= sync_end:
            self.repo.update_sync_job(
                job,
                status=SyncJobStatus.COMPLETED.value,
                finished_at=datetime.now(timezone.utc),
                metadata_json={"inserted": 0, "reason": "up_to_date"},
            )
            self.db.commit()
            return {"inserted": 0, "gaps": 0}

        candles = self.adapter.fetch_candles(symbol.symbol, timeframe.code, sync_start, sync_end)
        inserted = self.repo.upsert_candles(exchange.id, symbol.id, timeframe.id, candles)
        gaps = self._count_gaps(candles, timeframe.seconds)
        self.repo.update_sync_job(
            job,
            status=SyncJobStatus.COMPLETED.value,
            finished_at=datetime.now(timezone.utc),
            metadata_json={
                "inserted": inserted,
                "fetched": len(candles),
                "gaps": gaps,
                "symbol": symbol.symbol,
                "timeframe": timeframe.code,
            },
        )
        self.log_repo.create_log(
            category="market_data",
            level="info",
            message=f"Synced {symbol.symbol} {timeframe.code}",
            symbol_id=symbol.id,
            context={"inserted": inserted, "fetched": len(candles), "gaps": gaps},
        )
        self.db.commit()
        return {"inserted": inserted, "gaps": gaps}

    def fail_job(self, job_id: int, error_message: str) -> None:
        job = next((item for item in self.repo.list_sync_jobs(limit=500) if item.id == job_id), None)
        if job is not None:
            self.repo.update_sync_job(
                job,
                status=SyncJobStatus.FAILED.value,
                finished_at=datetime.now(timezone.utc),
                error_message=error_message,
            )
            self.log_repo.create_log(
                category="market_data",
                level="error",
                message="Historical sync failed",
                symbol_id=job.symbol_id,
                context={"job_id": job_id, "error": error_message},
            )
            self.db.commit()

    def get_status(self) -> DataStatusRead:
        coverage = self.repo.coverage_summary()
        jobs = self.repo.list_sync_jobs(limit=25)
        queue_depth = self.repo.queue_depth()
        loader_status = "running" if queue_depth else "idle"
        return DataStatusRead(
            loader_status=loader_status,
            queue_depth=queue_depth,
            last_sync_at=self.repo.last_sync_at(),
            coverage=coverage,
            jobs=jobs,
        )

    def _count_gaps(self, candles, expected_seconds: int) -> int:
        if len(candles) < 2:
            return 0
        gaps = 0
        for previous, current in zip(candles, candles[1:]):
            if int((current.open_time - previous.open_time).total_seconds()) != expected_seconds:
                gaps += 1
        return gaps
