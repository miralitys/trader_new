from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from math import ceil
from typing import Callable, Optional

from sqlalchemy import func, select
from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert

from app.models import FeatureRun, MarketFeature
from app.models.enums import SyncJobStatus
from app.repositories.base import BaseRepository


class FeatureRepository(BaseRepository):
    def create_run(
        self,
        *,
        exchange: str,
        symbol: str,
        timeframe: str,
        lookback_days: int,
        start_at: Optional[datetime],
        end_at: Optional[datetime],
    ) -> FeatureRun:
        run = FeatureRun(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            lookback_days=lookback_days,
            start_at=start_at,
            end_at=end_at,
            status=SyncJobStatus.QUEUED,
        )
        self.session.add(run)
        self.session.flush()
        return run

    def mark_running(self, run: FeatureRun) -> FeatureRun:
        run.status = SyncJobStatus.RUNNING
        self.session.add(run)
        self.session.flush()
        return run

    def mark_completed(
        self,
        run: FeatureRun,
        *,
        source_candle_count: int,
        feature_rows_upserted: int,
        computed_start_at: Optional[datetime],
        computed_end_at: Optional[datetime],
    ) -> FeatureRun:
        run.status = SyncJobStatus.COMPLETED
        run.source_candle_count = source_candle_count
        run.feature_rows_upserted = feature_rows_upserted
        run.computed_start_at = computed_start_at
        run.computed_end_at = computed_end_at
        run.error_text = None
        self.session.add(run)
        self.session.flush()
        return run

    def touch_run(
        self,
        run: FeatureRun,
        *,
        source_candle_count: Optional[int] = None,
        feature_rows_upserted: Optional[int] = None,
        computed_start_at: Optional[datetime] = None,
        computed_end_at: Optional[datetime] = None,
    ) -> FeatureRun:
        if source_candle_count is not None:
            run.source_candle_count = source_candle_count
        if feature_rows_upserted is not None:
            run.feature_rows_upserted = feature_rows_upserted
        if computed_start_at is not None:
            run.computed_start_at = computed_start_at
        if computed_end_at is not None:
            run.computed_end_at = computed_end_at
        run.updated_at = datetime.now(timezone.utc)
        self.session.add(run)
        self.session.flush()
        return run

    def mark_failed(
        self,
        run: FeatureRun,
        *,
        error_text: str,
        source_candle_count: int = 0,
        feature_rows_upserted: int = 0,
        computed_start_at: Optional[datetime] = None,
        computed_end_at: Optional[datetime] = None,
    ) -> FeatureRun:
        run.status = SyncJobStatus.FAILED
        run.error_text = error_text
        run.source_candle_count = source_candle_count
        run.feature_rows_upserted = feature_rows_upserted
        run.computed_start_at = computed_start_at
        run.computed_end_at = computed_end_at
        self.session.add(run)
        self.session.flush()
        return run

    def list_runs(
        self,
        *,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        limit: int = 500,
    ) -> list[FeatureRun]:
        stmt = select(FeatureRun).order_by(FeatureRun.updated_at.desc(), FeatureRun.id.desc()).limit(limit)
        if symbol:
            stmt = stmt.where(FeatureRun.symbol == symbol)
        if timeframe:
            stmt = stmt.where(FeatureRun.timeframe == timeframe)
        return list(self.session.scalars(stmt))

    def get_by_id(self, run_id: int) -> Optional[FeatureRun]:
        return self.session.scalar(select(FeatureRun).where(FeatureRun.id == run_id))

    def get_next_queued_run(self) -> Optional[FeatureRun]:
        stmt = (
            select(FeatureRun)
            .where(FeatureRun.status == SyncJobStatus.QUEUED)
            .order_by(FeatureRun.created_at.asc(), FeatureRun.id.asc())
            .limit(1)
        )
        return self.session.scalar(stmt)

    def list_stale_running_runs(self, *, stale_before: datetime) -> list[FeatureRun]:
        stmt = (
            select(FeatureRun)
            .where(
                FeatureRun.status == SyncJobStatus.RUNNING,
                FeatureRun.updated_at < stale_before,
            )
            .order_by(FeatureRun.updated_at.asc(), FeatureRun.id.asc())
        )
        return list(self.session.scalars(stmt))

    def has_active_runs(self) -> bool:
        stmt = select(func.count(FeatureRun.id)).where(
            FeatureRun.status.in_((SyncJobStatus.QUEUED, SyncJobStatus.RUNNING))
        )
        return bool(self.session.scalar(stmt) or 0)

    def delete_all_runs(self) -> int:
        result = self.session.execute(delete(FeatureRun))
        return int(result.rowcount or 0)

    def delete_failed_runs(self) -> int:
        result = self.session.execute(
            delete(FeatureRun).where(FeatureRun.status == SyncJobStatus.FAILED)
        )
        return int(result.rowcount or 0)

    def delete_all_features(self) -> int:
        result = self.session.execute(delete(MarketFeature))
        return int(result.rowcount or 0)

    def upsert_features(
        self,
        *,
        exchange_id: int,
        symbol_id: int,
        timeframe: str,
        rows: Iterable[dict[str, object]],
        chunk_size: int = 500,
        heartbeat: Optional[Callable[[int], None]] = None,
        heartbeat_every_chunks: int = 10,
    ) -> int:
        items = list(rows)
        if not items:
            return 0

        updatable_columns = [
            "ret_1",
            "ret_3",
            "ret_12",
            "ret_48",
            "range_pct",
            "atr_pct",
            "realized_vol_20",
            "body_pct",
            "upper_wick_pct",
            "lower_wick_pct",
            "distance_to_high_20_pct",
            "distance_to_low_20_pct",
            "ema20_dist_pct",
            "ema50_dist_pct",
            "ema200_dist_pct",
            "ema20_slope_pct",
            "ema50_slope_pct",
            "ema200_slope_pct",
            "relative_volume_20",
            "volume_zscore_20",
            "compression_ratio_12",
            "expansion_ratio_12",
        ]

        total_rows = 0
        chunk_count = ceil(len(items) / chunk_size)
        for chunk_index in range(chunk_count):
            chunk = items[chunk_index * chunk_size : (chunk_index + 1) * chunk_size]
            if not chunk:
                continue

            payload = [
                {
                    "exchange_id": exchange_id,
                    "symbol_id": symbol_id,
                    "timeframe": timeframe,
                    **row,
                }
                for row in chunk
            ]

            stmt = insert(MarketFeature).values(payload)
            stmt = stmt.on_conflict_do_update(
                index_elements=["exchange_id", "symbol_id", "timeframe", "open_time"],
                set_={column: getattr(stmt.excluded, column) for column in updatable_columns},
            )
            self.session.execute(stmt)
            total_rows += len(chunk)
            if heartbeat is not None and heartbeat_every_chunks > 0 and (chunk_index + 1) % heartbeat_every_chunks == 0:
                heartbeat(total_rows)

        return total_rows

    def get_feature_coverage(
        self,
        *,
        exchange_id: int,
        symbol_id: int,
        timeframe: str,
    ) -> dict[str, object]:
        row = self.session.execute(
            select(
                func.count(MarketFeature.id),
                func.min(MarketFeature.open_time),
                func.max(MarketFeature.open_time),
            ).where(
                MarketFeature.exchange_id == exchange_id,
                MarketFeature.symbol_id == symbol_id,
                MarketFeature.timeframe == timeframe,
            )
        ).one()
        return {
            "feature_count": int(row[0] or 0),
            "loaded_start_at": row[1],
            "loaded_end_at": row[2],
        }

    def list_feature_coverages(
        self,
        *,
        exchange_id: int,
        symbol_ids: list[int] | None = None,
        timeframes: list[str] | None = None,
    ) -> list[dict[str, object]]:
        stmt = select(
            MarketFeature.symbol_id,
            MarketFeature.timeframe,
            func.count(MarketFeature.id),
            func.min(MarketFeature.open_time),
            func.max(MarketFeature.open_time),
        ).where(MarketFeature.exchange_id == exchange_id)

        if symbol_ids:
            stmt = stmt.where(MarketFeature.symbol_id.in_(symbol_ids))
        if timeframes:
            stmt = stmt.where(MarketFeature.timeframe.in_(timeframes))

        rows = self.session.execute(
            stmt.group_by(MarketFeature.symbol_id, MarketFeature.timeframe)
        ).all()
        return [
            {
                "symbol_id": row[0],
                "timeframe": row[1],
                "feature_count": int(row[2] or 0),
                "loaded_start_at": row[3],
                "loaded_end_at": row[4],
            }
            for row in rows
        ]
