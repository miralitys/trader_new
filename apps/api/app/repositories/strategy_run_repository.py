from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.models import Strategy, StrategyRun
from app.models.enums import StrategyRunMode, StrategyRunStatus
from app.repositories.base import BaseRepository
from app.utils.time import to_iso8601


class StrategyRunRepository(BaseRepository):
    def ensure_strategy(self, code: str, name: str, description: str) -> Strategy:
        strategy = self.session.scalar(select(Strategy).where(Strategy.code == code))
        if strategy is not None:
            return strategy

        stmt = (
            insert(Strategy)
            .values(
                code=code,
                name=name,
                description=description,
                is_active=True,
            )
            .on_conflict_do_nothing(index_elements=["code"])
            .returning(Strategy.id)
        )
        inserted_id = self.session.scalar(stmt)
        if inserted_id is not None:
            return self.session.get(Strategy, inserted_id)

        strategy = self.session.scalar(select(Strategy).where(Strategy.code == code))
        if strategy is None:
            raise ValueError(f"Strategy {code} could not be resolved")
        return strategy

    def get_strategy_by_id(self, strategy_id: int) -> Optional[Strategy]:
        return self.session.get(Strategy, strategy_id)

    def get_strategy_by_code(self, code: str) -> Optional[Strategy]:
        return self.session.scalar(select(Strategy).where(Strategy.code == code))

    def create_paper_run(
        self,
        strategy_id: int,
        symbols: list[str],
        timeframes: list[str],
        metadata_json: dict[str, Any],
    ) -> StrategyRun:
        run = StrategyRun(
            strategy_id=strategy_id,
            mode=StrategyRunMode.PAPER,
            status=StrategyRunStatus.CREATED,
            symbols_json=symbols,
            timeframes_json=timeframes,
            metadata_json=metadata_json,
        )
        self.session.add(run)
        self.session.flush()
        return run

    def get_by_id(self, run_id: int) -> Optional[StrategyRun]:
        return self.session.scalar(select(StrategyRun).where(StrategyRun.id == run_id))

    def list_runs(
        self,
        strategy_code: Optional[str] = None,
        status: Optional[StrategyRunStatus] = None,
        mode: Optional[StrategyRunMode] = None,
        limit: int = 100,
    ) -> list[tuple[StrategyRun, Strategy]]:
        stmt = (
            select(StrategyRun, Strategy)
            .join(Strategy, Strategy.id == StrategyRun.strategy_id)
            .order_by(StrategyRun.created_at.desc(), StrategyRun.id.desc())
            .limit(limit)
        )
        if strategy_code:
            stmt = stmt.where(Strategy.code == strategy_code)
        if status is not None:
            stmt = stmt.where(StrategyRun.status == status)
        if mode is not None:
            stmt = stmt.where(StrategyRun.mode == mode)
        return list(self.session.execute(stmt).all())

    def get_active_paper_run_for_strategy(self, strategy_id: int) -> Optional[StrategyRun]:
        stmt = select(StrategyRun).where(
            StrategyRun.strategy_id == strategy_id,
            StrategyRun.mode == StrategyRunMode.PAPER,
            StrategyRun.status == StrategyRunStatus.RUNNING,
        )
        return self.session.scalar(stmt)

    def list_active_paper_runs(self) -> list[StrategyRun]:
        stmt = (
            select(StrategyRun)
            .where(
                StrategyRun.mode == StrategyRunMode.PAPER,
                StrategyRun.status == StrategyRunStatus.RUNNING,
            )
            .order_by(StrategyRun.started_at.asc().nullsfirst(), StrategyRun.id.asc())
        )
        return list(self.session.scalars(stmt))

    def mark_running(self, run: StrategyRun, started_at: datetime) -> StrategyRun:
        run.status = StrategyRunStatus.RUNNING
        run.started_at = started_at
        run.stopped_at = None
        self.session.add(run)
        self.session.flush()
        return run

    def mark_stopped(self, run: StrategyRun, stopped_at: datetime, reason: str) -> StrategyRun:
        run.status = StrategyRunStatus.STOPPED
        run.stopped_at = stopped_at
        metadata = dict(run.metadata_json or {})
        metadata["stop_reason"] = reason
        run.metadata_json = metadata
        self.session.add(run)
        self.session.flush()
        return run

    def mark_failed(self, run: StrategyRun, stopped_at: datetime, error_text: str) -> StrategyRun:
        run.status = StrategyRunStatus.FAILED
        run.stopped_at = stopped_at
        metadata = dict(run.metadata_json or {})
        metadata["last_error"] = error_text
        run.metadata_json = metadata
        self.session.add(run)
        self.session.flush()
        return run

    def update_last_processed(
        self,
        run: StrategyRun,
        candle_time: datetime,
        stream_key: str,
    ) -> StrategyRun:
        metadata = dict(run.metadata_json or {})
        watermarks = dict(metadata.get("last_processed_by_stream", {}))
        watermarks[stream_key] = to_iso8601(candle_time)
        metadata["last_processed_by_stream"] = watermarks
        run.metadata_json = metadata

        if run.last_processed_candle_at is None or candle_time > run.last_processed_candle_at:
            run.last_processed_candle_at = candle_time

        self.session.add(run)
        self.session.flush()
        return run

    def store_open_position_runtime(
        self,
        run: StrategyRun,
        symbol: str,
        runtime_payload: dict[str, Any],
    ) -> StrategyRun:
        metadata = dict(run.metadata_json or {})
        open_positions_runtime = dict(metadata.get("open_positions_runtime", {}))
        open_positions_runtime[symbol] = runtime_payload
        metadata["open_positions_runtime"] = open_positions_runtime
        run.metadata_json = metadata
        self.session.add(run)
        self.session.flush()
        return run

    def clear_open_position_runtime(self, run: StrategyRun, symbol: str) -> StrategyRun:
        metadata = dict(run.metadata_json or {})
        open_positions_runtime = dict(metadata.get("open_positions_runtime", {}))
        open_positions_runtime.pop(symbol, None)
        metadata["open_positions_runtime"] = open_positions_runtime
        run.metadata_json = metadata
        self.session.add(run)
        self.session.flush()
        return run
