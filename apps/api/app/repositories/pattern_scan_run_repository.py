from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import select

from app.models import PatternScanRun
from app.models.enums import SyncJobStatus
from app.repositories.base import BaseRepository


class PatternScanRunRepository(BaseRepository):
    def create_run(
        self,
        *,
        exchange: str,
        symbols: list[str],
        timeframes: list[str],
        lookback_days: int,
        forward_bars: int,
        fee_pct: str,
        slippage_pct: str,
        max_bars_per_series: int,
    ) -> PatternScanRun:
        run = PatternScanRun(
            exchange=exchange,
            symbols_json={"symbols": symbols},
            timeframes_json={"timeframes": timeframes},
            lookback_days=lookback_days,
            forward_bars=forward_bars,
            fee_pct=fee_pct,
            slippage_pct=slippage_pct,
            max_bars_per_series=max_bars_per_series,
            status=SyncJobStatus.QUEUED,
            progress_json={},
            report_summary_json={},
            report_json={},
        )
        self.session.add(run)
        self.session.flush()
        return run

    def get_by_id(self, run_id: int) -> Optional[PatternScanRun]:
        return self.session.scalar(select(PatternScanRun).where(PatternScanRun.id == run_id))

    def list_runs(self, limit: int = 50) -> list[PatternScanRun]:
        stmt = select(PatternScanRun).order_by(PatternScanRun.updated_at.desc(), PatternScanRun.id.desc()).limit(limit)
        return list(self.session.scalars(stmt))

    def get_next_queued_run(self) -> Optional[PatternScanRun]:
        stmt = (
            select(PatternScanRun)
            .where(PatternScanRun.status == SyncJobStatus.QUEUED)
            .order_by(PatternScanRun.created_at.asc(), PatternScanRun.id.asc())
            .limit(1)
        )
        return self.session.scalar(stmt)

    def list_stale_running_runs(self, *, stale_before: datetime) -> list[PatternScanRun]:
        stmt = (
            select(PatternScanRun)
            .where(
                PatternScanRun.status == SyncJobStatus.RUNNING,
                PatternScanRun.updated_at < stale_before,
            )
            .order_by(PatternScanRun.updated_at.asc(), PatternScanRun.id.asc())
        )
        return list(self.session.scalars(stmt))

    def mark_running(self, run: PatternScanRun, *, started_at: datetime) -> PatternScanRun:
        run.status = SyncJobStatus.RUNNING
        run.started_at = started_at
        run.completed_at = None
        run.error_text = None
        self.session.add(run)
        self.session.flush()
        return run

    def update_progress(self, run: PatternScanRun, *, progress_json: dict) -> PatternScanRun:
        run.progress_json = progress_json
        self.session.add(run)
        self.session.flush()
        return run

    def mark_completed(
        self,
        run: PatternScanRun,
        *,
        completed_at: datetime,
        report_summary_json: dict,
        report_json: dict,
    ) -> PatternScanRun:
        run.status = SyncJobStatus.COMPLETED
        run.completed_at = completed_at
        run.error_text = None
        total_series = len(report_summary_json.get("coverage", []))
        run.progress_json = {
            "phase": "completed",
            "processed_series": total_series,
            "total_series": total_series,
            "percent_complete": 100.0,
            "current_symbol": None,
            "current_timeframe": None,
        }
        run.report_summary_json = report_summary_json
        run.report_json = report_json
        self.session.add(run)
        self.session.flush()
        return run

    def mark_failed(self, run: PatternScanRun, *, completed_at: datetime, error_text: str) -> PatternScanRun:
        run.status = SyncJobStatus.FAILED
        run.completed_at = completed_at
        run.error_text = error_text
        progress = dict(run.progress_json or {})
        progress["phase"] = "failed"
        run.progress_json = progress
        self.session.add(run)
        self.session.flush()
        return run
