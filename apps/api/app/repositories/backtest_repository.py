from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.models import BacktestResult, BacktestRun, Strategy
from app.models.enums import BacktestStatus
from app.repositories.base import BaseRepository
from app.schemas.backtest import BacktestResponse


class BacktestRepository(BaseRepository):
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

    def create_run(self, strategy_id: int, params_json: dict[str, object]) -> BacktestRun:
        run = BacktestRun(
            strategy_id=strategy_id,
            status=BacktestStatus.QUEUED,
            params_json=params_json,
        )
        self.session.add(run)
        self.session.flush()
        return run

    def get_run(self, run_id: int) -> Optional[BacktestRun]:
        return self.session.get(BacktestRun, run_id)

    def get_result(self, backtest_run_id: int) -> Optional[BacktestResult]:
        stmt = select(BacktestResult).where(BacktestResult.backtest_run_id == backtest_run_id)
        return self.session.scalar(stmt)

    def list_runs(
        self,
        limit: int = 100,
        status: Optional[BacktestStatus] = None,
        strategy_code: Optional[str] = None,
    ) -> list[tuple[BacktestRun, Strategy, Optional[BacktestResult]]]:
        stmt = (
            select(BacktestRun, Strategy, BacktestResult)
            .join(Strategy, Strategy.id == BacktestRun.strategy_id)
            .outerjoin(BacktestResult, BacktestResult.backtest_run_id == BacktestRun.id)
            .order_by(BacktestRun.created_at.desc(), BacktestRun.id.desc())
            .limit(limit)
        )
        if status is not None:
            stmt = stmt.where(BacktestRun.status == status)
        if strategy_code:
            stmt = stmt.where(Strategy.code == strategy_code)
        return list(self.session.execute(stmt).all())

    def get_run_with_result(
        self,
        run_id: int,
    ) -> Optional[tuple[BacktestRun, Strategy, Optional[BacktestResult]]]:
        stmt = (
            select(BacktestRun, Strategy, BacktestResult)
            .join(Strategy, Strategy.id == BacktestRun.strategy_id)
            .outerjoin(BacktestResult, BacktestResult.backtest_run_id == BacktestRun.id)
            .where(BacktestRun.id == run_id)
        )
        return self.session.execute(stmt).first()

    def mark_running(self, run: BacktestRun, started_at: datetime) -> BacktestRun:
        run.status = BacktestStatus.RUNNING
        run.started_at = started_at
        run.error_text = None
        self.session.add(run)
        self.session.flush()
        return run

    def mark_completed(self, run: BacktestRun, completed_at: datetime) -> BacktestRun:
        run.status = BacktestStatus.COMPLETED
        run.completed_at = completed_at
        run.error_text = None
        self.session.add(run)
        self.session.flush()
        return run

    def mark_failed(self, run: BacktestRun, completed_at: datetime, error_text: str) -> BacktestRun:
        run.status = BacktestStatus.FAILED
        run.completed_at = completed_at
        run.error_text = error_text
        self.session.add(run)
        self.session.flush()
        return run

    def save_result(self, backtest_run_id: int, report: BacktestResponse) -> BacktestResult:
        existing = self.session.scalar(
            select(BacktestResult).where(BacktestResult.backtest_run_id == backtest_run_id)
        )
        payload = report.model_dump(mode="json")
        summary_json = {
            "strategy_code": payload["strategy_code"],
            "symbol": payload["symbol"],
            "timeframe": payload["timeframe"],
            "exchange_code": payload["exchange_code"],
            "initial_capital": payload["initial_capital"],
            "final_equity": payload["final_equity"],
            "params": payload["params"],
            "metrics": payload["metrics"],
            "trades": payload["trades"],
        }

        if existing is None:
            existing = BacktestResult(backtest_run_id=backtest_run_id)

        existing.total_return_pct = report.metrics.total_return_pct
        existing.max_drawdown_pct = report.metrics.max_drawdown_pct
        existing.win_rate_pct = report.metrics.win_rate_pct
        existing.profit_factor = report.metrics.profit_factor
        existing.expectancy = report.metrics.expectancy
        existing.total_trades = report.metrics.total_trades
        existing.avg_winner = report.metrics.avg_winner
        existing.avg_loser = report.metrics.avg_loser
        existing.equity_curve_json = payload["equity_curve"]
        existing.summary_json = summary_json

        self.session.add(existing)
        self.session.flush()
        return existing
