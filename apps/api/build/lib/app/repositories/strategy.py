from __future__ import annotations

from datetime import datetime

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.domain.models import Strategy, StrategyConfig, StrategyRun


class StrategyRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def list_strategies(self) -> list[Strategy]:
        return list(self.db.scalars(select(Strategy).order_by(Strategy.id)))

    def get_strategy(self, strategy_id: int) -> Strategy | None:
        return self.db.get(Strategy, strategy_id)

    def get_strategy_by_key(self, key: str) -> Strategy | None:
        return self.db.scalar(select(Strategy).where(Strategy.key == key))

    def get_config(self, strategy_id: int) -> StrategyConfig | None:
        return self.db.scalar(select(StrategyConfig).where(StrategyConfig.strategy_id == strategy_id))

    def upsert_config(
        self,
        strategy_id: int,
        settings: dict,
        risk_settings: dict,
        symbols: list[str],
        timeframes: list[str],
        paper_account_id: int | None,
    ) -> StrategyConfig:
        config = self.get_config(strategy_id)
        if config is None:
            config = StrategyConfig(
                strategy_id=strategy_id,
                settings=settings,
                risk_settings=risk_settings,
                symbols=symbols,
                timeframes=timeframes,
                paper_account_id=paper_account_id,
            )
        else:
            config.settings = settings
            config.risk_settings = risk_settings
            config.symbols = symbols
            config.timeframes = timeframes
            config.paper_account_id = paper_account_id
        self.db.add(config)
        self.db.flush()
        return config

    def list_runs(self, strategy_id: int, active_only: bool = False) -> list[StrategyRun]:
        query = select(StrategyRun).where(StrategyRun.strategy_id == strategy_id)
        if active_only:
            query = query.where(StrategyRun.ended_at.is_(None))
        query = query.order_by(desc(StrategyRun.started_at))
        return list(self.db.scalars(query))

    def list_active_runs(self, mode: str | None = None) -> list[StrategyRun]:
        query = select(StrategyRun).where(StrategyRun.ended_at.is_(None))
        if mode is not None:
            query = query.where(StrategyRun.mode == mode)
        query = query.order_by(StrategyRun.started_at.asc())
        return list(self.db.scalars(query))

    def create_runs(
        self,
        strategy_id: int,
        paper_account_id: int | None,
        symbol_ids: list[int],
        timeframe_ids: list[int],
        mode: str,
        status: str,
    ) -> list[StrategyRun]:
        runs: list[StrategyRun] = []
        for symbol_id in symbol_ids:
            for timeframe_id in timeframe_ids:
                run = StrategyRun(
                    strategy_id=strategy_id,
                    paper_account_id=paper_account_id,
                    symbol_id=symbol_id,
                    timeframe_id=timeframe_id,
                    mode=mode,
                    status=status,
                )
                self.db.add(run)
                runs.append(run)
        self.db.flush()
        return runs

    def stop_runs(self, strategy_id: int, ended_at: datetime) -> None:
        for run in self.list_runs(strategy_id, active_only=True):
            run.ended_at = ended_at
            run.status = "stopped"
            self.db.add(run)
        self.db.flush()

    def update_run_progress(self, run: StrategyRun, candle_time: datetime) -> StrategyRun:
        run.last_processed_candle = candle_time
        self.db.add(run)
        self.db.flush()
        return run

    def update_strategy_status(self, strategy: Strategy, status: str) -> Strategy:
        strategy.status = status
        self.db.add(strategy)
        self.db.flush()
        return strategy

    def update_strategy_timestamps(
        self,
        strategy: Strategy,
        *,
        last_signal_at: datetime | None = None,
        last_processed_candle_at: datetime | None = None,
    ) -> Strategy:
        if last_signal_at is not None:
            strategy.last_signal_at = last_signal_at
        if last_processed_candle_at is not None:
            strategy.last_processed_candle_at = last_processed_candle_at
        self.db.add(strategy)
        self.db.flush()
        return strategy
