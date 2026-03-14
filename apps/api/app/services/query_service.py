from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional

from app.core.config import get_settings
from app.core.logging import get_logger
from sqlalchemy.orm import Session

from app.api.errors import BadRequestError, NotFoundError
from app.models.enums import AppLogLevel, BacktestStatus, PositionStatus, StrategyRunMode, StrategyRunStatus, SyncJobStatus
from app.repositories.backtest_repository import BacktestRepository
from app.repositories.candle_repository import CandleRepository
from app.repositories.log_repository import LogRepository
from app.repositories.paper_account_repository import PaperAccountRepository
from app.repositories.position_repository import PositionRepository
from app.repositories.signal_repository import SignalRepository
from app.repositories.strategy_run_repository import StrategyRunRepository
from app.repositories.sync_job_repository import SyncJobRepository
from app.repositories.trade_repository import TradeRepository
from app.schemas.api import (
    AppLogResponse,
    BacktestListItemResponse,
    CandleResponse,
    DashboardDataSyncStatus,
    DashboardPerformanceSnapshot,
    DashboardRunStatus,
    DashboardSummaryResponse,
    PositionResponse,
    SignalResponse,
    StrategyRunDetailResponse,
    StrategyRunSummaryResponse,
    SyncJobResponse,
    TradeResponse,
)
from app.schemas.backtest import BacktestMetrics, BacktestResponse, BacktestTrade, EquityPoint
from app.services.strategy_service import StrategyService
from app.utils.time import ensure_utc, utc_now

logger = get_logger(__name__)


class QueryService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.backtest_repository = BacktestRepository(session)
        self.candle_repository = CandleRepository(session)
        self.log_repository = LogRepository(session)
        self.paper_account_repository = PaperAccountRepository(session)
        self.position_repository = PositionRepository(session)
        self.signal_repository = SignalRepository(session)
        self.strategy_run_repository = StrategyRunRepository(session)
        self.sync_job_repository = SyncJobRepository(session)
        self.trade_repository = TradeRepository(session)
        self.strategy_service = StrategyService(session)

    def list_strategy_runs(
        self,
        strategy_code: Optional[str] = None,
        status: Optional[str] = None,
        mode: Optional[str] = None,
        limit: int = 100,
    ) -> list[StrategyRunSummaryResponse]:
        parsed_status = self._parse_enum(StrategyRunStatus, status, "strategy run status")
        parsed_mode = self._parse_enum(StrategyRunMode, mode, "strategy run mode")
        rows = self.strategy_run_repository.list_runs(
            strategy_code=strategy_code,
            status=parsed_status,
            mode=parsed_mode,
            limit=limit,
        )
        return [self._build_strategy_run_response(run, strategy) for run, strategy in rows]

    def get_strategy_run(self, run_id: int) -> StrategyRunDetailResponse:
        run = self.strategy_run_repository.get_by_id(run_id)
        if run is None:
            raise NotFoundError(f"Strategy run {run_id} was not found")

        strategy = self.strategy_run_repository.get_strategy_by_id(run.strategy_id)
        if strategy is None:
            raise NotFoundError(f"Strategy for run {run_id} was not found")
        return StrategyRunDetailResponse.model_validate(self._build_strategy_run_response(run, strategy))

    def list_backtests(
        self,
        strategy_code: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> list[BacktestListItemResponse]:
        self._recover_stale_backtests()
        parsed_status = self._parse_enum(BacktestStatus, status, "backtest status")
        rows = self.backtest_repository.list_runs(
            limit=limit,
            status=parsed_status,
            strategy_code=strategy_code,
        )
        return [self._build_backtest_list_item(run, strategy, result) for run, strategy, result in rows]

    def get_backtest(self, run_id: int) -> BacktestResponse:
        self._recover_stale_backtests()
        row = self.backtest_repository.get_run_with_result(run_id)
        if row is None:
            raise NotFoundError(f"Backtest {run_id} was not found")
        run, strategy, result = row

        params = dict(run.params_json or {})
        summary = dict(result.summary_json or {}) if result is not None else {}
        summary_metrics = dict(summary.get("metrics", {}))
        metrics = (
            BacktestMetrics(
                total_return_pct=result.total_return_pct,
                max_drawdown_pct=result.max_drawdown_pct,
                win_rate_pct=result.win_rate_pct,
                profit_factor=result.profit_factor,
                expectancy=result.expectancy,
                gross_expectancy=Decimal(str(summary_metrics.get("gross_expectancy", "0"))),
                net_expectancy=Decimal(str(summary_metrics.get("net_expectancy", result.expectancy))),
                avg_winner=result.avg_winner,
                avg_loser=result.avg_loser,
                total_trades=result.total_trades,
            )
            if result is not None
            else BacktestMetrics()
        )
        equity_curve = [EquityPoint.model_validate(point) for point in (result.equity_curve_json if result else [])]
        trades = [BacktestTrade.model_validate(trade) for trade in summary.get("trades", [])]
        diagnostics = dict(summary.get("diagnostics", {}))
        initial_capital = Decimal(str(summary.get("initial_capital", params.get("initial_capital", "0"))))
        final_equity = Decimal(str(summary.get("final_equity", initial_capital)))

        return BacktestResponse(
            run_id=run.id,
            strategy_code=strategy.code,
            symbol=str(summary.get("symbol", params.get("symbol", ""))),
            timeframe=str(summary.get("timeframe", params.get("timeframe", ""))),
            exchange_code=str(summary.get("exchange_code", params.get("exchange_code", "coinbase"))),
            status=run.status.value,
            initial_capital=initial_capital,
            final_equity=final_equity,
            started_at=run.started_at or run.created_at,
            completed_at=run.completed_at,
            params=params,
            metrics=metrics,
            equity_curve=equity_curve,
            trades=trades,
            diagnostics=diagnostics,
            error_text=run.error_text,
        )

    def list_sync_jobs(
        self,
        status: Optional[str] = None,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        limit: int = 100,
    ) -> list[SyncJobResponse]:
        parsed_status = self._parse_enum(SyncJobStatus, status, "sync job status")
        jobs = self.sync_job_repository.list_jobs(
            limit=limit,
            status=parsed_status,
            symbol=symbol,
            timeframe=timeframe,
        )
        return [
            SyncJobResponse(
                id=job.id,
                exchange=job.exchange,
                symbol=job.symbol,
                timeframe=job.timeframe,
                start_at=job.start_at,
                end_at=job.end_at,
                status=job.status.value,
                rows_inserted=job.rows_inserted,
                error_text=job.error_text,
                created_at=job.created_at,
                updated_at=job.updated_at,
            )
            for job in jobs
        ]

    def list_candles(
        self,
        exchange_code: str,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
        limit: Optional[int] = None,
    ) -> list[CandleResponse]:
        candles = self.candle_repository.list_candles(
            exchange_code=exchange_code,
            symbol_code=symbol,
            timeframe=timeframe,
            start_at=ensure_utc(start_at),
            end_at=ensure_utc(end_at),
            limit=limit,
        )
        return [
            CandleResponse(
                id=candle.id,
                exchange_code=exchange_code,
                symbol=symbol,
                timeframe=timeframe,
                open_time=candle.open_time,
                open=candle.open,
                high=candle.high,
                low=candle.low,
                close=candle.close,
                volume=candle.volume,
                created_at=candle.created_at,
            )
            for candle in candles
        ]

    def list_signals(
        self,
        strategy_run_id: Optional[int] = None,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        limit: int = 100,
    ) -> list[SignalResponse]:
        signals = self.signal_repository.list_signals(
            strategy_run_id=strategy_run_id,
            symbol=symbol,
            timeframe=timeframe,
            limit=limit,
        )
        strategy_code_cache: dict[int, Optional[str]] = {}
        return [
            SignalResponse(
                id=signal.id,
                strategy_run_id=signal.strategy_run_id,
                strategy_code=self._resolve_strategy_code(signal.strategy_run_id, strategy_code_cache),
                symbol=signal.symbol,
                timeframe=signal.timeframe,
                signal_type=signal.signal_type.value,
                signal_strength=Decimal(str(signal.signal_strength)),
                payload=signal.payload_json,
                candle_time=signal.candle_time,
                created_at=signal.created_at,
            )
            for signal in signals
        ]

    def list_trades(
        self,
        strategy_run_id: Optional[int] = None,
        symbol: Optional[str] = None,
        limit: int = 100,
    ) -> list[TradeResponse]:
        trades = self.trade_repository.list_trades(
            strategy_run_id=strategy_run_id,
            symbol=symbol,
            limit=limit,
        )
        strategy_code_cache: dict[int, Optional[str]] = {}
        return [
            TradeResponse(
                id=trade.id,
                strategy_run_id=trade.strategy_run_id,
                strategy_code=self._resolve_strategy_code(trade.strategy_run_id, strategy_code_cache),
                symbol=trade.symbol,
                entry_price=trade.entry_price,
                exit_price=trade.exit_price,
                qty=trade.qty,
                pnl=trade.pnl,
                pnl_pct=trade.pnl_pct,
                fees=trade.fees,
                slippage=trade.slippage,
                opened_at=trade.opened_at,
                closed_at=trade.closed_at,
                metadata=trade.metadata_json,
            )
            for trade in trades
        ]

    def _recover_stale_backtests(self) -> None:
        settings = get_settings()
        stale_before = utc_now() - timedelta(seconds=settings.backtest_stale_after_seconds)
        try:
            recovered = self.backtest_repository.recover_stale_runs(stale_before=stale_before)
            if not recovered:
                return

            self.session.commit()
            logger.warning(
                "Recovered stale backtests during query",
                extra={
                    "stale_before": stale_before.isoformat(),
                    "recovered_runs": recovered,
                },
            )
        except Exception:
            self.session.rollback()
            logger.exception("Failed to recover stale backtests during query")

    def list_positions(
        self,
        strategy_run_id: Optional[int] = None,
        symbol: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> list[PositionResponse]:
        parsed_status = self._parse_enum(PositionStatus, status, "position status")
        positions = self.position_repository.list_positions(
            strategy_run_id=strategy_run_id,
            symbol=symbol,
            status=parsed_status,
            limit=limit,
        )
        strategy_code_cache: dict[int, Optional[str]] = {}
        return [
            PositionResponse(
                id=position.id,
                strategy_run_id=position.strategy_run_id,
                strategy_code=self._resolve_strategy_code(position.strategy_run_id, strategy_code_cache),
                symbol=position.symbol,
                side=position.side.value,
                qty=position.qty,
                avg_entry_price=position.avg_entry_price,
                stop_price=position.stop_price,
                take_profit_price=position.take_profit_price,
                status=position.status.value,
                opened_at=position.opened_at,
                closed_at=position.closed_at,
            )
            for position in positions
        ]

    def list_logs(
        self,
        scope: Optional[str] = None,
        level: Optional[str] = None,
        limit: int = 100,
    ) -> list[AppLogResponse]:
        parsed_level = self._parse_enum(AppLogLevel, level, "log level")
        logs = self.log_repository.list_logs(
            scope=scope,
            level=parsed_level.value if parsed_level is not None else None,
            limit=limit,
        )
        return [
            AppLogResponse(
                id=log.id,
                scope=log.scope,
                level=log.level.value,
                message=log.message,
                payload=log.payload_json,
                created_at=log.created_at,
            )
            for log in logs
        ]

    def get_dashboard_summary(self) -> DashboardSummaryResponse:
        strategies = self.strategy_service.list_strategies()
        visible_strategy_codes = self.strategy_service.visible_strategy_codes()
        paper_runs = [
            row
            for row in self.strategy_run_repository.list_runs(mode=StrategyRunMode.PAPER, limit=500)
            if row[1].code in visible_strategy_codes
        ]
        recent_backtests = [
            backtest
            for backtest in self.list_backtests(limit=100)
            if backtest.strategy_code in visible_strategy_codes
        ][:5]
        recent_trades = [
            trade
            for trade in self.list_trades(limit=100)
            if trade.strategy_code in visible_strategy_codes
        ][:5]
        open_positions_count = sum(
            1
            for position in self.list_positions(status=PositionStatus.OPEN.value, limit=500)
            if position.strategy_code in visible_strategy_codes
        )
        recent_jobs = self.list_sync_jobs(limit=5)
        completed_backtests = [
            backtest for backtest in recent_backtests if backtest.status == BacktestStatus.COMPLETED.value
        ]

        active_paper_runs = sum(1 for run, _ in paper_runs if run.status == StrategyRunStatus.RUNNING)
        stopped_paper_runs = sum(1 for run, _ in paper_runs if run.status == StrategyRunStatus.STOPPED)
        failed_paper_runs = sum(1 for run, _ in paper_runs if run.status == StrategyRunStatus.FAILED)

        key_metrics = [
            DashboardPerformanceSnapshot(
                backtest_run_id=backtest.id,
                strategy_code=backtest.strategy_code,
                symbol=backtest.symbol,
                timeframe=backtest.timeframe,
                total_return_pct=backtest.total_return_pct,
                win_rate_pct=backtest.win_rate_pct,
                max_drawdown_pct=backtest.max_drawdown_pct,
                total_trades=backtest.total_trades,
            )
            for backtest in completed_backtests
        ]

        return DashboardSummaryResponse(
            strategies=strategies,
            run_status=DashboardRunStatus(
                active_paper_runs=active_paper_runs,
                stopped_paper_runs=stopped_paper_runs,
                failed_paper_runs=failed_paper_runs,
                recent_backtests=len(recent_backtests),
            ),
            key_performance_metrics=key_metrics,
            open_positions_count=open_positions_count,
            recent_trades=recent_trades,
            recent_backtests=recent_backtests,
            data_sync_status=DashboardDataSyncStatus(
                latest_job=recent_jobs[0] if recent_jobs else None,
                recent_jobs=recent_jobs,
            ),
        )

    def _build_strategy_run_response(self, run, strategy) -> StrategyRunSummaryResponse:
        account = self.paper_account_repository.get_by_strategy_id(strategy.id)
        return StrategyRunSummaryResponse(
            id=run.id,
            strategy_code=strategy.code,
            strategy_name=strategy.name,
            mode=run.mode.value,
            status=run.status.value,
            symbols=run.symbols_json,
            timeframes=run.timeframes_json,
            started_at=run.started_at,
            stopped_at=run.stopped_at,
            last_processed_candle_at=run.last_processed_candle_at,
            created_at=run.created_at,
            metadata=run.metadata_json,
            account_balance=account.balance if account is not None else None,
            currency=account.currency if account is not None else None,
            open_positions_count=self.position_repository.count_open_positions(run.id),
        )

    def _build_backtest_list_item(self, run, strategy, result) -> BacktestListItemResponse:
        summary = dict(result.summary_json or {}) if result is not None else {}
        params = dict(run.params_json or {})
        initial_capital = Decimal(str(summary.get("initial_capital", params.get("initial_capital", "0"))))
        final_equity = Decimal(str(summary.get("final_equity", initial_capital)))
        return BacktestListItemResponse(
            id=run.id,
            strategy_code=strategy.code,
            strategy_name=strategy.name,
            status=run.status.value,
            symbol=str(summary.get("symbol", params.get("symbol", ""))),
            timeframe=str(summary.get("timeframe", params.get("timeframe", ""))),
            started_at=run.started_at,
            completed_at=run.completed_at,
            initial_capital=initial_capital,
            final_equity=final_equity,
            total_return_pct=result.total_return_pct if result is not None else Decimal("0"),
            max_drawdown_pct=result.max_drawdown_pct if result is not None else Decimal("0"),
            win_rate_pct=result.win_rate_pct if result is not None else Decimal("0"),
            total_trades=result.total_trades if result is not None else 0,
            error_text=run.error_text,
        )

    def _resolve_strategy_code(
        self,
        strategy_run_id: int,
        cache: dict[int, Optional[str]],
    ) -> Optional[str]:
        if strategy_run_id in cache:
            return cache[strategy_run_id]

        run = self.strategy_run_repository.get_by_id(strategy_run_id)
        if run is None:
            cache[strategy_run_id] = None
            return None

        strategy = self.strategy_run_repository.get_strategy_by_id(run.strategy_id)
        cache[strategy_run_id] = strategy.code if strategy is not None else None
        return cache[strategy_run_id]

    def _parse_enum(self, enum_class, raw_value: Optional[str], label: str):
        if raw_value is None:
            return None
        try:
            return enum_class(raw_value)
        except ValueError as exc:
            raise BadRequestError(f"Unsupported {label}: {raw_value}") from exc
