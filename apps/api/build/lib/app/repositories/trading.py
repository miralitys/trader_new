from __future__ import annotations

from datetime import datetime

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.domain.enums import BacktestStatus, OrderStatus, PositionStatus
from app.domain.models import AppLog, BacktestResult, BacktestRun, Order, PaperAccount, Position, Signal, Trade


class TradingRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def create_signal(
        self,
        *,
        strategy_id: int,
        strategy_run_id: int | None,
        symbol_id: int,
        timeframe_id: int,
        candle_time: datetime,
        action: str,
        side: str,
        strength: float,
        payload: dict,
    ) -> Signal:
        signal = Signal(
            strategy_id=strategy_id,
            strategy_run_id=strategy_run_id,
            symbol_id=symbol_id,
            timeframe_id=timeframe_id,
            candle_time=candle_time,
            action=action,
            side=side,
            strength=strength,
            payload=payload,
        )
        self.db.add(signal)
        self.db.flush()
        return signal

    def list_signals(self, strategy_id: int | None = None, limit: int = 200) -> list[Signal]:
        query = select(Signal).order_by(desc(Signal.candle_time)).limit(limit)
        if strategy_id is not None:
            query = query.where(Signal.strategy_id == strategy_id)
        return list(self.db.scalars(query))

    def get_open_position(self, strategy_id: int, symbol_id: int, timeframe_id: int, mode: str) -> Position | None:
        return self.db.scalar(
            select(Position).where(
                Position.strategy_id == strategy_id,
                Position.symbol_id == symbol_id,
                Position.timeframe_id == timeframe_id,
                Position.mode == mode,
                Position.status == PositionStatus.OPEN.value,
            )
        )

    def list_positions(
        self,
        strategy_id: int | None = None,
        status: str | None = None,
        limit: int = 200,
    ) -> list[Position]:
        query = select(Position).order_by(desc(Position.opened_at)).limit(limit)
        if strategy_id is not None:
            query = query.where(Position.strategy_id == strategy_id)
        if status is not None:
            query = query.where(Position.status == status)
        return list(self.db.scalars(query))

    def create_position(
        self,
        *,
        strategy_id: int,
        symbol_id: int,
        timeframe_id: int,
        mode: str,
        entry_price: float,
        quantity: float,
        stop_loss: float | None,
        take_profit: float | None,
        opened_at: datetime,
    ) -> Position:
        position = Position(
            strategy_id=strategy_id,
            symbol_id=symbol_id,
            timeframe_id=timeframe_id,
            mode=mode,
            entry_price=entry_price,
            quantity=quantity,
            stop_loss=stop_loss,
            take_profit=take_profit,
            opened_at=opened_at,
        )
        self.db.add(position)
        self.db.flush()
        return position

    def update_position_mark(self, position: Position, current_price: float) -> Position:
        position.unrealized_pnl = (current_price - position.entry_price) * position.quantity
        self.db.add(position)
        self.db.flush()
        return position

    def close_position(
        self,
        position: Position,
        *,
        exit_time: datetime,
        realized_pnl: float,
    ) -> Position:
        position.status = PositionStatus.CLOSED.value
        position.closed_at = exit_time
        position.realized_pnl = realized_pnl
        position.unrealized_pnl = 0.0
        self.db.add(position)
        self.db.flush()
        return position

    def create_order(
        self,
        *,
        strategy_id: int,
        signal_id: int | None,
        position_id: int | None,
        symbol_id: int,
        timeframe_id: int,
        mode: str,
        side: str,
        status: str,
        requested_qty: float,
        filled_qty: float,
        requested_price: float,
        filled_price: float,
        fee: float,
        slippage: float,
        metadata_json: dict,
        filled_at: datetime,
    ) -> Order:
        order = Order(
            strategy_id=strategy_id,
            signal_id=signal_id,
            position_id=position_id,
            symbol_id=symbol_id,
            timeframe_id=timeframe_id,
            mode=mode,
            side=side,
            status=status,
            requested_qty=requested_qty,
            filled_qty=filled_qty,
            requested_price=requested_price,
            filled_price=filled_price,
            fee=fee,
            slippage=slippage,
            metadata_json=metadata_json,
            filled_at=filled_at,
        )
        self.db.add(order)
        self.db.flush()
        return order

    def create_trade(
        self,
        *,
        strategy_id: int,
        position_id: int | None,
        order_id: int | None,
        symbol_id: int,
        timeframe_id: int,
        entry_time: datetime,
        exit_time: datetime,
        entry_price: float,
        exit_price: float,
        quantity: float,
        gross_pnl: float,
        net_pnl: float,
        fee: float,
        slippage: float,
        notes: dict,
    ) -> Trade:
        trade = Trade(
            strategy_id=strategy_id,
            position_id=position_id,
            order_id=order_id,
            symbol_id=symbol_id,
            timeframe_id=timeframe_id,
            entry_time=entry_time,
            exit_time=exit_time,
            entry_price=entry_price,
            exit_price=exit_price,
            quantity=quantity,
            gross_pnl=gross_pnl,
            net_pnl=net_pnl,
            fee=fee,
            slippage=slippage,
            notes=notes,
        )
        self.db.add(trade)
        self.db.flush()
        return trade

    def list_trades(self, strategy_id: int | None = None, limit: int = 500) -> list[Trade]:
        query = select(Trade).order_by(desc(Trade.exit_time)).limit(limit)
        if strategy_id is not None:
            query = query.where(Trade.strategy_id == strategy_id)
        return list(self.db.scalars(query))

    def get_paper_account(self, paper_account_id: int | None) -> PaperAccount | None:
        if paper_account_id is None:
            return None
        return self.db.get(PaperAccount, paper_account_id)

    def list_paper_accounts(self) -> list[PaperAccount]:
        return list(self.db.scalars(select(PaperAccount).order_by(PaperAccount.id)))

    def update_paper_account(self, account: PaperAccount, *, balance: float, equity: float) -> PaperAccount:
        account.balance = balance
        account.equity = equity
        self.db.add(account)
        self.db.flush()
        return account

    def create_log(
        self,
        *,
        category: str,
        level: str,
        message: str,
        strategy_id: int | None = None,
        symbol_id: int | None = None,
        context: dict | None = None,
    ) -> AppLog:
        log = AppLog(
            category=category,
            level=level,
            message=message,
            strategy_id=strategy_id,
            symbol_id=symbol_id,
            context=context or {},
        )
        self.db.add(log)
        self.db.flush()
        return log

    def list_logs(self, strategy_id: int | None = None, limit: int = 200) -> list[AppLog]:
        query = select(AppLog).order_by(desc(AppLog.created_at)).limit(limit)
        if strategy_id is not None:
            query = query.where(AppLog.strategy_id == strategy_id)
        return list(self.db.scalars(query))

    def create_backtest_run(self, strategy_id: int, params: dict) -> BacktestRun:
        run = BacktestRun(strategy_id=strategy_id, params=params, status=BacktestStatus.QUEUED.value)
        self.db.add(run)
        self.db.flush()
        return run

    def list_backtest_runs(self, limit: int = 50) -> list[BacktestRun]:
        return list(self.db.scalars(select(BacktestRun).order_by(desc(BacktestRun.created_at)).limit(limit)))

    def get_backtest_run(self, run_id: int) -> BacktestRun | None:
        return self.db.get(BacktestRun, run_id)

    def update_backtest_run(
        self,
        run: BacktestRun,
        *,
        status: str | None = None,
        started_at: datetime | None = None,
        ended_at: datetime | None = None,
        error_message: str | None = None,
    ) -> BacktestRun:
        if status is not None:
            run.status = status
        if started_at is not None:
            run.started_at = started_at
        if ended_at is not None:
            run.ended_at = ended_at
        if error_message is not None:
            run.error_message = error_message
        self.db.add(run)
        self.db.flush()
        return run

    def save_backtest_result(
        self,
        run_id: int,
        *,
        summary: dict,
        equity_curve: list[dict],
        trades_json: list[dict],
    ) -> BacktestResult:
        result = self.db.scalar(select(BacktestResult).where(BacktestResult.backtest_run_id == run_id))
        if result is None:
            result = BacktestResult(
                backtest_run_id=run_id,
                summary=summary,
                equity_curve=equity_curve,
                trades_json=trades_json,
            )
        else:
            result.summary = summary
            result.equity_curve = equity_curve
            result.trades_json = trades_json
        self.db.add(result)
        self.db.flush()
        return result

    def get_backtest_result(self, run_id: int) -> BacktestResult | None:
        return self.db.scalar(select(BacktestResult).where(BacktestResult.backtest_run_id == run_id))
