from __future__ import annotations

from sqlalchemy.orm import Session

from app.domain.enums import OrderStatus, SignalAction, SignalSide
from app.engines.risk_engine import RiskEngine
from app.repositories.strategy import StrategyRepository
from app.repositories.trading import TradingRepository
from app.strategies.registry import get_strategy
from app.strategies.types import CandleInput, PositionView, StrategyContext


class StrategyEngine:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.strategy_repo = StrategyRepository(db)
        self.trading_repo = TradingRepository(db)
        self.risk_engine = RiskEngine()

    def process_window(
        self,
        *,
        strategy_row,
        config_row,
        run_row,
        symbol_code: str,
        timeframe_code: str,
        candles: list[CandleInput],
    ) -> dict:
        strategy_impl = get_strategy(strategy_row.key)
        parsed_config = strategy_impl.parse_config(config_row.settings)
        latest = candles[-1]
        open_position = self.trading_repo.get_open_position(
            strategy_row.id,
            run_row.symbol_id,
            run_row.timeframe_id,
            run_row.mode,
        )
        paper_account = self.trading_repo.get_paper_account(config_row.paper_account_id or run_row.paper_account_id)
        cash = paper_account.balance if paper_account else 10000.0
        position_view = None
        if open_position is not None:
            position_view = PositionView(
                entry_price=open_position.entry_price,
                quantity=open_position.quantity,
                stop_loss=open_position.stop_loss,
                take_profit=open_position.take_profit,
                opened_at=open_position.opened_at,
            )

        context = StrategyContext(
            strategy_id=strategy_row.id,
            strategy_key=strategy_row.key,
            symbol=symbol_code,
            timeframe=timeframe_code,
            cash=cash,
            run_mode=run_row.mode,
            config=parsed_config,
            risk_settings=config_row.risk_settings,
            position=position_view,
        )

        signal = strategy_impl.generate_signal(candles, context)
        self.strategy_repo.update_run_progress(run_row, latest.open_time)
        self.strategy_repo.update_strategy_timestamps(
            strategy_row,
            last_processed_candle_at=latest.open_time,
        )

        if open_position is not None:
            self.trading_repo.update_position_mark(open_position, latest.close)

        if signal.action == SignalAction.HOLD:
            self.db.flush()
            return {"action": "hold", "reason": signal.reason}

        risk = self.risk_engine.evaluate(strategy_impl, signal, context)
        if not risk.approved:
            self.trading_repo.create_log(
                category="strategy",
                level="warning",
                message=f"Risk rejected signal for {strategy_row.key}",
                strategy_id=strategy_row.id,
                symbol_id=run_row.symbol_id,
                context={"reason": risk.reason, "time": latest.open_time.isoformat()},
            )
            self.db.flush()
            return {"action": "rejected", "reason": risk.reason}

        execution = strategy_impl.simulate_execution(signal, latest, context, risk)
        signal_row = self.trading_repo.create_signal(
            strategy_id=strategy_row.id,
            strategy_run_id=run_row.id,
            symbol_id=run_row.symbol_id,
            timeframe_id=run_row.timeframe_id,
            candle_time=latest.open_time,
            action=signal.action.value,
            side=SignalSide.LONG.value,
            strength=signal.strength,
            payload={
                "reason": signal.reason,
                "metadata": signal.metadata,
                "stop_loss": signal.stop_loss,
                "take_profit": signal.take_profit,
            },
        )
        self.strategy_repo.update_strategy_timestamps(
            strategy_row,
            last_signal_at=latest.open_time,
            last_processed_candle_at=latest.open_time,
        )

        if signal.action == SignalAction.ENTER:
            order = self.trading_repo.create_order(
                strategy_id=strategy_row.id,
                signal_id=signal_row.id,
                position_id=None,
                symbol_id=run_row.symbol_id,
                timeframe_id=run_row.timeframe_id,
                mode=run_row.mode,
                side=SignalSide.LONG.value,
                status=OrderStatus.FILLED.value,
                requested_qty=execution.quantity,
                filled_qty=execution.quantity,
                requested_price=execution.price,
                filled_price=execution.price,
                fee=execution.fee,
                slippage=execution.slippage,
                metadata_json=execution.notes,
                filled_at=latest.open_time,
            )
            position = self.trading_repo.create_position(
                strategy_id=strategy_row.id,
                symbol_id=run_row.symbol_id,
                timeframe_id=run_row.timeframe_id,
                mode=run_row.mode,
                entry_price=execution.price,
                quantity=execution.quantity,
                stop_loss=signal.stop_loss,
                take_profit=signal.take_profit,
                opened_at=latest.open_time,
            )
            if paper_account is not None:
                remaining_balance = paper_account.balance - execution.notional - execution.fee
                self.trading_repo.update_paper_account(
                    paper_account,
                    balance=remaining_balance,
                    equity=remaining_balance + execution.notional,
                )
            self.trading_repo.create_log(
                category="strategy",
                level="info",
                message=f"{strategy_row.name} entered position",
                strategy_id=strategy_row.id,
                symbol_id=run_row.symbol_id,
                context={"order_id": order.id, "position_id": position.id, "price": execution.price},
            )
            return {"action": "enter", "price": execution.price, "quantity": execution.quantity}

        order = self.trading_repo.create_order(
            strategy_id=strategy_row.id,
            signal_id=signal_row.id,
            position_id=open_position.id if open_position else None,
            symbol_id=run_row.symbol_id,
            timeframe_id=run_row.timeframe_id,
            mode=run_row.mode,
            side=SignalSide.LONG.value,
            status=OrderStatus.FILLED.value,
            requested_qty=execution.quantity,
            filled_qty=execution.quantity,
            requested_price=execution.price,
            filled_price=execution.price,
            fee=execution.fee,
            slippage=execution.slippage,
            metadata_json=execution.notes,
            filled_at=latest.open_time,
        )
        if open_position is not None:
            gross_pnl = (execution.price - open_position.entry_price) * open_position.quantity
            net_pnl = gross_pnl - execution.fee
            self.trading_repo.create_trade(
                strategy_id=strategy_row.id,
                position_id=open_position.id,
                order_id=order.id,
                symbol_id=run_row.symbol_id,
                timeframe_id=run_row.timeframe_id,
                entry_time=open_position.opened_at,
                exit_time=latest.open_time,
                entry_price=open_position.entry_price,
                exit_price=execution.price,
                quantity=open_position.quantity,
                gross_pnl=gross_pnl,
                net_pnl=net_pnl,
                fee=execution.fee,
                slippage=execution.slippage,
                notes={"reason": signal.reason},
            )
            self.trading_repo.close_position(
                open_position,
                exit_time=latest.open_time,
                realized_pnl=net_pnl,
            )
            if paper_account is not None:
                updated_balance = paper_account.balance + execution.notional - execution.fee
                self.trading_repo.update_paper_account(
                    paper_account,
                    balance=updated_balance,
                    equity=updated_balance,
                )
            self.trading_repo.create_log(
                category="strategy",
                level="info",
                message=f"{strategy_row.name} exited position",
                strategy_id=strategy_row.id,
                symbol_id=run_row.symbol_id,
                context={"order_id": order.id, "exit_price": execution.price, "gross_pnl": gross_pnl},
            )
        return {"action": "exit", "price": execution.price, "quantity": execution.quantity}
