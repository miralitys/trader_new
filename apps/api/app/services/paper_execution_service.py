from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from app.core.logging import get_logger
from app.db.session import SessionLocal
from app.engines.paper_engine import PaperEngine, PaperPositionState, PaperRuntimeState
from app.models.enums import SignalType
from app.repositories.candle_repository import CandleRepository
from app.repositories.order_repository import OrderRepository
from app.repositories.paper_account_repository import PaperAccountRepository
from app.repositories.position_repository import PositionRepository
from app.repositories.signal_repository import SignalRepository
from app.repositories.strategy_run_repository import StrategyRunRepository
from app.repositories.trade_repository import TradeRepository
from app.schemas.backtest import BacktestCandle
from app.schemas.paper import PaperRunResponse, PaperRunStartRequest
from app.strategies.registry import get_strategy
from app.utils.time import ensure_utc, parse_iso8601, to_iso8601, utc_now

logger = get_logger(__name__)


@dataclass
class PaperCounters:
    processed_candles: int = 0
    signals_created: int = 0
    orders_created: int = 0
    trades_created: int = 0


class PaperExecutionService:
    def __init__(self, engine: Optional[PaperEngine] = None) -> None:
        self.engine = engine or PaperEngine()

    def start_run(self, request: PaperRunStartRequest) -> PaperRunResponse:
        session = SessionLocal()
        try:
            strategy = get_strategy(request.strategy_code)
            strategy_run_repository = StrategyRunRepository(session)
            paper_account_repository = PaperAccountRepository(session)

            strategy_row = strategy_run_repository.ensure_strategy(
                code=strategy.key,
                name=strategy.name,
                description=strategy.description,
            )
            existing_run = strategy_run_repository.get_active_paper_run_for_strategy(strategy_row.id)
            if existing_run is not None:
                raise ValueError(f"Active paper run already exists for strategy {request.strategy_code}")

            account = paper_account_repository.ensure_account(
                strategy_id=strategy_row.id,
                balance=request.initial_balance,
                currency=request.currency,
                reset_existing=True,
            )
            metadata_json = {
                "exchange_code": request.exchange_code,
                "fee": str(request.fee),
                "slippage": str(request.slippage),
                "strategy_config_override": request.strategy_config_override,
                "last_processed_by_stream": {},
                "open_positions_runtime": {},
                **request.metadata,
            }
            run = strategy_run_repository.create_paper_run(
                strategy_id=strategy_row.id,
                symbols=request.symbols,
                timeframes=request.timeframes,
                metadata_json=metadata_json,
            )
            strategy_run_repository.mark_running(run, started_at=utc_now())
            session.commit()

            return PaperRunResponse(
                run_id=run.id,
                strategy_code=strategy.key,
                status=run.status.value,
                symbols=run.symbols_json,
                timeframes=run.timeframes_json,
                exchange_code=request.exchange_code,
                account_balance=account.balance,
                currency=account.currency,
                last_processed_candle_at=run.last_processed_candle_at,
            )
        finally:
            session.close()

    def stop_run(self, run_id: int, reason: str = "manual_stop") -> PaperRunResponse:
        session = SessionLocal()
        try:
            strategy_run_repository = StrategyRunRepository(session)
            paper_account_repository = PaperAccountRepository(session)
            run = strategy_run_repository.get_by_id(run_id)
            if run is None:
                raise ValueError(f"Paper run {run_id} not found")

            strategy = strategy_run_repository.get_strategy_by_id(run.strategy_id)
            if strategy is None:
                raise ValueError(f"Strategy for run {run_id} not found")

            strategy_run_repository.mark_stopped(run, stopped_at=utc_now(), reason=reason)
            account = paper_account_repository.ensure_account(
                strategy_id=run.strategy_id,
                balance=Decimal("10000"),
            )
            session.commit()

            return PaperRunResponse(
                run_id=run.id,
                strategy_code=strategy.code,
                status=run.status.value,
                symbols=run.symbols_json,
                timeframes=run.timeframes_json,
                exchange_code=run.metadata_json.get("exchange_code", "coinbase"),
                account_balance=account.balance,
                currency=account.currency,
                last_processed_candle_at=run.last_processed_candle_at,
            )
        finally:
            session.close()

    def process_active_runs(self, max_candles_per_stream: int = 100) -> list[PaperRunResponse]:
        session = SessionLocal()
        try:
            strategy_run_repository = StrategyRunRepository(session)
            run_ids = [run.id for run in strategy_run_repository.list_active_paper_runs()]
        finally:
            session.close()

        results: list[PaperRunResponse] = []
        for run_id in run_ids:
            try:
                results.append(
                    self.process_run(run_id, max_candles_per_stream=max_candles_per_stream)
                )
            except Exception:
                logger.exception(
                    "Skipping failed paper run during worker cycle",
                    extra={"run_id": run_id},
                )
        return results

    def process_run(self, run_id: int, max_candles_per_stream: int = 100) -> PaperRunResponse:
        session = SessionLocal()
        counters = PaperCounters()
        try:
            candle_repository = CandleRepository(session)
            strategy_run_repository = StrategyRunRepository(session)
            signal_repository = SignalRepository(session)
            order_repository = OrderRepository(session)
            position_repository = PositionRepository(session)
            trade_repository = TradeRepository(session)
            paper_account_repository = PaperAccountRepository(session)

            run = strategy_run_repository.get_by_id(run_id)
            if run is None:
                raise ValueError(f"Paper run {run_id} not found")
            if run.status.value != "running":
                raise ValueError(f"Paper run {run_id} is not running")

            strategy_row = strategy_run_repository.get_strategy_by_id(run.strategy_id)
            if strategy_row is None:
                raise ValueError(f"Strategy for run {run_id} not found")

            strategy = get_strategy(strategy_row.code)
            metadata = dict(run.metadata_json or {})
            exchange_code = metadata.get("exchange_code", "coinbase")
            fee_rate = Decimal(str(metadata.get("fee", "0.001")))
            slippage_rate = Decimal(str(metadata.get("slippage", "0.0005")))
            strategy_override = dict(metadata.get("strategy_config_override", {}))

            account = paper_account_repository.ensure_account(
                strategy_id=run.strategy_id,
                balance=Decimal("10000"),
            )
            cash = Decimal(str(account.balance))

            open_positions = {
                position.symbol: position for position in position_repository.list_open_positions(run.id)
            }
            stream_histories: dict[str, list[BacktestCandle]] = {}
            merged_stream: list[tuple[datetime, str, str, str, BacktestCandle]] = []
            watermarks = dict(metadata.get("last_processed_by_stream", {}))

            for symbol in run.symbols_json:
                for timeframe in run.timeframes_json:
                    stream_key = f"{symbol}|{timeframe}"
                    after_time = self._parse_watermark(watermarks.get(stream_key))
                    candles = candle_repository.list_candles_after(
                        exchange_code=exchange_code,
                        symbol_code=symbol,
                        timeframe=timeframe,
                        after_time=after_time,
                        limit=max_candles_per_stream,
                    )
                    history_bucket = stream_histories.setdefault(stream_key, [])
                    for candle in candles:
                        payload = BacktestCandle(
                            open_time=candle.open_time,
                            open=candle.open,
                            high=candle.high,
                            low=candle.low,
                            close=candle.close,
                            volume=candle.volume,
                        )
                        merged_stream.append((payload.open_time, symbol, timeframe, stream_key, payload))
                        history_bucket.append(payload)

            merged_stream.sort(key=lambda item: (item[0], item[1], item[2]))

            for candle_time, symbol, timeframe, stream_key, candle in merged_stream:
                position_model = open_positions.get(symbol)
                runtime_state = PaperRuntimeState(
                    cash=cash,
                    position=self._build_runtime_position(run, symbol, position_model),
                )
                history = [
                    item
                    for item in stream_histories.get(stream_key, [])
                    if item.open_time <= candle_time
                ]
                result = self.engine.process_candle(
                    strategy=strategy,
                    symbol=symbol,
                    timeframe=timeframe,
                    candle=candle,
                    history=history,
                    state=runtime_state,
                    fee_rate=fee_rate,
                    slippage_rate=slippage_rate,
                    strategy_config_override=strategy_override,
                    runtime_metadata={
                        "strategy_run_id": run.id,
                        "stream_key": stream_key,
                    },
                )

                signal_id = None
                if result.signal_event is not None:
                    signal = signal_repository.create_signal(
                        strategy_run_id=run.id,
                        symbol=symbol,
                        timeframe=timeframe,
                        signal_type=SignalType(result.signal_event.signal_type),
                        signal_strength=result.signal_event.signal_strength,
                        payload_json=result.signal_event.payload_json,
                        candle_time=result.signal_event.candle_time,
                    )
                    signal_id = signal.id
                    counters.signals_created += 1

                for order_event in result.orders:
                    order_repository.create_filled_order(
                        strategy_run_id=run.id,
                        symbol=symbol,
                        qty=order_event.qty,
                        price=order_event.price,
                        linked_signal_id=signal_id if order_event.linked_to_signal else None,
                    )
                    counters.orders_created += 1

                previous_position = runtime_state.position
                next_position = result.state.position
                if previous_position is None and next_position is not None:
                    open_positions[symbol] = position_repository.open_position(
                        strategy_run_id=run.id,
                        symbol=symbol,
                        qty=next_position.qty,
                        avg_entry_price=next_position.entry_price,
                        stop_price=next_position.stop_price,
                        take_profit_price=next_position.take_profit_price,
                        opened_at=next_position.entry_time,
                    )
                    strategy_run_repository.store_open_position_runtime(
                        run=run,
                        symbol=symbol,
                        runtime_payload={
                            "entry_time": to_iso8601(next_position.entry_time),
                            "entry_fee": str(next_position.entry_fee),
                            "entry_slippage": str(next_position.entry_slippage),
                            "capital_committed": str(next_position.capital_committed),
                            "entry_metadata": next_position.entry_metadata,
                        },
                    )
                    logger.info(
                        "Paper position opened",
                        extra={
                            "run_id": run.id,
                            "strategy_code": strategy.code,
                            "symbol": symbol,
                            "qty": str(next_position.qty),
                            "entry_price": str(next_position.entry_price),
                        },
                    )
                elif previous_position is not None and next_position is None and position_model is not None:
                    position_repository.close_position(position_model, closed_at=candle_time)
                    open_positions.pop(symbol, None)
                    strategy_run_repository.clear_open_position_runtime(run=run, symbol=symbol)

                if result.trade_event is not None:
                    trade_repository.create_trade(
                        strategy_run_id=run.id,
                        symbol=symbol,
                        entry_price=result.trade_event.entry_price,
                        exit_price=result.trade_event.exit_price,
                        qty=result.trade_event.qty,
                        pnl=result.trade_event.pnl,
                        pnl_pct=result.trade_event.pnl_pct,
                        fees=result.trade_event.fees,
                        slippage=result.trade_event.slippage,
                        opened_at=result.trade_event.opened_at,
                        closed_at=result.trade_event.closed_at,
                        metadata_json=result.trade_event.metadata_json,
                    )
                    counters.trades_created += 1
                    logger.info(
                        "Paper trade closed",
                        extra={
                            "run_id": run.id,
                            "strategy_code": strategy.code,
                            "symbol": symbol,
                            "pnl": str(result.trade_event.pnl),
                            "exit_reason": result.trade_event.metadata_json.get("exit_reason"),
                        },
                    )

                cash = result.state.cash
                paper_account_repository.update_balance(account, cash)
                strategy_run_repository.update_last_processed(run, candle_time=candle_time, stream_key=stream_key)
                session.commit()
                counters.processed_candles += 1

            return PaperRunResponse(
                run_id=run.id,
                strategy_code=strategy_row.code,
                status=run.status.value,
                symbols=run.symbols_json,
                timeframes=run.timeframes_json,
                exchange_code=exchange_code,
                account_balance=account.balance,
                currency=account.currency,
                last_processed_candle_at=run.last_processed_candle_at,
                processed_candles=counters.processed_candles,
                signals_created=counters.signals_created,
                orders_created=counters.orders_created,
                trades_created=counters.trades_created,
            )
        except Exception as exc:
            session.rollback()
            logger.exception("Paper execution failed", extra={"run_id": run_id})
            try:
                strategy_run_repository = StrategyRunRepository(session)
                run = strategy_run_repository.get_by_id(run_id)
                if run is not None:
                    strategy_run_repository.mark_failed(run, stopped_at=utc_now(), error_text=str(exc))
                    session.commit()
            except Exception:
                session.rollback()
                logger.exception("Failed to mark paper run as failed", extra={"run_id": run_id})
            raise
        finally:
            session.close()

    def _parse_watermark(self, value: object) -> Optional[datetime]:
        if not isinstance(value, str) or not value:
            return None
        return parse_iso8601(value)

    def _build_runtime_position(
        self,
        run,
        symbol: str,
        position_model,
    ) -> Optional[PaperPositionState]:
        if position_model is None:
            return None

        metadata = dict(run.metadata_json or {})
        runtime_payload = dict(metadata.get("open_positions_runtime", {})).get(symbol, {})
        entry_time = runtime_payload.get("entry_time")
        if entry_time is None:
            entry_time_value = position_model.opened_at
        else:
            entry_time_value = parse_iso8601(entry_time)

        return PaperPositionState(
            entry_time=entry_time_value,
            entry_price=Decimal(str(position_model.avg_entry_price)),
            qty=Decimal(str(position_model.qty)),
            entry_fee=Decimal(str(runtime_payload.get("entry_fee", "0"))),
            entry_slippage=Decimal(str(runtime_payload.get("entry_slippage", "0"))),
            capital_committed=Decimal(
                str(
                    runtime_payload.get(
                        "capital_committed",
                        Decimal(str(position_model.qty)) * Decimal(str(position_model.avg_entry_price)),
                    )
                )
            ),
            stop_price=Decimal(str(position_model.stop_price)) if position_model.stop_price is not None else None,
            take_profit_price=(
                Decimal(str(position_model.take_profit_price))
                if position_model.take_profit_price is not None
                else None
            ),
            entry_metadata=dict(runtime_payload.get("entry_metadata", {})),
        )
