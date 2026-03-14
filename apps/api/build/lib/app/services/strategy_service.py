from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.domain.enums import PositionStatus, StrategyStatus
from app.domain.schemas.strategy import StrategyConfigUpdate, StrategyDetail, StrategyListItem
from app.repositories.market_data import MarketDataRepository
from app.repositories.strategy import StrategyRepository
from app.repositories.trading import TradingRepository
from app.services.metrics import calculate_trade_metrics
from app.strategies.registry import get_strategy


class StrategyService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.strategy_repo = StrategyRepository(db)
        self.market_repo = MarketDataRepository(db)
        self.trading_repo = TradingRepository(db)

    def list_strategies(self) -> list[StrategyListItem]:
        items: list[StrategyListItem] = []
        for strategy in self.strategy_repo.list_strategies():
            items.append(self._build_strategy_item(strategy))
        return items

    def get_strategy_detail(self, strategy_id: int) -> StrategyDetail:
        strategy = self.strategy_repo.get_strategy(strategy_id)
        if strategy is None:
            raise ValueError("strategy not found")
        config = self.strategy_repo.get_config(strategy_id)
        trades = self.trading_repo.list_trades(strategy_id=strategy_id, limit=500)
        metrics = calculate_trade_metrics(trades)
        runs = self.strategy_repo.list_runs(strategy_id)
        logs = self.trading_repo.list_logs(strategy_id=strategy_id, limit=200)

        return StrategyDetail(
            **self._build_strategy_item(strategy).model_dump(),
            config={
                "settings": config.settings if config else {},
                "risk_settings": config.risk_settings if config else {},
                "symbols": config.symbols if config else [],
                "timeframes": config.timeframes if config else [],
                "paper_account_id": config.paper_account_id if config else None,
            },
            runs=runs,
            signals=self.trading_repo.list_signals(strategy_id=strategy_id, limit=200),
            positions=self.trading_repo.list_positions(strategy_id=strategy_id, limit=200),
            trades=trades,
            logs=[
                {
                    "id": log.id,
                    "category": log.category,
                    "level": log.level,
                    "message": log.message,
                    "context": log.context,
                    "created_at": log.created_at,
                }
                for log in logs
            ],
            equity_curve=metrics["equity_curve"],
        )

    def start_strategy(self, strategy_id: int, mode: str) -> dict:
        strategy = self.strategy_repo.get_strategy(strategy_id)
        if strategy is None:
            raise ValueError("strategy not found")
        config = self.strategy_repo.get_config(strategy_id)
        if config is None:
            raise ValueError("strategy config missing")

        exchange = self.market_repo.get_exchange_by_slug("coinbase")
        if exchange is None:
            raise ValueError("coinbase exchange missing")

        self.strategy_repo.stop_runs(strategy_id, datetime.now(timezone.utc))
        symbol_ids = []
        timeframe_ids = []
        for symbol_code in config.symbols:
            symbol = self.market_repo.get_symbol(exchange.id, symbol_code)
            if symbol is not None:
                symbol_ids.append(symbol.id)
        for timeframe_code in config.timeframes:
            timeframe = self.market_repo.get_timeframe(timeframe_code)
            if timeframe is not None:
                timeframe_ids.append(timeframe.id)
        if not symbol_ids or not timeframe_ids:
            raise ValueError("strategy config has no valid symbols/timeframes")

        status = StrategyStatus.PAPER_TRADING.value if mode == "paper_trading" else StrategyStatus.RUNNING.value
        runs = self.strategy_repo.create_runs(
            strategy_id=strategy_id,
            paper_account_id=config.paper_account_id,
            symbol_ids=symbol_ids,
            timeframe_ids=timeframe_ids,
            mode=mode,
            status=status,
        )
        self.strategy_repo.update_strategy_status(strategy, status)
        self.trading_repo.create_log(
            category="strategy",
            level="info",
            message=f"Started strategy {strategy.name}",
            strategy_id=strategy.id,
            context={"mode": mode, "runs_created": len(runs)},
        )
        self.db.commit()
        return {"strategy_id": strategy_id, "status": status, "runs_created": len(runs)}

    def stop_strategy(self, strategy_id: int) -> dict:
        strategy = self.strategy_repo.get_strategy(strategy_id)
        if strategy is None:
            raise ValueError("strategy not found")
        self.strategy_repo.stop_runs(strategy_id, datetime.now(timezone.utc))
        self.strategy_repo.update_strategy_status(strategy, StrategyStatus.STOPPED.value)
        self.trading_repo.create_log(
            category="strategy",
            level="info",
            message=f"Stopped strategy {strategy.name}",
            strategy_id=strategy.id,
        )
        self.db.commit()
        return {"strategy_id": strategy_id, "status": StrategyStatus.STOPPED.value}

    def update_config(self, strategy_id: int, payload: StrategyConfigUpdate) -> dict:
        strategy = self.strategy_repo.get_strategy(strategy_id)
        if strategy is None:
            raise ValueError("strategy not found")
        current = self.strategy_repo.get_config(strategy_id)
        strategy_impl = get_strategy(strategy.key)
        merged_settings = {**(current.settings if current else {}), **payload.settings}
        validated_settings = strategy_impl.parse_config(merged_settings).model_dump()
        merged_risk = {**(current.risk_settings if current else {}), **payload.risk_settings}
        symbols = payload.symbols or (current.symbols if current else [])
        timeframes = payload.timeframes or (current.timeframes if current else [])
        paper_account_id = payload.paper_account_id if payload.paper_account_id is not None else (
            current.paper_account_id if current else None
        )
        config = self.strategy_repo.upsert_config(
            strategy_id=strategy_id,
            settings=validated_settings,
            risk_settings=merged_risk,
            symbols=symbols,
            timeframes=timeframes,
            paper_account_id=paper_account_id,
        )
        self.trading_repo.create_log(
            category="strategy",
            level="info",
            message=f"Updated config for {strategy.name}",
            strategy_id=strategy.id,
            context={"symbols": symbols, "timeframes": timeframes},
        )
        self.db.commit()
        return {
            "strategy_id": strategy_id,
            "config": {
                "settings": config.settings,
                "risk_settings": config.risk_settings,
                "symbols": config.symbols,
                "timeframes": config.timeframes,
                "paper_account_id": config.paper_account_id,
            },
        }

    def get_config(self, strategy_id: int) -> dict:
        config = self.strategy_repo.get_config(strategy_id)
        if config is None:
            raise ValueError("strategy config not found")
        return {
            "settings": config.settings,
            "risk_settings": config.risk_settings,
            "symbols": config.symbols,
            "timeframes": config.timeframes,
            "paper_account_id": config.paper_account_id,
        }

    def _build_strategy_item(self, strategy) -> StrategyListItem:
        config = self.strategy_repo.get_config(strategy.id)
        trades = self.trading_repo.list_trades(strategy_id=strategy.id, limit=500)
        metrics = calculate_trade_metrics(trades)
        open_positions = len(
            self.trading_repo.list_positions(
                strategy_id=strategy.id,
                status=PositionStatus.OPEN.value,
                limit=100,
            )
        )
        return StrategyListItem(
            id=strategy.id,
            key=strategy.key,
            name=strategy.name,
            description=strategy.description,
            status=strategy.status,
            is_enabled=strategy.is_enabled,
            last_signal_at=strategy.last_signal_at,
            last_processed_candle_at=strategy.last_processed_candle_at,
            metrics=metrics,
            open_positions=open_positions,
        )
