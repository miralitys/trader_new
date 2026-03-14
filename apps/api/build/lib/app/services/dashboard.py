from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.domain.schemas.dashboard import DashboardRead, DashboardStrategyCard
from app.repositories.strategy import StrategyRepository
from app.repositories.trading import TradingRepository
from app.services.market_data import MarketDataService
from app.services.strategy_service import StrategyService


class DashboardService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.strategy_service = StrategyService(db)
        self.strategy_repo = StrategyRepository(db)
        self.trading_repo = TradingRepository(db)
        self.market_data_service = MarketDataService(db)

    def get_dashboard(self) -> DashboardRead:
        strategy_cards = []
        for item in self.strategy_service.list_strategies():
            config = self.strategy_repo.get_config(item.id)
            metrics = item.metrics
            strategy_cards.append(
                DashboardStrategyCard(
                    id=item.id,
                    key=item.key,
                    name=item.name,
                    status=item.status,
                    pnl=metrics["net_pnl"],
                    win_rate=metrics["win_rate"],
                    number_of_trades=metrics["total_trades"],
                    max_drawdown=metrics["max_drawdown"],
                    profit_factor=metrics["profit_factor"],
                    expectancy=metrics["expectancy"],
                    open_positions=item.open_positions,
                    last_signal_time=item.last_signal_at,
                    last_processed_candle=item.last_processed_candle_at,
                    symbols=config.symbols if config else [],
                    timeframes=config.timeframes if config else [],
                )
            )
        recent_backtests = [
            {
                "id": run.id,
                "strategy_id": run.strategy_id,
                "status": run.status,
                "created_at": run.created_at,
            }
            for run in self.trading_repo.list_backtest_runs(limit=10)
        ]
        return DashboardRead(
            generated_at=datetime.now(timezone.utc),
            strategies=strategy_cards,
            sync_status=self.market_data_service.get_status().model_dump(mode="json"),
            recent_backtests=recent_backtests,
        )
