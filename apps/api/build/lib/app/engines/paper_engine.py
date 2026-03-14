from __future__ import annotations

from sqlalchemy.orm import Session

from app.repositories.market_data import MarketDataRepository
from app.repositories.strategy import StrategyRepository
from app.engines.strategy_engine import StrategyEngine
from app.strategies.types import CandleInput


class PaperTradingEngine:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.market_repo = MarketDataRepository(db)
        self.strategy_repo = StrategyRepository(db)
        self.strategy_engine = StrategyEngine(db)

    def run_cycle(self) -> list[dict]:
        results: list[dict] = []
        active_runs = self.strategy_repo.list_active_runs(mode="paper_trading")
        for run in active_runs:
            strategy = self.strategy_repo.get_strategy(run.strategy_id)
            config = self.strategy_repo.get_config(run.strategy_id)
            symbol = self.market_repo.get_symbol_by_id(run.symbol_id)
            timeframe = self.market_repo.get_timeframe_by_id(run.timeframe_id)
            if strategy is None or config is None or symbol is None or timeframe is None:
                continue
            new_candles = self.market_repo.list_new_candles(run.symbol_id, run.timeframe_id, run.last_processed_candle)
            for candle in new_candles:
                window = self.market_repo.list_recent_window(run.symbol_id, run.timeframe_id, candle.open_time, 200)
                if not window:
                    continue
                result = self.strategy_engine.process_window(
                    strategy_row=strategy,
                    config_row=config,
                    run_row=run,
                    symbol_code=symbol.symbol,
                    timeframe_code=timeframe.code,
                    candles=[
                        CandleInput(
                            open_time=item.open_time,
                            close_time=item.close_time,
                            open=item.open,
                            high=item.high,
                            low=item.low,
                            close=item.close,
                            volume=item.volume,
                        )
                        for item in window
                    ],
                )
                results.append(
                    {
                        "run_id": run.id,
                        "strategy_id": strategy.id,
                        "symbol": symbol.symbol,
                        "timeframe": timeframe.code,
                        "result": result,
                    }
                )
        return results
