from __future__ import annotations

from datetime import datetime

from app.core.celery_app import celery_app
from app.core.config import get_settings
from app.core.database import session_scope
from app.engines.paper_engine import PaperTradingEngine
from app.services.backtest_service import BacktestService
from app.services.market_data import MarketDataService


@celery_app.task(name="app.background.tasks.sync_recent_market_data")
def sync_recent_market_data() -> dict:
    settings = get_settings()
    with session_scope() as db:
        service = MarketDataService(db)
        jobs = service.schedule_sync_jobs(
            symbols=settings.default_symbol_list,
            timeframes=settings.default_timeframe_list,
            start=None,
            end=None,
            full_resync=False,
        )
        return {"scheduled_jobs": jobs}


@celery_app.task(name="app.background.tasks.sync_symbol_timeframe")
def sync_symbol_timeframe(
    job_id: int,
    symbol_code: str,
    timeframe_code: str,
    start_iso: str | None,
    end_iso: str | None,
    full_resync: bool = False,
) -> dict:
    with session_scope() as db:
        service = MarketDataService(db)
        try:
            start = datetime.fromisoformat(start_iso) if start_iso else None
            end = datetime.fromisoformat(end_iso) if end_iso else None
            return service.sync_history(
                job_id=job_id,
                symbol_code=symbol_code,
                timeframe_code=timeframe_code,
                start=start,
                end=end,
                full_resync=full_resync,
            )
        except Exception as exc:
            service.fail_job(job_id, str(exc))
            raise


@celery_app.task(name="app.background.tasks.run_active_paper_trading")
def run_active_paper_trading() -> dict:
    with session_scope() as db:
        engine = PaperTradingEngine(db)
        results = engine.run_cycle()
        return {"processed": len(results), "results": results[-10:]}


@celery_app.task(name="app.background.tasks.run_backtest")
def run_backtest(run_id: int) -> dict:
    with session_scope() as db:
        service = BacktestService(db)
        return service.execute_run(run_id)
