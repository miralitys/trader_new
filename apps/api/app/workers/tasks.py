from __future__ import annotations

from app.core.logging import get_logger

logger = get_logger(__name__)


def schedule_market_data_sync() -> dict[str, str]:
    logger.info("Market data sync placeholder invoked.")
    return {"task": "market_data_sync", "status": "placeholder"}


def run_paper_cycle() -> dict[str, str]:
    logger.info("Paper engine cycle placeholder invoked.")
    return {"task": "paper_cycle", "status": "placeholder"}


def run_backtest_job() -> dict[str, str]:
    logger.info("Backtest job placeholder invoked.")
    return {"task": "backtest_job", "status": "placeholder"}
