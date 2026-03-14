from __future__ import annotations

from celery import Celery

from app.core.config import get_settings

settings = get_settings()

celery_app = Celery("trader", broker=settings.redis_url, backend=settings.redis_url)
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    beat_schedule={
        "sync-recent-market-data": {
            "task": "app.background.tasks.sync_recent_market_data",
            "schedule": 300.0,
        },
        "run-paper-trading-cycle": {
            "task": "app.background.tasks.run_active_paper_trading",
            "schedule": 60.0,
        },
    },
)
celery_app.autodiscover_tasks(["app.background"])
