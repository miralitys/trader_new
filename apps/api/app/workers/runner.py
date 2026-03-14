from __future__ import annotations

import time

from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger
from app.services.paper_execution_service import PaperExecutionService

logger = get_logger(__name__)


def main() -> None:
    configure_logging()
    settings = get_settings()
    service = PaperExecutionService()
    poll_seconds = max(settings.worker_poll_seconds, 1)

    logger.info(
        "Worker loop started",
        extra={
            "poll_seconds": poll_seconds,
            "max_candles_per_stream": settings.worker_max_candles_per_stream,
        },
    )

    while True:
        try:
            runs = service.process_active_runs(
                max_candles_per_stream=settings.worker_max_candles_per_stream
            )
            if runs:
                logger.info(
                    "Worker cycle completed",
                    extra={
                        "processed_runs": len(runs),
                        "run_ids": [run.run_id for run in runs],
                    },
                )
        except Exception:
            logger.exception("Worker cycle failed")

        time.sleep(poll_seconds)


if __name__ == "__main__":
    main()
