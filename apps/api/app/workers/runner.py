from __future__ import annotations

import time

from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger
from app.db.session import session_scope
from app.services.nightly_data_sync_service import NightlyDataSyncService
from app.services.paper_execution_service import PaperExecutionService
from app.services.pattern_scan_run_service import PatternScanRunService
from app.services.validation_run_service import ValidationRunService

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
            with session_scope() as session:
                nightly_sync_service = NightlyDataSyncService(session)
                try:
                    scheduled_processed = nightly_sync_service.process_if_due()
                    if scheduled_processed:
                        logger.info("Nightly all-data sync cycle completed")
                finally:
                    nightly_sync_service.close()

            with session_scope() as session:
                validation_service = ValidationRunService(session)
                stale_count = validation_service.mark_stale_running_runs(
                    stale_after_seconds=settings.validation_run_stale_after_seconds
                )
                if stale_count:
                    logger.warning(
                        "Validation stale sweep completed",
                        extra={"stale_runs_marked_failed": stale_count},
                    )
                queued_processed = validation_service.process_next_queued_run()
                if queued_processed:
                    logger.info("Validation worker cycle completed")

            with session_scope() as session:
                pattern_scan_service = PatternScanRunService(session)
                stale_count = pattern_scan_service.mark_stale_running_runs(
                    stale_after_seconds=settings.pattern_scan_run_stale_after_seconds
                )
                if stale_count:
                    logger.warning(
                        "Pattern scan stale sweep completed",
                        extra={"stale_runs_marked_failed": stale_count},
                    )
                queued_processed = pattern_scan_service.process_next_queued_run()
                if queued_processed:
                    logger.info("Pattern scan worker cycle completed")

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
