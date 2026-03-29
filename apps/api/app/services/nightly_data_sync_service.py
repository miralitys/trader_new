from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from app.core.config import Settings, get_settings
from app.core.logging import get_logger
from app.repositories.scheduled_task_state_repository import ScheduledTaskStateRepository
from app.services.market_data_service import MarketDataService
from app.utils.time import ensure_utc, utc_now

logger = get_logger(__name__)

TASK_KEY = "nightly_all_data_sync"
ORDERED_TIMEFRAMES = ("4h", "1h", "15m", "5m", "1m")


@dataclass(frozen=True)
class ScheduledSyncTask:
    symbol: str
    timeframe: str


class NightlyDataSyncService:
    def __init__(
        self,
        session: Session,
        *,
        settings: Settings | None = None,
        market_data_service: MarketDataService | None = None,
    ) -> None:
        self.session = session
        self.settings = settings or get_settings()
        self.repository = ScheduledTaskStateRepository(session)
        self.market_data_service = market_data_service or MarketDataService(settings=self.settings)

    def close(self) -> None:
        self.market_data_service.close()

    def process_if_due(self) -> bool:
        if not self.settings.nightly_all_data_sync_enabled:
            return False

        timezone_name = self.settings.nightly_all_data_sync_timezone
        local_now = utc_now().astimezone(ZoneInfo(timezone_name))
        schedule_today = local_now.replace(
            hour=self.settings.nightly_all_data_sync_hour,
            minute=self.settings.nightly_all_data_sync_minute,
            second=0,
            microsecond=0,
        )
        local_date = local_now.date().isoformat()

        state = self.repository.get_or_create(
            task_key=TASK_KEY,
            timezone_name=timezone_name,
            schedule_local_hour=self.settings.nightly_all_data_sync_hour,
            schedule_local_minute=self.settings.nightly_all_data_sync_minute,
            lookback_days=self.settings.nightly_all_data_sync_lookback_days,
        )

        if local_now < schedule_today:
            self.session.commit()
            return False

        if state.last_scheduled_date == local_date and state.last_status in {"running", "completed", "failed"}:
            self.session.commit()
            return False

        state.last_scheduled_date = local_date
        state.last_started_at = utc_now()
        state.last_completed_at = None
        state.last_status = "running"
        state.last_error_text = None
        self.session.add(state)
        self.session.commit()

        logger.info(
            "Nightly all-data sync started",
            extra={
                "task_key": TASK_KEY,
                "local_date": local_date,
                "timezone": timezone_name,
                "lookback_days": state.lookback_days,
            },
        )

        try:
            end_at = utc_now()
            start_at = end_at - timedelta(days=state.lookback_days)
            tasks = build_nightly_sync_plan(self.settings.default_symbol_list, self.settings.default_timeframe_list)

            total_jobs = len(tasks)
            for index, task in enumerate(tasks, start=1):
                logger.info(
                    "Nightly all-data sync task started",
                    extra={
                        "task_key": TASK_KEY,
                        "job_index": index,
                        "job_total": total_jobs,
                        "symbol": task.symbol,
                        "timeframe": task.timeframe,
                        "start_at": ensure_utc(start_at).isoformat(),
                        "end_at": ensure_utc(end_at).isoformat(),
                    },
                )
                self.market_data_service.manual_sync(
                    exchange_code="binance_us",
                    symbol=task.symbol,
                    timeframe=task.timeframe,
                    start_at=start_at,
                    end_at=end_at,
                )

            refreshed_state = self.repository.get_by_task_key(TASK_KEY)
            if refreshed_state is not None:
                refreshed_state.last_completed_at = utc_now()
                refreshed_state.last_status = "completed"
                refreshed_state.last_error_text = None
                self.session.add(refreshed_state)
                self.session.commit()

            logger.info(
                "Nightly all-data sync completed",
                extra={"task_key": TASK_KEY, "local_date": local_date, "job_total": total_jobs},
            )
            return True
        except Exception as exc:
            refreshed_state = self.repository.get_by_task_key(TASK_KEY)
            if refreshed_state is not None:
                refreshed_state.last_completed_at = utc_now()
                refreshed_state.last_status = "failed"
                refreshed_state.last_error_text = str(exc)
                self.session.add(refreshed_state)
                self.session.commit()
            logger.exception("Nightly all-data sync failed", extra={"task_key": TASK_KEY, "local_date": local_date})
            return False


def build_nightly_sync_plan(symbols: list[str], configured_timeframes: list[str]) -> list[ScheduledSyncTask]:
    enabled_timeframes = set(configured_timeframes)
    tasks: list[ScheduledSyncTask] = []
    for timeframe in ORDERED_TIMEFRAMES:
        if timeframe not in enabled_timeframes:
            continue
        for symbol in symbols:
            tasks.append(ScheduledSyncTask(symbol=symbol, timeframe=timeframe))
    return tasks
