from __future__ import annotations

from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from app.core.config import Settings, get_settings
from app.core.logging import get_logger
from app.repositories.scheduled_task_state_repository import ScheduledTaskStateRepository
from app.schemas.api import DataValidationRequest
from app.services.validation_run_service import ValidationRunService
from app.utils.time import utc_now

logger = get_logger(__name__)

TASK_KEY = "nightly_validation_report"
DEPENDENCY_TASK_KEY = "nightly_all_data_sync"


class NightlyValidationScheduleService:
    def __init__(
        self,
        session: Session,
        *,
        settings: Settings | None = None,
    ) -> None:
        self.session = session
        self.settings = settings or get_settings()
        self.repository = ScheduledTaskStateRepository(session)
        self.validation_run_service = ValidationRunService(session)

    def process_if_due(self) -> bool:
        if not self.settings.nightly_validation_enabled:
            return False

        timezone_name = self.settings.nightly_validation_timezone
        local_now = utc_now().astimezone(ZoneInfo(timezone_name))
        schedule_today = local_now.replace(
            hour=self.settings.nightly_validation_hour,
            minute=self.settings.nightly_validation_minute,
            second=0,
            microsecond=0,
        )
        local_date = local_now.date().isoformat()

        state = self.repository.get_or_create(
            task_key=TASK_KEY,
            timezone_name=timezone_name,
            schedule_local_hour=self.settings.nightly_validation_hour,
            schedule_local_minute=self.settings.nightly_validation_minute,
            lookback_days=self.settings.nightly_validation_lookback_days,
        )

        if local_now < schedule_today:
            self.session.commit()
            return False

        if state.last_scheduled_date == local_date and state.last_status in {"queued", "running", "completed", "failed"}:
            self.session.commit()
            return False

        dependency_state = self.repository.get_by_task_key(DEPENDENCY_TASK_KEY)
        if dependency_state is None or dependency_state.last_scheduled_date != local_date or dependency_state.last_status != "completed":
            self.session.commit()
            return False

        request = DataValidationRequest(
            exchange_code="binance_us",
            symbols=self.settings.default_symbol_list,
            timeframes=self.settings.default_timeframe_list,
            lookback_days=self.settings.nightly_validation_lookback_days,
            sample_limit=self.settings.nightly_validation_sample_limit,
            perform_resync=False,
            resync_days=14,
        )
        run = self.validation_run_service.create_run(request)

        refreshed_state = self.repository.get_or_create(
            task_key=TASK_KEY,
            timezone_name=timezone_name,
            schedule_local_hour=self.settings.nightly_validation_hour,
            schedule_local_minute=self.settings.nightly_validation_minute,
            lookback_days=self.settings.nightly_validation_lookback_days,
        )
        refreshed_state.last_scheduled_date = local_date
        refreshed_state.last_started_at = utc_now()
        refreshed_state.last_completed_at = None
        refreshed_state.last_status = "queued"
        refreshed_state.last_error_text = None
        self.session.add(refreshed_state)
        self.session.commit()

        logger.info(
            "Nightly validation queued",
            extra={
                "task_key": TASK_KEY,
                "local_date": local_date,
                "run_id": run.id,
                "timezone": timezone_name,
                "lookback_days": request.lookback_days,
            },
        )
        return True
