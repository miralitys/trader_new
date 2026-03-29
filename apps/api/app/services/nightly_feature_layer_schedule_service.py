from __future__ import annotations

from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from app.core.config import Settings, get_settings
from app.core.logging import get_logger
from app.repositories.scheduled_task_state_repository import ScheduledTaskStateRepository
from app.repositories.validation_run_repository import ValidationRunRepository
from app.services.feature_layer_service import FeatureLayerService
from app.utils.time import utc_now

logger = get_logger(__name__)

TASK_KEY = "nightly_feature_layer"
DEPENDENCY_TASK_KEY = "nightly_validation_report"
ORDERED_TIMEFRAMES = ("4h", "1h", "15m", "5m", "1m")


class NightlyFeatureLayerScheduleService:
    def __init__(
        self,
        session: Session,
        *,
        settings: Settings | None = None,
    ) -> None:
        self.session = session
        self.settings = settings or get_settings()
        self.repository = ScheduledTaskStateRepository(session)
        self.validation_runs = ValidationRunRepository(session)
        self.feature_layer_service = FeatureLayerService(session)

    def process_if_due(self) -> bool:
        if not self.settings.nightly_feature_layer_enabled:
            return False

        timezone_name = self.settings.nightly_feature_layer_timezone
        local_now = utc_now().astimezone(ZoneInfo(timezone_name))
        schedule_today = local_now.replace(
            hour=self.settings.nightly_feature_layer_hour,
            minute=self.settings.nightly_feature_layer_minute,
            second=0,
            microsecond=0,
        )
        local_date = local_now.date().isoformat()

        state = self.repository.get_or_create(
            task_key=TASK_KEY,
            timezone_name=timezone_name,
            schedule_local_hour=self.settings.nightly_feature_layer_hour,
            schedule_local_minute=self.settings.nightly_feature_layer_minute,
            lookback_days=self.settings.nightly_feature_layer_lookback_days,
        )

        if local_now < schedule_today:
            self.session.commit()
            return False

        if state.last_scheduled_date == local_date and state.last_status in {"running", "completed", "failed"}:
            self.session.commit()
            return False

        dependency_state = self.repository.get_by_task_key(DEPENDENCY_TASK_KEY)
        if dependency_state is None or dependency_state.last_scheduled_date != local_date:
            self.session.commit()
            return False

        latest_validation_run = self.validation_runs.get_latest_completed_run()
        if latest_validation_run is None or latest_validation_run.completed_at is None:
            self.session.commit()
            return False

        validation_local_date = latest_validation_run.completed_at.astimezone(ZoneInfo(timezone_name)).date().isoformat()
        validation_verdict = str((latest_validation_run.report_summary_json or {}).get("verdict") or "")
        if validation_local_date != local_date or validation_verdict not in {"PASS", "PASS WITH WARNINGS"}:
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
            "Nightly feature layer started",
            extra={
                "task_key": TASK_KEY,
                "local_date": local_date,
                "timezone": timezone_name,
                "lookback_days": state.lookback_days,
                "validation_run_id": latest_validation_run.id,
                "validation_verdict": validation_verdict,
            },
        )

        try:
            for timeframe in ORDERED_TIMEFRAMES:
                if timeframe not in self.settings.default_timeframe_list:
                    continue
                for symbol in self.settings.default_symbol_list:
                    logger.info(
                        "Nightly feature layer task started",
                        extra={
                            "task_key": TASK_KEY,
                            "local_date": local_date,
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "lookback_days": state.lookback_days,
                        },
                    )
                    self.feature_layer_service.create_run(
                        request=self._build_request(
                            symbol=symbol,
                            timeframe=timeframe,
                            lookback_days=state.lookback_days,
                        )
                    )

            refreshed_state = self.repository.get_by_task_key(TASK_KEY)
            if refreshed_state is not None:
                refreshed_state.last_completed_at = utc_now()
                refreshed_state.last_status = "completed"
                refreshed_state.last_error_text = None
                self.session.add(refreshed_state)
                self.session.commit()
            logger.info("Nightly feature layer completed", extra={"task_key": TASK_KEY, "local_date": local_date})
            return True
        except Exception as exc:
            refreshed_state = self.repository.get_by_task_key(TASK_KEY)
            if refreshed_state is not None:
                refreshed_state.last_completed_at = utc_now()
                refreshed_state.last_status = "failed"
                refreshed_state.last_error_text = str(exc)
                self.session.add(refreshed_state)
                self.session.commit()
            logger.exception("Nightly feature layer failed", extra={"task_key": TASK_KEY, "local_date": local_date})
            return False

    @staticmethod
    def _build_request(*, symbol: str, timeframe: str, lookback_days: int):
        from app.schemas.api import FeatureRunRequest

        return FeatureRunRequest(
            exchange_code="binance_us",
            symbol=symbol,
            timeframe=timeframe,
            lookback_days=lookback_days,
        )
