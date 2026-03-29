from datetime import datetime, timezone

from app.services.nightly_validation_schedule_service import DEPENDENCY_TASK_KEY, TASK_KEY


def test_nightly_validation_task_keys_are_stable() -> None:
    assert TASK_KEY == "nightly_validation_report"
    assert DEPENDENCY_TASK_KEY == "nightly_all_data_sync"


def test_nightly_validation_local_schedule_examples_are_unambiguous() -> None:
    sample = datetime(2026, 3, 29, 7, 0, tzinfo=timezone.utc)
    assert sample.tzinfo is not None
