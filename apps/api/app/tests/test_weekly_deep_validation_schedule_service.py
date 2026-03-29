from app.services.weekly_deep_validation_schedule_service import (
    DEPENDENCY_TASK_KEY,
    TASK_KEY,
    WEEKDAY_MAP,
)


def test_weekly_deep_validation_task_keys_are_stable() -> None:
    assert TASK_KEY == "weekly_deep_validation_report"
    assert DEPENDENCY_TASK_KEY == "nightly_feature_layer"


def test_weekly_deep_validation_defaults_to_friday_schedule() -> None:
    assert WEEKDAY_MAP["FRI"] == 4
    assert set(WEEKDAY_MAP) == {"MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"}
