from app.services.nightly_feature_layer_schedule_service import DEPENDENCY_TASK_KEY, TASK_KEY, ORDERED_TIMEFRAMES


def test_nightly_feature_layer_task_keys_are_stable() -> None:
    assert TASK_KEY == "nightly_feature_layer"
    assert DEPENDENCY_TASK_KEY == "nightly_validation_report"


def test_nightly_feature_layer_uses_expected_timeframe_order() -> None:
    assert ORDERED_TIMEFRAMES == ("4h", "1h", "15m", "5m", "1m")
