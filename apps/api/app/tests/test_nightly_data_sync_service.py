from app.services.nightly_data_sync_service import build_nightly_sync_plan


def test_build_nightly_sync_plan_uses_batch_timeframe_order() -> None:
    tasks = build_nightly_sync_plan(
        ["BTC-USDT", "ETH-USDT"],
        ["1m", "5m", "15m", "1h", "4h"],
    )

    assert [(task.symbol, task.timeframe) for task in tasks[:6]] == [
        ("BTC-USDT", "4h"),
        ("ETH-USDT", "4h"),
        ("BTC-USDT", "1h"),
        ("ETH-USDT", "1h"),
        ("BTC-USDT", "15m"),
        ("ETH-USDT", "15m"),
    ]
    assert tasks[-2].timeframe == "1m"
    assert tasks[-1].timeframe == "1m"


def test_build_nightly_sync_plan_skips_disabled_timeframes() -> None:
    tasks = build_nightly_sync_plan(
        ["BTC-USDT", "ETH-USDT"],
        ["15m", "4h"],
    )

    assert [(task.symbol, task.timeframe) for task in tasks] == [
        ("BTC-USDT", "4h"),
        ("ETH-USDT", "4h"),
        ("BTC-USDT", "15m"),
        ("ETH-USDT", "15m"),
    ]
