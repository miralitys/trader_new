from __future__ import annotations

import argparse
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from app.scripts.benchmark_btc_cross_source_trend_sweep import (
    fetch_source_candles,
    evaluate_variant_on_source,
)
from app.scripts.benchmark_btc_sleeve_risk_controls import find_flat_priority_variant
from app.scripts.benchmark_regime_aware_adaptive_allocator import add_months
from app.scripts.benchmark_strategy_categories import CategoryVariant, parse_datetime

UTC = timezone.utc
SYMBOL = "ETH-USDT"
POSITION_SIZE_PCT = Decimal("1.00")
TRAIN_POSITION_SIZE_PCT = Decimal("0.10")
DRAWDOWN_GUARD_PCT = 4.0
COOLDOWN_PERIODS = 3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Focused ETH-only cross-source refinement around the best soft-extension variant."
    )
    parser.add_argument("--evaluation-start-at", default="2022-03-15T00:00:00+00:00")
    parser.add_argument("--end-at", default="2026-03-15T00:00:00+00:00")
    parser.add_argument("--warmup-days", type=int, default=60)
    parser.add_argument("--initial-capital", default="10000")
    parser.add_argument("--fee", default="0.001")
    parser.add_argument("--slippage", default="0.0005")
    parser.add_argument("--output")
    return parser.parse_args()


def build_variants() -> tuple[dict[str, Any], ...]:
    base = find_flat_priority_variant()
    base_overrides = deepcopy(base.overrides)

    # Best ETH candidate from the first alt sweep:
    seed = deepcopy(base_overrides)
    seed.update(
        {
            "trend_min_gap_pct": 0.006,
            "trend_min_slope_pct": 0.003,
            "flat_max_width_pct": 0.075,
            "flat_max_center_shift_pct": 0.010,
            "min_average_dollar_volume": 100000,
        }
    )
    seed["trend_config"].update(
        {
            "min_trend_gap_pct": 0.006,
            "min_slow_ema_slope_pct": 0.003,
            "min_volume_multiple": 0.80,
            "min_average_dollar_volume": 75000,
            "breakout_buffer_pct": 0.001,
            "breakout_min_close_location": 0.60,
            "max_extension_above_fast_ema_pct": 0.025,
        }
    )

    configs: list[dict[str, Any]] = []

    def add(label: str, *, overrides: dict[str, Any], min_train_trades: int) -> None:
        configs.append(
            {
                "label": label,
                "variant": CategoryVariant(label=label, overrides=deepcopy(overrides)),
                "min_train_trades": min_train_trades,
            }
        )

    add("eth_seed_m1", overrides=seed, min_train_trades=1)
    add("eth_seed_m2", overrides=seed, min_train_trades=2)

    looser_regime = deepcopy(seed)
    looser_regime.update(
        {
            "trend_min_gap_pct": 0.0055,
            "trend_min_slope_pct": 0.0025,
            "flat_max_width_pct": 0.080,
            "flat_max_center_shift_pct": 0.012,
            "min_average_dollar_volume": 75000,
        }
    )
    add("eth_looser_regime_m1", overrides=looser_regime, min_train_trades=1)
    add("eth_looser_regime_m2", overrides=looser_regime, min_train_trades=2)

    tighter_regime = deepcopy(seed)
    tighter_regime.update(
        {
            "trend_min_gap_pct": 0.0065,
            "trend_min_slope_pct": 0.0035,
            "flat_max_width_pct": 0.070,
            "flat_max_center_shift_pct": 0.009,
            "min_average_dollar_volume": 125000,
        }
    )
    add("eth_tighter_regime_m1", overrides=tighter_regime, min_train_trades=1)

    softer_entry = deepcopy(seed)
    softer_entry["trend_config"].update(
        {
            "min_trend_gap_pct": 0.005,
            "min_slow_ema_slope_pct": 0.0025,
            "min_volume_multiple": 0.75,
            "min_average_dollar_volume": 50000,
            "breakout_buffer_pct": 0.0005,
            "breakout_min_close_location": 0.55,
            "max_extension_above_fast_ema_pct": 0.03,
        }
    )
    add("eth_softer_entry_m1", overrides=softer_entry, min_train_trades=1)
    add("eth_softer_entry_m2", overrides=softer_entry, min_train_trades=2)

    tighter_entry = deepcopy(seed)
    tighter_entry["trend_config"].update(
        {
            "min_trend_gap_pct": 0.007,
            "min_slow_ema_slope_pct": 0.0035,
            "min_volume_multiple": 0.90,
            "min_average_dollar_volume": 100000,
            "breakout_buffer_pct": 0.0015,
            "breakout_min_close_location": 0.65,
            "max_extension_above_fast_ema_pct": 0.02,
        }
    )
    add("eth_tighter_entry_m1", overrides=tighter_entry, min_train_trades=1)

    lower_extension = deepcopy(seed)
    lower_extension["trend_config"]["max_extension_above_fast_ema_pct"] = 0.02
    add("eth_lower_extension_m1", overrides=lower_extension, min_train_trades=1)

    higher_extension = deepcopy(seed)
    higher_extension["trend_config"]["max_extension_above_fast_ema_pct"] = 0.03
    add("eth_higher_extension_m1", overrides=higher_extension, min_train_trades=1)

    lower_close_req = deepcopy(seed)
    lower_close_req["trend_config"]["breakout_min_close_location"] = 0.55
    add("eth_lower_close_req_m1", overrides=lower_close_req, min_train_trades=1)

    higher_close_req = deepcopy(seed)
    higher_close_req["trend_config"]["breakout_min_close_location"] = 0.65
    add("eth_higher_close_req_m1", overrides=higher_close_req, min_train_trades=1)

    lower_liquidity = deepcopy(seed)
    lower_liquidity["min_average_dollar_volume"] = 75000
    lower_liquidity["trend_config"]["min_average_dollar_volume"] = 50000
    add("eth_lower_liquidity_m1", overrides=lower_liquidity, min_train_trades=1)

    higher_liquidity = deepcopy(seed)
    higher_liquidity["min_average_dollar_volume"] = 150000
    higher_liquidity["trend_config"]["min_average_dollar_volume"] = 125000
    add("eth_higher_liquidity_m1", overrides=higher_liquidity, min_train_trades=1)

    return tuple(configs)


def summarize_pair(
    *,
    label: str,
    min_train_trades: int,
    by_source: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    us = by_source["binance_us"]
    archive = by_source["binance_archive_spot"]
    min_return_pct = min(float(us["total_return_pct"]), float(archive["total_return_pct"]))
    avg_return_pct = (float(us["total_return_pct"]) + float(archive["total_return_pct"])) / 2.0
    gap_return_pct = abs(float(us["total_return_pct"]) - float(archive["total_return_pct"]))
    avg_drawdown_pct = (float(us["max_drawdown_pct"]) + float(archive["max_drawdown_pct"])) / 2.0
    min_profit_factor = min(float(us["profit_factor"]), float(archive["profit_factor"]))
    both_sources_positive = float(us["total_return_pct"]) > 0.0 and float(archive["total_return_pct"]) > 0.0
    robustness_score = round(
        (2.0 if both_sources_positive else 0.0)
        + min_return_pct
        - (gap_return_pct * 0.30)
        - (avg_drawdown_pct * 0.10),
        6,
    )
    return {
        "label": label,
        "min_train_trades": min_train_trades,
        "summary": {
            "binance_us_return_pct": round(float(us["total_return_pct"]), 4),
            "binance_archive_return_pct": round(float(archive["total_return_pct"]), 4),
            "binance_us_max_drawdown_pct": round(float(us["max_drawdown_pct"]), 4),
            "binance_archive_max_drawdown_pct": round(float(archive["max_drawdown_pct"]), 4),
            "binance_us_profit_factor": round(float(us["profit_factor"]), 4),
            "binance_archive_profit_factor": round(float(archive["profit_factor"]), 4),
            "binance_us_total_trades": int(us["total_trades"]),
            "binance_archive_total_trades": int(archive["total_trades"]),
            "min_return_pct": round(min_return_pct, 4),
            "avg_return_pct": round(avg_return_pct, 4),
            "gap_return_pct": round(gap_return_pct, 4),
            "avg_drawdown_pct": round(avg_drawdown_pct, 4),
            "min_profit_factor": round(min_profit_factor, 4),
            "both_sources_positive": both_sources_positive,
            "robustness_score": robustness_score,
        },
        "sources": by_source,
    }


def run_scenario(
    *,
    label: str,
    config: dict[str, Any],
    candles_by_source: dict[str, list[Any]],
    evaluation_start_at: datetime,
    end_at: datetime,
    initial_capital: Decimal,
    fee: Decimal,
    slippage: Decimal,
) -> dict[str, Any]:
    by_source = {}
    for source, candles in candles_by_source.items():
        by_source[source] = evaluate_variant_on_source(
            source=source,
            symbol=SYMBOL,
            candles=candles,
            variant=config["variant"],
            evaluation_start_at=evaluation_start_at,
            end_at=end_at,
            initial_capital=initial_capital,
            fee=fee,
            slippage=slippage,
            position_size_pct=POSITION_SIZE_PCT,
            train_position_size_pct=TRAIN_POSITION_SIZE_PCT,
            drawdown_guard_pct=DRAWDOWN_GUARD_PCT,
            cooldown_periods=COOLDOWN_PERIODS,
            min_train_trades=int(config["min_train_trades"]),
        )
    report = summarize_pair(label=label, min_train_trades=int(config["min_train_trades"]), by_source=by_source)
    report["fee"] = float(fee)
    report["slippage"] = float(slippage)
    return report


def main() -> None:
    args = parse_args()
    evaluation_start_at = parse_datetime(args.evaluation_start_at)
    end_at = parse_datetime(args.end_at)
    initial_capital = Decimal(str(args.initial_capital))
    fee = Decimal(str(args.fee))
    slippage = Decimal(str(args.slippage))
    fetch_start_at = add_months(evaluation_start_at, -2) - timedelta(days=args.warmup_days)

    candles_by_source = fetch_source_candles(
        symbol=SYMBOL,
        fetch_start_at=fetch_start_at,
        end_at=end_at,
    )
    configs = build_variants()

    reports: list[dict[str, Any]] = []
    for config in configs:
        print(f"Evaluating ETH refine {config['label']}...")
        reports.append(
            run_scenario(
                label=str(config["label"]),
                config=config,
                candles_by_source=candles_by_source,
                evaluation_start_at=evaluation_start_at,
                end_at=end_at,
                initial_capital=initial_capital,
                fee=fee,
                slippage=slippage,
            )
        )

    reports.sort(
        key=lambda item: (
            item["summary"]["robustness_score"],
            item["summary"]["min_return_pct"],
            -item["summary"]["gap_return_pct"],
        ),
        reverse=True,
    )
    winner = reports[0]
    winner_config = next(config for config in configs if config["label"] == winner["label"])

    stress_scenarios = (
        ("base", fee, slippage),
        ("moderate_costs", Decimal("0.0015"), Decimal("0.00075")),
        ("heavy_costs", Decimal("0.002"), Decimal("0.001")),
    )
    stress_reports = [
        run_scenario(
            label=f"{winner['label']}::{scenario_label}",
            config=winner_config,
            candles_by_source=candles_by_source,
            evaluation_start_at=evaluation_start_at,
            end_at=end_at,
            initial_capital=initial_capital,
            fee=scenario_fee,
            slippage=scenario_slippage,
        )
        for scenario_label, scenario_fee, scenario_slippage in stress_scenarios
    ]

    payload = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "symbol": SYMBOL,
        "window": {
            "fetch_start_at": fetch_start_at.isoformat(),
            "evaluation_start_at": evaluation_start_at.isoformat(),
            "end_at": end_at.isoformat(),
        },
        "config": {
            "position_size_pct": float(POSITION_SIZE_PCT),
            "train_position_size_pct": float(TRAIN_POSITION_SIZE_PCT),
            "drawdown_guard_pct": DRAWDOWN_GUARD_PCT,
            "cooldown_periods": COOLDOWN_PERIODS,
        },
        "reports": reports,
        "winner": winner,
        "stress_reports": stress_reports,
    }

    rendered = json.dumps(payload, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
        print(f"Wrote ETH cross-source refinement report to {output_path}")
    print(rendered)


if __name__ == "__main__":
    main()
