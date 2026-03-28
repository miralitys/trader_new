from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from app.schemas.backtest import BacktestCandle
from app.scripts.benchmark_btc_cross_source_trend_sweep import (
    fetch_source_candles,
    evaluate_variant_on_source,
)
from app.scripts.benchmark_regime_aware import build_regime_variants
from app.scripts.benchmark_regime_aware_adaptive_allocator import add_months
from app.scripts.benchmark_strategy_categories import parse_datetime, parse_symbols

UTC = timezone.utc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark a fixed cross-source regime variant across multiple liquid symbols."
    )
    parser.add_argument("--symbols", default="BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,ADA-USDT,DOGE-USDT,LTC-USDT")
    parser.add_argument("--variant-label", default="flat_priority_cross_source")
    parser.add_argument("--evaluation-start-at", default="2022-03-15T00:00:00+00:00")
    parser.add_argument("--end-at", default="2026-03-15T00:00:00+00:00")
    parser.add_argument("--warmup-days", type=int, default=60)
    parser.add_argument("--initial-capital", default="10000")
    parser.add_argument("--fee", default="0.001")
    parser.add_argument("--slippage", default="0.0005")
    parser.add_argument("--output")
    return parser.parse_args()


def find_variant(label: str):
    for variant in build_regime_variants():
        if variant.label == label:
            return variant
    raise ValueError(f"Unknown variant label: {label}")


def pair_summary(symbol: str, variant_label: str, by_source: dict[str, dict[str, Any]], effective_start: datetime) -> dict[str, Any]:
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
        "symbol": symbol,
        "variant_label": variant_label,
        "effective_evaluation_start_at": effective_start.isoformat(),
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


def common_first_candle_at(candles_by_source: dict[str, list[BacktestCandle]]) -> datetime | None:
    first_times = []
    for candles in candles_by_source.values():
        if not candles:
            return None
        first_times.append(candles[0].open_time)
    return max(first_times) if first_times else None


def main() -> None:
    args = parse_args()
    symbols = parse_symbols(args.symbols)
    variant = find_variant(args.variant_label)
    evaluation_start_at = parse_datetime(args.evaluation_start_at)
    end_at = parse_datetime(args.end_at)
    initial_capital = Decimal(str(args.initial_capital))
    fee = Decimal(str(args.fee))
    slippage = Decimal(str(args.slippage))
    fetch_start_at = add_months(evaluation_start_at, -2) - timedelta(days=args.warmup_days)

    reports: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for symbol in symbols:
        print(f"Evaluating transfer for {symbol}...")
        try:
            candles_by_source = fetch_source_candles(
                symbol=symbol,
                fetch_start_at=fetch_start_at,
                end_at=end_at,
            )
        except Exception as exc:  # pragma: no cover - research script guardrail
            skipped.append({"symbol": symbol, "reason": f"fetch_failed:{type(exc).__name__}:{exc}"})
            continue

        first_common = common_first_candle_at(candles_by_source)
        if first_common is None:
            skipped.append({"symbol": symbol, "reason": "no_common_candles"})
            continue

        effective_evaluation_start_at = max(evaluation_start_at, add_months(first_common, 2))
        if effective_evaluation_start_at >= end_at:
            skipped.append(
                {
                    "symbol": symbol,
                    "reason": "effective_window_empty",
                    "effective_evaluation_start_at": effective_evaluation_start_at.isoformat(),
                }
            )
            continue

        by_source = {}
        for source, candles in candles_by_source.items():
            by_source[source] = evaluate_variant_on_source(
                source=source,
                symbol=symbol,
                candles=candles,
                variant=variant,
                evaluation_start_at=effective_evaluation_start_at,
                end_at=end_at,
                initial_capital=initial_capital,
                fee=fee,
                slippage=slippage,
            )
        reports.append(
            pair_summary(
                symbol=symbol,
                variant_label=variant.label,
                by_source=by_source,
                effective_start=effective_evaluation_start_at,
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

    payload = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "variant": {
            "label": variant.label,
            "overrides": variant.overrides,
        },
        "window": {
            "fetch_start_at": fetch_start_at.isoformat(),
            "requested_evaluation_start_at": evaluation_start_at.isoformat(),
            "end_at": end_at.isoformat(),
        },
        "reports": reports,
        "skipped": skipped,
        "winner": reports[0] if reports else None,
    }

    rendered = json.dumps(payload, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
        print(f"Wrote cross-source transfer report to {output_path}")
    print(rendered)


if __name__ == "__main__":
    main()
