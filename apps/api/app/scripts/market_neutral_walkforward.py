"""CLI for PerpPremiumMeanReversion walk-forward research."""
from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from app.schemas.market_neutral_research import MarketNeutralSweepConfig
from app.services.market_neutral_research_service import MarketNeutralResearchService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run walk-forward research for PerpPremiumMeanReversion.")
    parser.add_argument("--symbol", default="ACT-USDT")
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--start-at", required=True)
    parser.add_argument("--end-at", required=True)
    parser.add_argument("--train-days", default="14")
    parser.add_argument("--test-days", default="7")
    parser.add_argument("--notional-usd", default="10000")
    parser.add_argument("--max-alignment-seconds", default="600")
    parser.add_argument("--min-trades-for-viability", default="3")
    parser.add_argument("--positive-share-threshold", default="0.50")
    parser.add_argument("--cost-scenarios", default="maker_taker,maker_maker")
    return parser.parse_args()


def json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def main() -> int:
    args = parse_args()
    config = MarketNeutralSweepConfig(
        notional_usd=Decimal(args.notional_usd),
        max_alignment_seconds=int(args.max_alignment_seconds),
        min_trades_for_viability=int(args.min_trades_for_viability),
        positive_share_threshold=Decimal(args.positive_share_threshold),
    )
    cost_scenario_names = [item.strip() for item in args.cost_scenarios.split(",") if item.strip()]
    service = MarketNeutralResearchService()
    report = service.run_perp_premium_walk_forward(
        symbol=args.symbol,
        timeframe=args.timeframe,
        start_at=datetime.fromisoformat(args.start_at.replace("Z", "+00:00")),
        end_at=datetime.fromisoformat(args.end_at.replace("Z", "+00:00")),
        train_days=int(args.train_days),
        test_days=int(args.test_days),
        config=config,
        cost_scenario_names=cost_scenario_names,
    )
    print(json.dumps([item.model_dump(mode="json") for item in report], indent=2, default=json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
