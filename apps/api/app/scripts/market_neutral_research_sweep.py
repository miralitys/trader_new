"""CLI sweep for market-neutral research ideas."""
from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from app.schemas.market_neutral_research import MarketNeutralSweepConfig
from app.services.market_neutral_research_service import MarketNeutralResearchService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run market-neutral research sweep.")
    parser.add_argument(
        "--symbols",
        default="BTC-USDT,ETH-USDT,SOL-USDT,BNB-USDT,ADA-USDT,ALPINE-USDT,XRP-USDT,1INCH-USDT,LTC-USDT,BCH-USDT,AVAX-USDT,LINK-USDT,DOGE-USDT,ACT-USDT,MMT-USDT",
    )
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--start-at", required=True)
    parser.add_argument("--end-at", required=True)
    parser.add_argument("--notional-usd", default="10000")
    parser.add_argument("--max-alignment-seconds", default="600")
    parser.add_argument("--min-trades-for-viability", default="3")
    parser.add_argument("--positive-share-threshold", default="0.50")
    parser.add_argument("--top", default="15")
    return parser.parse_args()


def json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def main() -> int:
    args = parse_args()
    symbols = [item.strip() for item in args.symbols.split(",") if item.strip()]
    config = MarketNeutralSweepConfig(
        notional_usd=Decimal(args.notional_usd),
        max_alignment_seconds=int(args.max_alignment_seconds),
        min_trades_for_viability=int(args.min_trades_for_viability),
        positive_share_threshold=Decimal(args.positive_share_threshold),
    )
    service = MarketNeutralResearchService()
    report = service.build_report(
        symbols=symbols,
        timeframe=args.timeframe,
        start_at=datetime.fromisoformat(args.start_at.replace("Z", "+00:00")),
        end_at=datetime.fromisoformat(args.end_at.replace("Z", "+00:00")),
        config=config,
    )
    payload = report.model_dump(mode="json")
    payload["results"] = payload["results"][: int(args.top)]
    print(json.dumps(payload, indent=2, default=json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
