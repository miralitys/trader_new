"""CLI for FundingBasisCarry research ingestion."""
from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from app.services.funding_basis_ingestion_service import FundingBasisIngestionService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest spot/perp/funding data for FundingBasisCarry research.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    history_parser = subparsers.add_parser("history", help="Load historical data.")
    history_parser.add_argument("--symbols", default="BTC-USDT,ETH-USDT,SOL-USDT")
    history_parser.add_argument("--timeframe", default="5m")
    history_parser.add_argument("--perp-venue", default="binance_futures", choices=["binance_futures", "okx_swap"])
    history_parser.add_argument("--start-at", required=True)
    history_parser.add_argument("--end-at", required=True)
    history_parser.add_argument("--prefer-archive", action="store_true")

    incremental_parser = subparsers.add_parser("incremental", help="Refresh the latest data incrementally.")
    incremental_parser.add_argument("--symbols", default="BTC-USDT,ETH-USDT,SOL-USDT")
    incremental_parser.add_argument("--timeframe", default="5m")
    incremental_parser.add_argument("--perp-venue", default="binance_futures", choices=["binance_futures", "okx_swap"])
    incremental_parser.add_argument("--end-at")
    incremental_parser.add_argument("--prefer-archive", action="store_true")
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
    service = FundingBasisIngestionService(
        prefer_archive=args.prefer_archive,
        perp_exchange=args.perp_venue,
    )
    try:
        if args.command == "history":
            result = service.backfill(
                symbols=symbols,
                timeframe=args.timeframe,
                start_at=datetime.fromisoformat(args.start_at.replace("Z", "+00:00")),
                end_at=datetime.fromisoformat(args.end_at.replace("Z", "+00:00")),
            )
        else:
            result = service.incremental(
                symbols=symbols,
                timeframe=args.timeframe,
                end_at=datetime.fromisoformat(args.end_at.replace("Z", "+00:00")) if args.end_at else None,
            )
    finally:
        service.close()

    print(json.dumps(result.model_dump(mode="json"), indent=2, default=json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
