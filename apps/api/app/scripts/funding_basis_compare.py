"""CLI report for comparing FundingBasisCarry research across perp venues."""
from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from app.schemas.funding_basis import FundingBasisResearchConfig
from app.services.funding_basis_comparison_service import FundingBasisComparisonService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare FundingBasisCarry research across perp venues.")
    parser.add_argument("--symbols", default="BTC-USDT,ETH-USDT,SOL-USDT,BNB-USDT,ADA-USDT,ALPINE-USDT,XRP-USDT,1INCH-USDT")
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--perp-venues", default="binance_futures,okx_swap")
    parser.add_argument("--start-at", required=True)
    parser.add_argument("--end-at", required=True)
    parser.add_argument("--min-funding-rate", default="0.0001")
    parser.add_argument("--min-basis-pct", default="0.0005")
    parser.add_argument("--notional-usd", default="10000")
    parser.add_argument("--spot-fee-pct", default="0.001")
    parser.add_argument("--perp-fee-pct", default="0.0005")
    parser.add_argument("--slippage-pct", default="0.0003")
    parser.add_argument("--max-snapshot-alignment-seconds", type=int, default=600)
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
    perp_venues = [item.strip() for item in args.perp_venues.split(",") if item.strip()]
    config = FundingBasisResearchConfig(
        min_funding_rate=Decimal(args.min_funding_rate),
        min_basis_pct=Decimal(args.min_basis_pct),
        notional_usd=Decimal(args.notional_usd),
        spot_fee_pct=Decimal(args.spot_fee_pct),
        perp_fee_pct=Decimal(args.perp_fee_pct),
        slippage_pct=Decimal(args.slippage_pct),
        max_snapshot_alignment_seconds=args.max_snapshot_alignment_seconds,
    )
    service = FundingBasisComparisonService()
    report = service.build_comparison_report(
        symbols=symbols,
        timeframe=args.timeframe,
        start_at=datetime.fromisoformat(args.start_at.replace("Z", "+00:00")),
        end_at=datetime.fromisoformat(args.end_at.replace("Z", "+00:00")),
        perp_exchanges=perp_venues,
        config=config,
    )
    print(json.dumps(report.model_dump(mode="json"), indent=2, default=json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
