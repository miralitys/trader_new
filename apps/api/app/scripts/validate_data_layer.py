from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from app.db.session import SessionLocal
from app.services.data_validation_service import DataValidationService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Binance.US candle storage truthfulness.")
    parser.add_argument("--exchange", default="binance_us")
    parser.add_argument("--symbols", default="BTC-USDT,ETH-USDT,SOL-USDT,BNB-USDT,ADA-USDT,ALPINE-USDT,XRP-USDT,1INCH-USDT,LTC-USDT,BCH-USDT,AVAX-USDT,LINK-USDT,DOGE-USDT")
    parser.add_argument("--timeframes", default="5m,15m,1h")
    parser.add_argument("--lookback-days", type=int, default=90)
    parser.add_argument("--perform-resync", action="store_true")
    parser.add_argument("--resync-days", type=int, default=14)
    parser.add_argument("--sample-limit", type=int, default=5)
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
    timeframes = [item.strip() for item in args.timeframes.split(",") if item.strip()]

    session = SessionLocal()
    service = DataValidationService(session)
    try:
        report = service.validate(
            exchange_code=args.exchange,
            symbols=symbols,
            timeframes=timeframes,
            lookback_days=args.lookback_days,
            perform_resync=args.perform_resync,
            resync_days=args.resync_days,
            sample_limit=args.sample_limit,
        )
    finally:
        service.close()
        session.close()

    print(json.dumps(asdict(report), indent=2, default=json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
