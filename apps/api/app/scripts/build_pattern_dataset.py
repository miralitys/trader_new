"""Local batch ingestion for the pattern-research dataset.

This script exists to replace repetitive UI-driven syncs with one resumable
CLI workflow. It ingests candle history for a basket of symbols and timeframes,
prints progress, and emits a JSON summary with final coverage.
"""
from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from app.db.session import SessionLocal
from app.schemas.api import CandleCoverageResponse
from app.services.data_validation_service import DataValidationService, serialize_validation_report
from app.services.market_data_service import MarketDataService, MarketDataSyncResult
from app.services.query_service import QueryService
from app.utils.symbols import supported_symbol_codes
from app.utils.time import ensure_utc, utc_now

DEFAULT_SYMBOL_COUNT = 12
DEFAULT_TIMEFRAMES = ("1m", "5m", "15m", "1h")
DEFAULT_CHUNK_DAYS = {
    "1m": 5,
    "5m": 21,
    "15m": 45,
    "1h": 120,
}


@dataclass(frozen=True)
class ChunkSummary:
    symbol: str
    timeframe: str
    start_at: datetime
    end_at: datetime
    job_id: int
    inserted_rows: int
    fetched_rows: int
    status: str


@dataclass(frozen=True)
class CombinationSummary:
    symbol: str
    timeframe: str
    requested_start_at: datetime
    requested_end_at: datetime
    chunk_days: int
    skipped_as_complete: bool
    chunk_count: int
    total_fetched_rows: int
    total_inserted_rows: int
    coverage: dict[str, Any]
    chunks: list[ChunkSummary] = field(default_factory=list)


@dataclass(frozen=True)
class DatasetBuildReport:
    generated_at: datetime
    exchange_code: str
    symbols: list[str]
    timeframes: list[str]
    lookback_days: int
    start_at: datetime
    end_at: datetime
    combinations: list[CombinationSummary]
    validation: dict[str, Any] | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a local research dataset for recurring-pattern analysis.",
    )
    parser.add_argument("--exchange", default="binance_us")
    parser.add_argument(
        "--symbols",
        default=",".join(supported_symbol_codes()[:DEFAULT_SYMBOL_COUNT]),
        help="Comma-separated symbol list. Defaults to the first supported 12 symbols.",
    )
    parser.add_argument(
        "--timeframes",
        default=",".join(DEFAULT_TIMEFRAMES),
        help="Comma-separated timeframe list. Defaults to 1m,5m,15m,1h.",
    )
    parser.add_argument("--lookback-days", type=int, default=730, help="Historical depth to load.")
    parser.add_argument(
        "--end-at",
        default=None,
        help="Optional ISO timestamp for the dataset end. Defaults to now (UTC).",
    )
    parser.add_argument(
        "--skip-complete",
        action="store_true",
        help="Skip a symbol/timeframe if stored coverage for the target range is already ~complete.",
    )
    parser.add_argument(
        "--coverage-threshold-pct",
        type=float,
        default=99.5,
        help="Coverage threshold used by --skip-complete.",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run data validation after ingestion.",
    )
    parser.add_argument(
        "--validation-lookback-days",
        type=int,
        default=90,
        help="Validation window after ingestion.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional file path for a JSON report.",
    )
    for timeframe, days in DEFAULT_CHUNK_DAYS.items():
        parser.add_argument(
            f"--chunk-days-{timeframe.replace('m', 'min').replace('h', 'hour')}",
            type=int,
            default=days,
            help=f"Chunk size in days for {timeframe} syncs.",
        )
    return parser.parse_args()


def json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def parse_csv(value: str) -> list[str]:
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def chunk_days_by_timeframe(args: argparse.Namespace) -> dict[str, int]:
    return {
        "1m": args.chunk_days_1min,
        "5m": args.chunk_days_5min,
        "15m": args.chunk_days_15min,
        "1h": args.chunk_days_1hour,
    }


def load_coverage(
    exchange_code: str,
    symbol: str,
    timeframe: str,
    start_at: datetime,
    end_at: datetime,
) -> CandleCoverageResponse:
    session = SessionLocal()
    try:
        service = QueryService(session)
        return service.get_candle_coverage(
            exchange_code=exchange_code,
            symbol=symbol,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
        )
    finally:
        session.close()


def coverage_to_dict(coverage: CandleCoverageResponse) -> dict[str, Any]:
    return coverage.model_dump(mode="json")


def iter_ranges(start_at: datetime, end_at: datetime, chunk_days: int) -> list[tuple[datetime, datetime]]:
    ranges: list[tuple[datetime, datetime]] = []
    cursor = ensure_utc(start_at)
    normalized_end = ensure_utc(end_at)
    while cursor < normalized_end:
        chunk_end = min(cursor + timedelta(days=chunk_days), normalized_end)
        ranges.append((cursor, chunk_end))
        cursor = chunk_end
    return ranges


def run_sync_chunk(
    service: MarketDataService,
    exchange_code: str,
    symbol: str,
    timeframe: str,
    start_at: datetime,
    end_at: datetime,
) -> MarketDataSyncResult:
    return service.manual_sync(
        exchange_code=exchange_code,
        symbol=symbol,
        timeframe=timeframe,
        start_at=start_at,
        end_at=end_at,
    )


def main() -> int:
    args = parse_args()
    symbols = parse_csv(args.symbols)
    timeframes = parse_csv(args.timeframes)
    end_at = ensure_utc(datetime.fromisoformat(args.end_at.replace("Z", "+00:00"))) if args.end_at else utc_now()
    start_at = end_at - timedelta(days=args.lookback_days)
    chunk_days_map = chunk_days_by_timeframe(args)

    service = MarketDataService()
    combinations: list[CombinationSummary] = []
    validation_payload: dict[str, Any] | None = None

    try:
        for symbol in symbols:
            for timeframe in timeframes:
                chunk_days = chunk_days_map.get(timeframe, 30)
                existing_coverage = load_coverage(
                    exchange_code=args.exchange,
                    symbol=symbol,
                    timeframe=timeframe,
                    start_at=start_at,
                    end_at=end_at,
                )
                if args.skip_complete and float(existing_coverage.completion_pct) >= args.coverage_threshold_pct:
                    print(
                        f"[skip] {symbol} {timeframe} coverage={existing_coverage.completion_pct}% "
                        f"for {start_at.isoformat()} -> {end_at.isoformat()}",
                    )
                    combinations.append(
                        CombinationSummary(
                            symbol=symbol,
                            timeframe=timeframe,
                            requested_start_at=start_at,
                            requested_end_at=end_at,
                            chunk_days=chunk_days,
                            skipped_as_complete=True,
                            chunk_count=0,
                            total_fetched_rows=0,
                            total_inserted_rows=0,
                            coverage=coverage_to_dict(existing_coverage),
                            chunks=[],
                        )
                    )
                    continue

                print(
                    f"[sync] {symbol} {timeframe} {start_at.isoformat()} -> {end_at.isoformat()} "
                    f"in {chunk_days}d chunks",
                )
                chunk_summaries: list[ChunkSummary] = []
                total_fetched_rows = 0
                total_inserted_rows = 0

                for chunk_start, chunk_end in iter_ranges(start_at, end_at, chunk_days):
                    result = run_sync_chunk(
                        service=service,
                        exchange_code=args.exchange,
                        symbol=symbol,
                        timeframe=timeframe,
                        start_at=chunk_start,
                        end_at=chunk_end,
                    )
                    total_fetched_rows += result.fetched_rows
                    total_inserted_rows += result.inserted_rows
                    chunk_summaries.append(
                        ChunkSummary(
                            symbol=symbol,
                            timeframe=timeframe,
                            start_at=result.start_at,
                            end_at=result.end_at,
                            job_id=result.job_id,
                            inserted_rows=result.inserted_rows,
                            fetched_rows=result.fetched_rows,
                            status=result.status,
                        )
                    )
                    print(
                        f"  -> chunk {chunk_start.date()}..{chunk_end.date()} "
                        f"job={result.job_id} inserted={result.inserted_rows} fetched={result.fetched_rows}",
                    )

                final_coverage = load_coverage(
                    exchange_code=args.exchange,
                    symbol=symbol,
                    timeframe=timeframe,
                    start_at=start_at,
                    end_at=end_at,
                )
                combinations.append(
                    CombinationSummary(
                        symbol=symbol,
                        timeframe=timeframe,
                        requested_start_at=start_at,
                        requested_end_at=end_at,
                        chunk_days=chunk_days,
                        skipped_as_complete=False,
                        chunk_count=len(chunk_summaries),
                        total_fetched_rows=total_fetched_rows,
                        total_inserted_rows=total_inserted_rows,
                        coverage=coverage_to_dict(final_coverage),
                        chunks=chunk_summaries,
                    )
                )
                print(
                    f"[done] {symbol} {timeframe} completion={final_coverage.completion_pct}% "
                    f"candles={final_coverage.candle_count}/{final_coverage.expected_candle_count}",
                )

        if args.validate:
            session = SessionLocal()
            validator = DataValidationService(session)
            try:
                validation_report = validator.validate(
                    exchange_code=args.exchange,
                    symbols=symbols,
                    timeframes=timeframes,
                    lookback_days=args.validation_lookback_days,
                    perform_resync=False,
                )
                validation_payload = serialize_validation_report(validation_report)
                print(f"[validation] verdict={validation_report.verdict}")
            finally:
                validator.close()
                session.close()
    finally:
        service.close()

    report = DatasetBuildReport(
        generated_at=utc_now(),
        exchange_code=args.exchange,
        symbols=symbols,
        timeframes=timeframes,
        lookback_days=args.lookback_days,
        start_at=start_at,
        end_at=end_at,
        combinations=combinations,
        validation=validation_payload,
    )
    payload = asdict(report)
    rendered = json.dumps(payload, indent=2, default=json_default)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
        print(f"[report] wrote {output_path}")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
