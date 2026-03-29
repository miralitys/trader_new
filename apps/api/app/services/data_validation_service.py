from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Callable, Optional

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.integrations.binance_us.schemas import BinanceUSTimeframe
from app.models import Candle, SyncJob
from app.repositories.candle_repository import CandleCoverageSummary, CandleRepository
from app.services.market_data_service import MarketDataService
from app.services.query_service import QueryService
from app.utils.exchanges import normalize_exchange_code
from app.utils.symbols import compact_supported_symbols
from app.utils.time import ensure_utc, utc_now

logger = get_logger(__name__)


@dataclass(frozen=True)
class StoredRangeSummary:
    first_candle: Optional[datetime]
    last_candle: Optional[datetime]
    candle_count: int
    expected_candle_count: int
    completion_pct: Decimal


@dataclass(frozen=True)
class DuplicateSample:
    open_time: datetime
    row_count: int


@dataclass(frozen=True)
class DuplicateSummary:
    duplicate_count: int
    duplicate_bucket_count: int
    sample_duplicates: list[DuplicateSample] = field(default_factory=list)


@dataclass(frozen=True)
class TimestampAlignmentSummary:
    invalid_timestamp_count: int
    sample_invalid_timestamps: list[datetime] = field(default_factory=list)


@dataclass(frozen=True)
class GapSummary:
    missing_candle_count: int
    sample_missing_timestamps: list[datetime] = field(default_factory=list)


@dataclass(frozen=True)
class ApiTruthfulnessSummary:
    coverage_endpoint_matches_db: bool
    status_endpoint_matches_db: bool
    latest_sync_job_id: Optional[int]
    latest_sync_job_status: Optional[str]
    notes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ResyncSummary:
    requested_start_at: datetime
    requested_end_at: datetime
    before_count: int
    after_count: int
    before_loaded_start_at: Optional[datetime]
    before_loaded_end_at: Optional[datetime]
    after_loaded_start_at: Optional[datetime]
    after_loaded_end_at: Optional[datetime]
    rows_inserted: int
    duplicate_count_after: int
    stable: bool


@dataclass(frozen=True)
class ValidationIssue:
    severity: str
    code: str
    message: str


@dataclass(frozen=True)
class DataValidationResult:
    exchange_code: str
    symbol: str
    timeframe: str
    stored_range: StoredRangeSummary
    validation_window: CandleCoverageSummary
    duplicates: DuplicateSummary
    timestamp_alignment: TimestampAlignmentSummary
    gaps: GapSummary
    api_truthfulness: ApiTruthfulnessSummary
    resync: Optional[ResyncSummary]
    issues: list[ValidationIssue]
    verdict: str


@dataclass(frozen=True)
class DataValidationReport:
    generated_at: datetime
    exchange_code: str
    lookback_days: int
    resync_days: int
    perform_resync: bool
    results: list[DataValidationResult]
    verdict: str


def serialize_validation_report(report: DataValidationReport) -> dict[str, Any]:
    return asdict(report)


def build_validation_report_payload(report: DataValidationReport) -> dict[str, Any]:
    payload = serialize_validation_report(report)
    results = [_normalize_validation_result_payload(row) for row in payload["results"]]

    completion_by_timeframe: dict[str, list[float]] = {}
    symbol_rollup: dict[str, dict[str, float | int | str]] = {}
    timeframe_rollup: dict[str, dict[str, float | int | str | list[float]]] = {}
    best_completion_by_symbol: dict[str, float] = {}
    one_minute_rows: list[dict[str, object]] = []

    pass_count = warning_count = fail_count = 0
    duplicate_rows_total = invalid_timestamps_total = internal_gap_total = 0

    for row in results:
        verdict = row["verdict"] if isinstance(row, dict) else getattr(row, "verdict")
        if verdict == "PASS":
            pass_count += 1
        elif verdict == "PASS WITH WARNINGS":
            warning_count += 1
        else:
            fail_count += 1

        symbol = row["symbol"] if isinstance(row, dict) else getattr(row, "symbol")
        timeframe = row["timeframe"] if isinstance(row, dict) else getattr(row, "timeframe")
        validation_window = row["validation_window"] if isinstance(row, dict) else getattr(row, "validation_window")
        gaps = row["gaps"] if isinstance(row, dict) else getattr(row, "gaps")
        duplicates = row["duplicates"] if isinstance(row, dict) else getattr(row, "duplicates")
        timestamp_alignment = row["timestamp_alignment"] if isinstance(row, dict) else getattr(row, "timestamp_alignment")

        completion_pct = float(validation_window["completion_pct"] if isinstance(validation_window, dict) else getattr(validation_window, "completion_pct"))
        gap_count = int(gaps["missing_candle_count"] if isinstance(gaps, dict) else getattr(gaps, "missing_candle_count"))
        duplicate_count = int(duplicates["duplicate_count"] if isinstance(duplicates, dict) else getattr(duplicates, "duplicate_count"))
        invalid_timestamp_count = int(
            timestamp_alignment["invalid_timestamp_count"]
            if isinstance(timestamp_alignment, dict)
            else getattr(timestamp_alignment, "invalid_timestamp_count")
        )

        duplicate_rows_total += duplicate_count
        invalid_timestamps_total += invalid_timestamp_count
        internal_gap_total += gap_count

        completion_by_timeframe.setdefault(timeframe, []).append(completion_pct)
        best_completion_by_symbol[symbol] = max(best_completion_by_symbol.get(symbol, 0.0), completion_pct)

        symbol_entry = symbol_rollup.setdefault(
            symbol,
            {
                "symbol": symbol,
                "worst_completion_pct": 100.0,
                "total_gap_count": 0,
                "total_duplicate_count": 0,
                "invalid_timestamp_count": 0,
                "failing_series_count": 0,
            },
        )
        symbol_entry["worst_completion_pct"] = min(float(symbol_entry["worst_completion_pct"]), completion_pct)
        symbol_entry["total_gap_count"] = int(symbol_entry["total_gap_count"]) + gap_count
        symbol_entry["total_duplicate_count"] = int(symbol_entry["total_duplicate_count"]) + duplicate_count
        symbol_entry["invalid_timestamp_count"] = int(symbol_entry["invalid_timestamp_count"]) + invalid_timestamp_count
        if verdict != "PASS":
            symbol_entry["failing_series_count"] = int(symbol_entry["failing_series_count"]) + 1

        timeframe_entry = timeframe_rollup.setdefault(
            timeframe,
            {
                "timeframe": timeframe,
                "completion_values": [],
                "total_gap_count": 0,
                "failing_series_count": 0,
            },
        )
        timeframe_entry["completion_values"].append(completion_pct)
        timeframe_entry["total_gap_count"] = int(timeframe_entry["total_gap_count"]) + gap_count
        if verdict != "PASS":
            timeframe_entry["failing_series_count"] = int(timeframe_entry["failing_series_count"]) + 1

        if timeframe == "1m":
            one_minute_rows.append(
                {
                    "symbol": symbol,
                    "completion_pct": completion_pct,
                    "gap_count": gap_count,
                }
            )

    worst_symbols = sorted(
        symbol_rollup.values(),
        key=lambda item: (
            float(item["worst_completion_pct"]),
            -int(item["total_gap_count"]),
            -int(item["failing_series_count"]),
        ),
    )[:10]

    worst_timeframes = []
    for _, item in timeframe_rollup.items():
        completion_values = item.pop("completion_values")
        avg_completion = sum(completion_values) / max(1, len(completion_values))
        worst_timeframes.append(
            {
                "timeframe": item["timeframe"],
                "avg_completion_pct": avg_completion,
                "total_gap_count": item["total_gap_count"],
                "failing_series_count": item["failing_series_count"],
            }
        )
    worst_timeframes = sorted(
        worst_timeframes,
        key=lambda item: (float(item["avg_completion_pct"]), -int(item["total_gap_count"])),
    )

    one_minute_laggards = sorted(
        (
            {
                "symbol": row["symbol"],
                "completion_pct": row["completion_pct"],
                "gap_vs_best_timeframe_pct": max(best_completion_by_symbol.get(row["symbol"], 0.0) - float(row["completion_pct"]), 0.0),
                "gap_count": row["gap_count"],
            }
            for row in one_minute_rows
        ),
        key=lambda item: (float(item["completion_pct"]), -float(item["gap_vs_best_timeframe_pct"]), -int(item["gap_count"])),
    )[:10]

    return {
        "summary": {
            "generated_at": payload["generated_at"],
            "exchange_code": payload["exchange_code"],
            "lookback_days": payload["lookback_days"],
            "verdict": payload["verdict"],
            "overview": {
                "total_series": len(results),
                "pass_count": pass_count,
                "warning_count": warning_count,
                "fail_count": fail_count,
                "duplicate_rows_total": duplicate_rows_total,
                "invalid_timestamps_total": invalid_timestamps_total,
                "internal_gap_total": internal_gap_total,
            },
            "worst_symbols": worst_symbols,
            "worst_timeframes": worst_timeframes,
            "one_minute_laggards": one_minute_laggards,
            "completion_by_timeframe": {
                timeframe: round(sum(values) / max(1, len(values)), 4)
                for timeframe, values in completion_by_timeframe.items()
            },
        },
        "results": results,
    }


def _normalize_validation_result_payload(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    validation_window = dict(normalized.get("validation_window") or {})
    if validation_window:
        normalized["validation_window"] = {
            "exchange_code": validation_window.get("exchange_code"),
            "symbol": validation_window.get("symbol") or validation_window.get("symbol_code"),
            "timeframe": validation_window.get("timeframe"),
            "requested_start_at": validation_window.get("requested_start_at"),
            "requested_end_at": validation_window.get("requested_end_at"),
            "loaded_start_at": validation_window.get("loaded_start_at") or validation_window.get("actual_start_at"),
            "loaded_end_at": validation_window.get("loaded_end_at") or validation_window.get("actual_end_at"),
            "candle_count": validation_window.get("candle_count", 0),
            "expected_candle_count": validation_window.get("expected_candle_count", 0),
            "missing_candle_count": validation_window.get("missing_candle_count", 0),
            "completion_pct": validation_window.get("completion_pct", Decimal("0")),
        }
    return normalized


def derive_recent_window(
    first_candle: Optional[datetime],
    last_candle: Optional[datetime],
    timeframe: str,
    lookback_days: int,
) -> tuple[Optional[datetime], Optional[datetime]]:
    if first_candle is None or last_candle is None:
        return None, None

    timeframe_value = BinanceUSTimeframe.from_code(timeframe)
    interval = timeframe_value.interval
    candidate_start = ensure_utc(last_candle) - timedelta(days=lookback_days) + interval
    start_at = max(ensure_utc(first_candle), candidate_start)
    return start_at, ensure_utc(last_candle)


class DataValidationService:
    def __init__(self, session: Session, market_data_service: Optional[MarketDataService] = None) -> None:
        self.session = session
        self.candle_repository = CandleRepository(session)
        self.query_service = QueryService(session)
        self.market_data_service = market_data_service or MarketDataService()

    def close(self) -> None:
        self.market_data_service.close()

    def validate(
        self,
        exchange_code: str,
        symbols: list[str],
        timeframes: list[str],
        lookback_days: int = 90,
        perform_resync: bool = False,
        resync_days: int = 14,
        sample_limit: int = 5,
        progress_callback: Optional[Callable[[str, str, int, int], None]] = None,
    ) -> DataValidationReport:
        normalized_exchange = normalize_exchange_code(exchange_code)
        normalized_symbols = compact_supported_symbols(symbols)
        normalized_timeframes = [BinanceUSTimeframe.from_code(code).value for code in timeframes]
        results: list[DataValidationResult] = []
        total = len(normalized_symbols) * len(normalized_timeframes)
        processed = 0

        for symbol in normalized_symbols:
            for timeframe in normalized_timeframes:
                results.append(
                    self._validate_combination(
                        exchange_code=normalized_exchange,
                        symbol=symbol,
                        timeframe=timeframe,
                        lookback_days=lookback_days,
                        perform_resync=perform_resync,
                        resync_days=resync_days,
                        sample_limit=sample_limit,
                    )
                )
                processed += 1
                if progress_callback is not None:
                    progress_callback(symbol, timeframe, processed, total)

        verdict = "PASS"
        if any(result.verdict == "FAIL" for result in results):
            verdict = "FAIL"
        elif any(result.verdict == "PASS WITH WARNINGS" for result in results):
            verdict = "PASS WITH WARNINGS"

        return DataValidationReport(
            generated_at=utc_now(),
            exchange_code=normalized_exchange,
            lookback_days=lookback_days,
            resync_days=resync_days,
            perform_resync=perform_resync,
            results=results,
            verdict=verdict,
        )

    def _validate_combination(
        self,
        exchange_code: str,
        symbol: str,
        timeframe: str,
        lookback_days: int,
        perform_resync: bool,
        resync_days: int,
        sample_limit: int,
    ) -> DataValidationResult:
        exchange = self.candle_repository.get_exchange(exchange_code)
        if exchange is None:
            issues = [
                ValidationIssue(
                    severity="critical",
                    code="missing_exchange",
                    message=f"Exchange {exchange_code} is missing from reference tables.",
                )
            ]
            empty_coverage = CandleCoverageSummary(
                exchange_code=exchange_code,
                symbol_code=symbol,
                timeframe=timeframe,
                requested_start_at=None,
                requested_end_at=None,
                actual_start_at=None,
                actual_end_at=None,
                candle_count=0,
                expected_candle_count=0,
                missing_candle_count=0,
                completion_pct=Decimal("0"),
            )
            empty_range = StoredRangeSummary(None, None, 0, 0, Decimal("0"))
            empty_duplicates = DuplicateSummary(0, 0, [])
            empty_alignment = TimestampAlignmentSummary(0, [])
            empty_gaps = GapSummary(0, [])
            empty_api = ApiTruthfulnessSummary(False, False, None, None, [])
            return DataValidationResult(
                exchange_code=exchange_code,
                symbol=symbol,
                timeframe=timeframe,
                stored_range=empty_range,
                validation_window=empty_coverage,
                duplicates=empty_duplicates,
                timestamp_alignment=empty_alignment,
                gaps=empty_gaps,
                api_truthfulness=empty_api,
                resync=None,
                issues=issues,
                verdict="FAIL",
            )

        symbol_row = self.candle_repository.get_symbol(exchange.id, symbol)
        if symbol_row is None:
            issues = [
                ValidationIssue(
                    severity="critical",
                    code="missing_symbol",
                    message=f"Symbol {symbol} is missing for exchange {exchange_code}.",
                )
            ]
            empty_coverage = CandleCoverageSummary(
                exchange_code=exchange_code,
                symbol_code=symbol,
                timeframe=timeframe,
                requested_start_at=None,
                requested_end_at=None,
                actual_start_at=None,
                actual_end_at=None,
                candle_count=0,
                expected_candle_count=0,
                missing_candle_count=0,
                completion_pct=Decimal("0"),
            )
            empty_range = StoredRangeSummary(None, None, 0, 0, Decimal("0"))
            empty_duplicates = DuplicateSummary(0, 0, [])
            empty_alignment = TimestampAlignmentSummary(0, [])
            empty_gaps = GapSummary(0, [])
            empty_api = ApiTruthfulnessSummary(False, False, None, None, [])
            return DataValidationResult(
                exchange_code=exchange_code,
                symbol=symbol,
                timeframe=timeframe,
                stored_range=empty_range,
                validation_window=empty_coverage,
                duplicates=empty_duplicates,
                timestamp_alignment=empty_alignment,
                gaps=empty_gaps,
                api_truthfulness=empty_api,
                resync=None,
                issues=issues,
                verdict="FAIL",
            )

        stored_range = self._get_stored_range(exchange.id, symbol_row.id, timeframe)
        validation_start_at, validation_end_at = self._recent_window(
            first_candle=stored_range.first_candle,
            last_candle=stored_range.last_candle,
            timeframe=timeframe,
            lookback_days=lookback_days,
        )
        validation_window = self.candle_repository.get_candle_coverage(
            exchange_code=exchange_code,
            symbol_code=symbol,
            timeframe=timeframe,
            start_at=validation_start_at,
            end_at=validation_end_at,
        )
        duplicates = self._get_duplicate_summary(exchange.id, symbol_row.id, timeframe, sample_limit)
        alignment = self._get_timestamp_alignment_summary(exchange.id, symbol_row.id, timeframe, sample_limit)
        gaps = self._get_gap_summary(
            exchange.id,
            symbol_row.id,
            timeframe,
            validation_start_at,
            validation_end_at,
            sample_limit,
        )
        resync_summary: Optional[ResyncSummary] = None
        if perform_resync and validation_end_at is not None:
            resync_start_at, resync_end_at = self._recent_window(
                first_candle=stored_range.first_candle,
                last_candle=stored_range.last_candle,
                timeframe=timeframe,
                lookback_days=resync_days,
            )
            if resync_start_at is not None and resync_end_at is not None:
                resync_summary = self._run_resync_summary(
                    exchange_code=exchange_code,
                    exchange_id=exchange.id,
                    symbol=symbol,
                    symbol_id=symbol_row.id,
                    timeframe=timeframe,
                    start_at=resync_start_at,
                    end_at=resync_end_at,
                )

        api_truthfulness = self._get_api_truthfulness_summary(
            exchange_code=exchange_code,
            symbol=symbol,
            timeframe=timeframe,
            coverage=validation_window,
        )

        issues = self._build_issues(
            stored_range=stored_range,
            validation_window=validation_window,
            duplicates=duplicates,
            alignment=alignment,
            gaps=gaps,
            api_truthfulness=api_truthfulness,
            resync=resync_summary,
        )
        verdict = "PASS"
        if any(issue.severity == "critical" for issue in issues):
            verdict = "FAIL"
        elif issues:
            verdict = "PASS WITH WARNINGS"

        return DataValidationResult(
            exchange_code=exchange_code,
            symbol=symbol,
            timeframe=timeframe,
            stored_range=stored_range,
            validation_window=validation_window,
            duplicates=duplicates,
            timestamp_alignment=alignment,
            gaps=gaps,
            api_truthfulness=api_truthfulness,
            resync=resync_summary,
            issues=issues,
            verdict=verdict,
        )

    def _get_stored_range(self, exchange_id: int, symbol_id: int, timeframe: str) -> StoredRangeSummary:
        candle_count, first_candle, last_candle = self.session.execute(
            select(
                func.count(Candle.id),
                func.min(Candle.open_time),
                func.max(Candle.open_time),
            ).where(
                Candle.exchange_id == exchange_id,
                Candle.symbol_id == symbol_id,
                Candle.timeframe == timeframe,
            )
        ).one()

        candle_count = int(candle_count or 0)
        if candle_count == 0 or first_candle is None or last_candle is None:
            return StoredRangeSummary(
                first_candle=None,
                last_candle=None,
                candle_count=0,
                expected_candle_count=0,
                completion_pct=Decimal("0"),
            )

        timeframe_value = BinanceUSTimeframe.from_code(timeframe)
        expected = int(((int(last_candle.timestamp()) - int(first_candle.timestamp())) // timeframe_value.granularity_seconds) + 1)
        completion_pct = Decimal("0")
        if expected > 0:
            completion_pct = ((Decimal(candle_count) * Decimal("100")) / Decimal(expected)).quantize(Decimal("0.01"))

        return StoredRangeSummary(
            first_candle=first_candle,
            last_candle=last_candle,
            candle_count=candle_count,
            expected_candle_count=expected,
            completion_pct=completion_pct,
        )

    def _recent_window(
        self,
        first_candle: Optional[datetime],
        last_candle: Optional[datetime],
        timeframe: str,
        lookback_days: int,
    ) -> tuple[Optional[datetime], Optional[datetime]]:
        return derive_recent_window(
            first_candle=first_candle,
            last_candle=last_candle,
            timeframe=timeframe,
            lookback_days=lookback_days,
        )

    def _get_duplicate_summary(
        self,
        exchange_id: int,
        symbol_id: int,
        timeframe: str,
        sample_limit: int,
    ) -> DuplicateSummary:
        aggregate_stmt = text(
            """
            with dupes as (
                select open_time, count(*)::int as row_count
                from candles
                where exchange_id = :exchange_id
                  and symbol_id = :symbol_id
                  and timeframe = :timeframe
                group by open_time
                having count(*) > 1
            )
            select
                coalesce(sum(row_count - 1), 0)::int as duplicate_count,
                count(*)::int as duplicate_bucket_count
            from dupes
            """
        )
        duplicate_count, duplicate_bucket_count = self.session.execute(
            aggregate_stmt,
            {
                "exchange_id": exchange_id,
                "symbol_id": symbol_id,
                "timeframe": timeframe,
            },
        ).one()

        sample_stmt = text(
            """
            select open_time, row_count
            from (
                select open_time, count(*)::int as row_count
                from candles
                where exchange_id = :exchange_id
                  and symbol_id = :symbol_id
                  and timeframe = :timeframe
                group by open_time
                having count(*) > 1
                order by open_time asc
                limit :sample_limit
            ) dupes
            """
        )
        samples = [
            DuplicateSample(open_time=row[0], row_count=row[1])
            for row in self.session.execute(
                sample_stmt,
                {
                    "exchange_id": exchange_id,
                    "symbol_id": symbol_id,
                    "timeframe": timeframe,
                    "sample_limit": sample_limit,
                },
            ).all()
        ]

        return DuplicateSummary(
            duplicate_count=int(duplicate_count or 0),
            duplicate_bucket_count=int(duplicate_bucket_count or 0),
            sample_duplicates=samples,
        )

    def _get_timestamp_alignment_summary(
        self,
        exchange_id: int,
        symbol_id: int,
        timeframe: str,
        sample_limit: int,
    ) -> TimestampAlignmentSummary:
        step_seconds = BinanceUSTimeframe.from_code(timeframe).granularity_seconds
        invalid_count = self.session.execute(
            text(
                """
                select count(*)::int
                from candles
                where exchange_id = :exchange_id
                  and symbol_id = :symbol_id
                  and timeframe = :timeframe
                  and mod(extract(epoch from open_time)::bigint, :step_seconds) <> 0
                """
            ),
            {
                "exchange_id": exchange_id,
                "symbol_id": symbol_id,
                "timeframe": timeframe,
                "step_seconds": step_seconds,
            },
        ).scalar_one()

        sample_rows = self.session.execute(
            text(
                """
                select open_time
                from candles
                where exchange_id = :exchange_id
                  and symbol_id = :symbol_id
                  and timeframe = :timeframe
                  and mod(extract(epoch from open_time)::bigint, :step_seconds) <> 0
                order by open_time asc
                limit :sample_limit
                """
            ),
            {
                "exchange_id": exchange_id,
                "symbol_id": symbol_id,
                "timeframe": timeframe,
                "step_seconds": step_seconds,
                "sample_limit": sample_limit,
            },
        ).all()

        return TimestampAlignmentSummary(
            invalid_timestamp_count=int(invalid_count or 0),
            sample_invalid_timestamps=[row[0] for row in sample_rows],
        )

    def _get_gap_summary(
        self,
        exchange_id: int,
        symbol_id: int,
        timeframe: str,
        start_at: Optional[datetime],
        end_at: Optional[datetime],
        sample_limit: int,
    ) -> GapSummary:
        if start_at is None or end_at is None:
            return GapSummary(missing_candle_count=0, sample_missing_timestamps=[])

        step_seconds = BinanceUSTimeframe.from_code(timeframe).granularity_seconds
        params = {
            "exchange_id": exchange_id,
            "symbol_id": symbol_id,
            "timeframe": timeframe,
            "start_at": start_at,
            "end_at": end_at,
            "step_seconds": step_seconds,
            "sample_limit": sample_limit,
        }

        count_stmt = text(
            """
            with expected as (
                select generate_series(
                    CAST(:start_at AS timestamptz),
                    CAST(:end_at AS timestamptz),
                    (:step_seconds || ' seconds')::interval
                ) as open_time
            )
            select count(*)::int
            from expected
            left join candles c
              on c.exchange_id = :exchange_id
             and c.symbol_id = :symbol_id
             and c.timeframe = :timeframe
             and c.open_time = expected.open_time
            where c.id is null
            """
        )
        missing_candle_count = self.session.execute(count_stmt, params).scalar_one()

        sample_stmt = text(
            """
            with expected as (
                select generate_series(
                    CAST(:start_at AS timestamptz),
                    CAST(:end_at AS timestamptz),
                    (:step_seconds || ' seconds')::interval
                ) as open_time
            )
            select expected.open_time
            from expected
            left join candles c
              on c.exchange_id = :exchange_id
             and c.symbol_id = :symbol_id
             and c.timeframe = :timeframe
             and c.open_time = expected.open_time
            where c.id is null
            order by expected.open_time asc
            limit :sample_limit
            """
        )
        sample_missing_timestamps = [
            row[0] for row in self.session.execute(sample_stmt, params).all()
        ]

        return GapSummary(
            missing_candle_count=int(missing_candle_count or 0),
            sample_missing_timestamps=sample_missing_timestamps,
        )

    def _get_api_truthfulness_summary(
        self,
        exchange_code: str,
        symbol: str,
        timeframe: str,
        coverage: CandleCoverageSummary,
    ) -> ApiTruthfulnessSummary:
        notes: list[str] = []
        coverage_matches_db = False
        status_matches_db = False
        latest_sync_job_id: Optional[int] = None
        latest_sync_job_status: Optional[str] = None

        if coverage.requested_start_at is not None and coverage.requested_end_at is not None:
            service_coverage = self.query_service.get_candle_coverage(
                exchange_code=exchange_code,
                symbol=symbol,
                timeframe=timeframe,
                start_at=coverage.requested_start_at,
                end_at=coverage.requested_end_at,
            )
            coverage_matches_db = (
                service_coverage.loaded_start_at == coverage.actual_start_at
                and service_coverage.loaded_end_at == coverage.actual_end_at
                and service_coverage.candle_count == coverage.candle_count
                and service_coverage.expected_candle_count == coverage.expected_candle_count
                and service_coverage.missing_candle_count == coverage.missing_candle_count
                and Decimal(str(service_coverage.completion_pct)) == coverage.completion_pct
            )
            if not coverage_matches_db:
                notes.append("Coverage endpoint result differs from repository aggregate.")

        latest_job = self.session.scalar(
            select(SyncJob)
            .where(
                SyncJob.exchange == exchange_code,
                SyncJob.symbol == symbol,
                SyncJob.timeframe == timeframe,
            )
            .order_by(SyncJob.updated_at.desc(), SyncJob.id.desc())
            .limit(1)
        )
        if latest_job is None:
            notes.append("No sync job found for status endpoint comparison.")
        else:
            latest_sync_job_id = latest_job.id
            latest_sync_job_status = latest_job.status.value
            if latest_job.start_at is not None and latest_job.end_at is not None:
                latest_coverage = self.candle_repository.get_candle_coverage(
                    exchange_code=exchange_code,
                    symbol_code=symbol,
                    timeframe=timeframe,
                    start_at=latest_job.start_at,
                    end_at=latest_job.end_at,
                )
                status_rows = self.query_service.list_sync_jobs(
                    symbol=symbol,
                    timeframe=timeframe,
                    limit=20,
                )
                matching_status_job = next(
                    (job for job in status_rows if job.id == latest_job.id and job.exchange == exchange_code),
                    None,
                )
                if matching_status_job is None or matching_status_job.coverage is None:
                    notes.append("Latest sync job is missing from status endpoint coverage output.")
                else:
                    status_matches_db = (
                        matching_status_job.coverage.loaded_start_at == latest_coverage.actual_start_at
                        and matching_status_job.coverage.loaded_end_at == latest_coverage.actual_end_at
                        and matching_status_job.coverage.candle_count == latest_coverage.candle_count
                        and matching_status_job.coverage.expected_candle_count == latest_coverage.expected_candle_count
                        and matching_status_job.coverage.missing_candle_count == latest_coverage.missing_candle_count
                        and Decimal(str(matching_status_job.coverage.completion_pct))
                        == latest_coverage.completion_pct
                    )
                    if not status_matches_db:
                        notes.append("Status endpoint coverage differs from repository aggregate.")
            else:
                notes.append("Latest sync job has no requested range.")

        return ApiTruthfulnessSummary(
            coverage_endpoint_matches_db=coverage_matches_db,
            status_endpoint_matches_db=status_matches_db,
            latest_sync_job_id=latest_sync_job_id,
            latest_sync_job_status=latest_sync_job_status,
            notes=notes,
        )

    def _run_resync_summary(
        self,
        exchange_code: str,
        exchange_id: int,
        symbol: str,
        symbol_id: int,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> ResyncSummary:
        before = self.candle_repository.get_candle_coverage(
            exchange_code=exchange_code,
            symbol_code=symbol,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
        )
        before_duplicates = self._get_duplicate_summary(exchange_id, symbol_id, timeframe, sample_limit=1)
        result = self.market_data_service.manual_sync(
            exchange_code=exchange_code,
            symbol=symbol,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
        )
        after = self.candle_repository.get_candle_coverage(
            exchange_code=exchange_code,
            symbol_code=symbol,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
        )
        after_duplicates = self._get_duplicate_summary(exchange_id, symbol_id, timeframe, sample_limit=1)
        stable = (
            result.inserted_rows == 0
            and before.candle_count == after.candle_count
            and before.actual_start_at == after.actual_start_at
            and before.actual_end_at == after.actual_end_at
            and after_duplicates.duplicate_count == before_duplicates.duplicate_count
        )

        return ResyncSummary(
            requested_start_at=start_at,
            requested_end_at=end_at,
            before_count=before.candle_count,
            after_count=after.candle_count,
            before_loaded_start_at=before.actual_start_at,
            before_loaded_end_at=before.actual_end_at,
            after_loaded_start_at=after.actual_start_at,
            after_loaded_end_at=after.actual_end_at,
            rows_inserted=result.inserted_rows,
            duplicate_count_after=after_duplicates.duplicate_count,
            stable=stable,
        )

    def _build_issues(
        self,
        stored_range: StoredRangeSummary,
        validation_window: CandleCoverageSummary,
        duplicates: DuplicateSummary,
        alignment: TimestampAlignmentSummary,
        gaps: GapSummary,
        api_truthfulness: ApiTruthfulnessSummary,
        resync: Optional[ResyncSummary],
    ) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []

        if stored_range.candle_count == 0:
            issues.append(
                ValidationIssue(
                    severity="critical",
                    code="empty_dataset",
                    message="No candles are stored for this market/timeframe combination.",
                )
            )
        if validation_window.missing_candle_count > 0:
            issues.append(
                ValidationIssue(
                    severity="critical",
                    code="coverage_gap",
                    message=f"Validation window is missing {validation_window.missing_candle_count} candles.",
                )
            )
        if duplicates.duplicate_count > 0:
            issues.append(
                ValidationIssue(
                    severity="critical",
                    code="duplicates_detected",
                    message=f"Detected {duplicates.duplicate_count} duplicate candle rows.",
                )
            )
        if alignment.invalid_timestamp_count > 0:
            issues.append(
                ValidationIssue(
                    severity="critical",
                    code="invalid_timestamp_grid",
                    message=f"Detected {alignment.invalid_timestamp_count} candles off the timeframe grid.",
                )
            )
        if gaps.missing_candle_count > 0:
            issues.append(
                ValidationIssue(
                    severity="critical",
                    code="internal_gaps",
                    message=f"Detected {gaps.missing_candle_count} missing timestamps inside the validation window.",
                )
            )
        if not api_truthfulness.coverage_endpoint_matches_db:
            issues.append(
                ValidationIssue(
                    severity="critical",
                    code="coverage_endpoint_mismatch",
                    message="Coverage endpoint output does not match repository aggregates.",
                )
            )
        if api_truthfulness.latest_sync_job_id is not None and not api_truthfulness.status_endpoint_matches_db:
            issues.append(
                ValidationIssue(
                    severity="critical",
                    code="status_endpoint_mismatch",
                    message="Status endpoint coverage does not match repository aggregates for the latest sync job.",
                )
            )
        if resync is None:
            issues.append(
                ValidationIssue(
                    severity="medium",
                    code="resync_not_performed",
                    message="Re-sync stability check was not executed in this validator run.",
                )
            )
        elif not resync.stable:
            issues.append(
                ValidationIssue(
                    severity="critical",
                    code="resync_instability",
                    message=(
                        f"Re-sync inserted {resync.rows_inserted} rows or changed coverage unexpectedly "
                        "for an already loaded range."
                    ),
                )
            )

        for note in api_truthfulness.notes:
            if "No sync job found" in note:
                issues.append(
                    ValidationIssue(
                        severity="medium",
                        code="missing_sync_job",
                        message=note,
                    )
                )

        return issues
