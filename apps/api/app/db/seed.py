from __future__ import annotations

from sqlalchemy import delete, select

from app.core.config import get_settings
from app.db.session import session_scope
from app.models import (
    AppLog,
    BacktestResult,
    BacktestRun,
    Candle,
    Exchange,
    Order,
    PaperAccount,
    Position,
    Signal,
    Strategy,
    StrategyConfig,
    StrategyRun,
    Symbol,
    SyncJob,
    Timeframe,
    Trade,
)
from app.models.enums import AppLogLevel
from app.strategies.registry import list_strategies
from app.utils.exchanges import normalize_exchange_code
from app.utils.symbols import supported_symbol_codes


BINANCE_SCOPE_CLEANUP_MARKER = "cleanup:binance_us_scope_v3"


def _run_scope_cleanup(session) -> None:
    marker_exists = session.scalar(
        select(AppLog.id).where(
            AppLog.scope == "db.seed",
            AppLog.message == BINANCE_SCOPE_CLEANUP_MARKER,
        )
    )
    if marker_exists is not None:
        return

    settings = get_settings()
    allowed_exchange_code = "binance_us"
    allowed_symbols = set(settings.default_symbol_list) or set(supported_symbol_codes())

    cleanup_counts: dict[str, int] = {}

    cleanup_counts["sync_jobs_deleted"] = session.execute(delete(SyncJob)).rowcount or 0
    cleanup_counts["candles_deleted"] = session.execute(delete(Candle)).rowcount or 0

    strategy_runs = list(session.scalars(select(StrategyRun)))
    bad_strategy_run_ids = [
        run.id
        for run in strategy_runs
        if _strategy_run_out_of_scope(run, allowed_exchange_code=allowed_exchange_code, allowed_symbols=allowed_symbols)
    ]
    if bad_strategy_run_ids:
        cleanup_counts["orders_deleted"] = session.execute(
            delete(Order).where(Order.strategy_run_id.in_(bad_strategy_run_ids))
        ).rowcount or 0
        cleanup_counts["signals_deleted"] = session.execute(
            delete(Signal).where(Signal.strategy_run_id.in_(bad_strategy_run_ids))
        ).rowcount or 0
        cleanup_counts["positions_deleted"] = session.execute(
            delete(Position).where(Position.strategy_run_id.in_(bad_strategy_run_ids))
        ).rowcount or 0
        cleanup_counts["trades_deleted"] = session.execute(
            delete(Trade).where(Trade.strategy_run_id.in_(bad_strategy_run_ids))
        ).rowcount or 0
        cleanup_counts["strategy_runs_deleted"] = session.execute(
            delete(StrategyRun).where(StrategyRun.id.in_(bad_strategy_run_ids))
        ).rowcount or 0
    else:
        cleanup_counts["orders_deleted"] = 0
        cleanup_counts["signals_deleted"] = 0
        cleanup_counts["positions_deleted"] = 0
        cleanup_counts["trades_deleted"] = 0
        cleanup_counts["strategy_runs_deleted"] = 0

    cleanup_counts["backtest_results_deleted"] = session.execute(delete(BacktestResult)).rowcount or 0
    cleanup_counts["backtest_runs_deleted"] = session.execute(delete(BacktestRun)).rowcount or 0

    sanitized_strategy_configs = 0
    for strategy_config in session.scalars(select(StrategyConfig)):
        sanitized_config = _sanitize_strategy_config_payload(strategy_config.config_json, allowed_symbols)
        if sanitized_config != strategy_config.config_json:
            strategy_config.config_json = sanitized_config
            session.add(strategy_config)
            sanitized_strategy_configs += 1
    cleanup_counts["strategy_configs_sanitized"] = sanitized_strategy_configs

    symbols = list(session.scalars(select(Symbol)))
    bad_symbol_ids = [
        symbol.id
        for symbol in symbols
        if symbol.code not in allowed_symbols
        or _exchange_code_for_symbol(session, symbol.id) != allowed_exchange_code
    ]
    if bad_symbol_ids:
        cleanup_counts["symbols_deleted"] = session.execute(
            delete(Symbol).where(Symbol.id.in_(bad_symbol_ids))
        ).rowcount or 0
    else:
        cleanup_counts["symbols_deleted"] = 0

    exchanges = list(session.scalars(select(Exchange)))
    bad_exchange_ids = [exchange.id for exchange in exchanges if exchange.code != allowed_exchange_code]
    if bad_exchange_ids:
        cleanup_counts["exchanges_deleted"] = session.execute(
            delete(Exchange).where(Exchange.id.in_(bad_exchange_ids))
        ).rowcount or 0
    else:
        cleanup_counts["exchanges_deleted"] = 0

    session.add(
        AppLog(
            scope="db.seed",
            level=AppLogLevel.INFO,
            message=BINANCE_SCOPE_CLEANUP_MARKER,
            payload_json={
                "allowed_exchange_code": allowed_exchange_code,
                "allowed_symbols": sorted(allowed_symbols),
                **cleanup_counts,
            },
        )
    )
    session.flush()


def _strategy_run_out_of_scope(run: StrategyRun, *, allowed_exchange_code: str, allowed_symbols: set[str]) -> bool:
    metadata = dict(run.metadata_json or {})
    raw_exchange_code = str(metadata.get("exchange_code", allowed_exchange_code))
    try:
        normalized_exchange_code = normalize_exchange_code(raw_exchange_code)
    except ValueError:
        return True

    symbols = [str(symbol).strip().upper() for symbol in (run.symbols_json or []) if str(symbol).strip()]
    return normalized_exchange_code != allowed_exchange_code or any(symbol not in allowed_symbols for symbol in symbols)


def _backtest_run_out_of_scope(run: BacktestRun, *, allowed_exchange_code: str, allowed_symbols: set[str]) -> bool:
    params = dict(run.params_json or {})
    raw_exchange_code = str(params.get("exchange_code", allowed_exchange_code))
    try:
        normalized_exchange_code = normalize_exchange_code(raw_exchange_code)
    except ValueError:
        return True

    symbol = str(params.get("symbol", "")).strip().upper()
    return normalized_exchange_code != allowed_exchange_code or (bool(symbol) and symbol not in allowed_symbols)


def _exchange_code_for_symbol(session, symbol_id: int) -> str | None:
    return session.scalar(
        select(Exchange.code)
        .join(Symbol, Symbol.exchange_id == Exchange.id)
        .where(Symbol.id == symbol_id)
    )


def _sanitize_strategy_config_payload(config_json: dict[str, object], allowed_symbols: set[str]) -> dict[str, object]:
    payload = dict(config_json or {})

    raw_symbols = payload.get("symbols")
    if isinstance(raw_symbols, list):
        payload["symbols"] = [
            str(symbol).strip().upper()
            for symbol in raw_symbols
            if str(symbol).strip().upper() in allowed_symbols
        ]

    raw_exchange_code = payload.get("exchange_code")
    if raw_exchange_code is not None:
        payload["exchange_code"] = "binance_us"

    return payload


def seed_reference_data() -> None:
    settings = get_settings()
    timeframe_rows = {
        "5m": {"code": "5m", "name": "5 Minutes", "duration_seconds": 300},
        "15m": {"code": "15m", "name": "15 Minutes", "duration_seconds": 900},
        "1h": {"code": "1h", "name": "1 Hour", "duration_seconds": 3600},
    }

    with session_scope() as session:
        _run_scope_cleanup(session)

        binance_us_exchange = session.scalar(select(Exchange).where(Exchange.code == "binance_us"))
        if binance_us_exchange is None:
            binance_us_exchange = Exchange(
                code="binance_us",
                name="Binance.US",
                description="Binance.US exchange reference record.",
                is_active=True,
            )
            session.add(binance_us_exchange)
            session.flush()

        for timeframe_code in settings.default_timeframe_list:
            timeframe = timeframe_rows.get(timeframe_code)
            if timeframe is None:
                continue

            exists = session.scalar(select(Timeframe).where(Timeframe.code == timeframe["code"]))
            if exists is None:
                session.add(Timeframe(**timeframe, is_active=True))

        for symbol_code in settings.default_symbol_list:
            exists = session.scalar(
                select(Symbol).where(Symbol.exchange_id == binance_us_exchange.id, Symbol.code == symbol_code)
            )
            if exists is not None:
                continue

            if "-" not in symbol_code:
                continue

            base_asset, quote_asset = symbol_code.split("-", 1)
            session.add(
                Symbol(
                    exchange_id=binance_us_exchange.id,
                    code=symbol_code,
                    base_asset=base_asset,
                    quote_asset=quote_asset,
                    price_precision=2,
                    qty_precision=8,
                    is_active=True,
                )
            )

        session.flush()

        for strategy_impl in list_strategies():
            strategy = session.scalar(select(Strategy).where(Strategy.code == strategy_impl.key))
            if strategy is None:
                strategy = Strategy(
                    code=strategy_impl.key,
                    name=strategy_impl.name,
                    description=strategy_impl.description,
                    is_active=True,
                )
                session.add(strategy)
                session.flush()

            strategy_config = session.scalar(
                select(StrategyConfig).where(
                    StrategyConfig.strategy_id == strategy.id,
                    StrategyConfig.is_active.is_(True),
                )
            )
            if strategy_config is None:
                session.add(
                    StrategyConfig(
                        strategy_id=strategy.id,
                        config_json=strategy_impl.default_config(),
                        is_active=True,
                    )
                )

            paper_account = session.scalar(
                select(PaperAccount).where(PaperAccount.strategy_id == strategy.id)
            )
            if paper_account is None:
                session.add(
                    PaperAccount(
                        strategy_id=strategy.id,
                        balance=10000,
                        currency="USD",
                        is_active=True,
                    )
                )


if __name__ == "__main__":
    seed_reference_data()
