from __future__ import annotations

from sqlalchemy import select

from app.core.config import get_settings
from app.db.session import session_scope
from app.models import Exchange, PaperAccount, Strategy, StrategyConfig, Symbol, Timeframe
from app.strategies.registry import list_strategies


def seed_reference_data() -> None:
    settings = get_settings()
    timeframe_rows = {
        "5m": {"code": "5m", "name": "5 Minutes", "duration_seconds": 300},
        "15m": {"code": "15m", "name": "15 Minutes", "duration_seconds": 900},
        "1h": {"code": "1h", "name": "1 Hour", "duration_seconds": 3600},
    }

    with session_scope() as session:
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
