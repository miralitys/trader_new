from __future__ import annotations

from sqlalchemy import select

from app.core.database import session_scope
from app.domain.models import Exchange, PaperAccount, Symbol, Timeframe, User
from app.repositories.strategy import StrategyRepository
from app.strategies.registry import list_strategies


def seed() -> None:
    with session_scope() as db:
        if db.scalar(select(User).where(User.email == "admin@local")) is None:
            db.add(User(email="admin@local", name="Local Admin"))

        exchange = db.scalar(select(Exchange).where(Exchange.slug == "coinbase"))
        if exchange is None:
            exchange = Exchange(name="Coinbase", slug="coinbase", adapter_key="coinbase")
            db.add(exchange)
            db.flush()

        for code, seconds, label in [("5m", 300, "5 Minutes"), ("15m", 900, "15 Minutes"), ("1h", 3600, "1 Hour")]:
            if db.scalar(select(Timeframe).where(Timeframe.code == code)) is None:
                db.add(Timeframe(code=code, seconds=seconds, label=label))

        for symbol_code in ["BTC-USD", "ETH-USD", "SOL-USD"]:
            if db.scalar(
                select(Symbol).where(Symbol.exchange_id == exchange.id, Symbol.symbol == symbol_code)
            ) is None:
                base_asset, quote_asset = symbol_code.split("-")
                db.add(
                    Symbol(
                        exchange_id=exchange.id,
                        symbol=symbol_code,
                        base_asset=base_asset,
                        quote_asset=quote_asset,
                    )
                )

        repo = StrategyRepository(db)
        for strategy_impl in list_strategies():
            strategy = repo.get_strategy_by_key(strategy_impl.key)
            if strategy is None:
                from app.domain.models import Strategy

                strategy = Strategy(
                    key=strategy_impl.key,
                    name=strategy_impl.name,
                    description=strategy_impl.description,
                )
                db.add(strategy)
                db.flush()

            account_name = f"Paper::{strategy_impl.name}"
            paper_account = db.scalar(select(PaperAccount).where(PaperAccount.name == account_name))
            if paper_account is None:
                paper_account = PaperAccount(name=account_name, balance=10000.0, equity=10000.0)
                db.add(paper_account)
                db.flush()

            repo.upsert_config(
                strategy_id=strategy.id,
                settings=strategy_impl.default_config(),
                risk_settings={"max_open_positions": 1, "long_only": True, "spot_only": True},
                symbols=["BTC-USD", "ETH-USD"],
                timeframes=["5m", "15m"],
                paper_account_id=paper_account.id,
            )


if __name__ == "__main__":
    seed()
