from __future__ import annotations

from decimal import Decimal
from typing import Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.models import PaperAccount
from app.repositories.base import BaseRepository


class PaperAccountRepository(BaseRepository):
    def get_by_strategy_id(self, strategy_id: int) -> Optional[PaperAccount]:
        stmt = select(PaperAccount).where(PaperAccount.strategy_id == strategy_id)
        return self.session.scalar(stmt)

    def ensure_account(
        self,
        strategy_id: int,
        balance: Decimal,
        currency: str = "USD",
        reset_existing: bool = False,
    ) -> PaperAccount:
        account = self.get_by_strategy_id(strategy_id)
        if account is not None:
            if reset_existing:
                account.balance = balance
                account.currency = currency
                self.session.add(account)
                self.session.flush()
            return account

        stmt = (
            insert(PaperAccount)
            .values(
                strategy_id=strategy_id,
                balance=balance,
                currency=currency,
                is_active=True,
            )
            .on_conflict_do_nothing(index_elements=["strategy_id"])
            .returning(PaperAccount.id)
        )
        inserted_id = self.session.scalar(stmt)
        if inserted_id is not None:
            return self.session.get(PaperAccount, inserted_id)

        account = self.get_by_strategy_id(strategy_id)
        if account is None:
            raise ValueError(f"Paper account for strategy {strategy_id} could not be resolved")
        return account

    def update_balance(self, account: PaperAccount, balance: Decimal) -> PaperAccount:
        account.balance = balance
        self.session.add(account)
        self.session.flush()
        return account
