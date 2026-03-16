from __future__ import annotations

import csv
import io
import zipfile
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal

import httpx

from app.core.config import Settings
from app.integrations.binance_archive import BinanceArchiveClient
from app.integrations.binance_futures import BinanceFuturesClientError
from app.integrations.binance_spot import BinanceSpotClientError
from app.services import funding_basis_ingestion_service as service_module
from app.services.funding_basis_ingestion_service import FundingBasisIngestionService


def _zip_csv(filename: str, rows: list[list[str]]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer, lineterminator="\n")
        writer.writerows(rows)
        archive.writestr(filename, csv_buffer.getvalue())
    return buffer.getvalue()


def test_binance_archive_client_parses_spot_microsecond_archive() -> None:
    payload = _zip_csv(
        "BTCUSDT-5m-2025-02.csv",
        [
            ["1738368000000000", "1", "2", "0.5", "1.5", "123"],
            ["1738368300000000", "1.5", "2", "1.0", "1.9", "456"],
        ],
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/data/spot/monthly/klines/BTCUSDT/5m/BTCUSDT-5m-2025-02.zip")
        return httpx.Response(200, content=payload)

    client = httpx.Client(
        transport=httpx.MockTransport(handler),
        base_url="https://data.binance.vision/data",
    )
    archive_client = BinanceArchiveClient(
        settings=Settings(FUNDING_BASIS_ARCHIVE_BASE_URL="https://data.binance.vision/data"),
        client=client,
    )

    chunks = list(
        archive_client.iter_spot_klines(
            symbol="BTC-USDT",
            timeframe="5m",
            start_at=datetime(2025, 2, 1, 0, 0, tzinfo=timezone.utc),
            end_at=datetime(2025, 2, 1, 0, 6, tzinfo=timezone.utc),
        )
    )

    assert len(chunks) == 1
    assert chunks[0][0][0] == 1738368000000
    assert chunks[0][1][0] == 1738368300000
    archive_client.close()


def test_funding_basis_ingestion_service_falls_back_to_archive(monkeypatch) -> None:
    class FailingSpotClient:
        def iter_historical_klines(self, **_: object):
            raise BinanceSpotClientError("451 restricted")

        def close(self) -> None:
            return None

    class FailingFuturesClient:
        def iter_mark_price_klines(self, **_: object):
            raise BinanceFuturesClientError("451 restricted")

        def iter_index_price_klines(self, **_: object):
            raise AssertionError("rest fallback should not reach index endpoint")

        def iter_trade_klines(self, **_: object):
            raise AssertionError("rest fallback should not reach trade endpoint")

        def iter_open_interest_hist(self, **_: object):
            raise AssertionError("rest fallback should not reach oi endpoint")

        def iter_funding_rates(self, **_: object):
            raise BinanceFuturesClientError("451 restricted")

        def close(self) -> None:
            return None

    class FakeArchiveClient:
        def iter_spot_klines(self, **_: object):
            yield [[1738368000000, "1", "2", "0.5", "100", "3"]]

        def iter_mark_price_klines(self, **_: object):
            yield [[1738368000000, "1", "2", "0.5", "101", "0"]]

        def iter_index_price_klines(self, **_: object):
            yield [[1738368000000, "1", "2", "0.5", "100.5", "0"]]

        def iter_trade_klines(self, **_: object):
            yield [[1738368000000, "1", "2", "0.5", "101", "9"]]

        def iter_funding_rates(self, **_: object):
            yield [{"fundingTime": 1738368000000, "fundingRate": "0.0003"}]

        def close(self) -> None:
            return None

    captured: dict[str, object] = {}

    class FakeRepository:
        def __init__(self, session: object) -> None:
            self.session = session

        def upsert_spot_prices(self, rows):
            row_list = list(rows)
            captured["spot"] = row_list
            return len(row_list)

        def upsert_perp_prices(self, rows):
            row_list = list(rows)
            captured["perp"] = row_list
            return len(row_list)

        def upsert_funding_rates(self, rows):
            row_list = list(rows)
            captured["funding"] = row_list
            return len(row_list)

    @contextmanager
    def fake_session_scope():
        yield object()

    monkeypatch.setattr(service_module, "FundingBasisRepository", FakeRepository)
    monkeypatch.setattr(service_module, "session_scope", fake_session_scope)

    service = FundingBasisIngestionService(
        settings=Settings(),
        spot_client=FailingSpotClient(),
        futures_client=FailingFuturesClient(),
        archive_client=FakeArchiveClient(),
    )

    result = service._ingest_symbol_range(
        symbol="BTC-USDT",
        timeframe="5m",
        start_at=datetime(2025, 2, 1, 0, 0, tzinfo=timezone.utc),
        end_at=datetime(2025, 2, 1, 8, 0, tzinfo=timezone.utc),
    )

    assert result.spot_rows_inserted == 1
    assert result.perp_rows_inserted == 1
    assert result.funding_rows_inserted == 1
    assert result.spot_source == "archive"
    assert result.perp_source == "archive"
    assert result.funding_source == "archive"
    assert any(note.startswith("spot_rest_failed_fallback_archive:") for note in result.notes)
    assert any(note.startswith("perp_rest_failed_fallback_archive:") for note in result.notes)
    assert any(note.startswith("funding_rest_failed_fallback_archive:") for note in result.notes)
    assert "perp_open_interest_unavailable_in_archive" in result.notes
    assert captured["spot"][0].close == 100
    assert captured["perp"][0].mark_price == 101
    assert captured["funding"][0].funding_rate == captured["funding"][0].realized_funding_rate


def test_funding_basis_ingestion_service_supports_okx_swap(monkeypatch) -> None:
    class FakeSpotClient:
        def iter_historical_klines(self, **_: object):
            yield [[1738368000000, "1", "2", "0.5", "100", "3"]]

        def close(self) -> None:
            return None

    class FakeFuturesClient:
        def close(self) -> None:
            return None

    class FakeArchiveClient:
        def close(self) -> None:
            return None

    class FakeOkxClient:
        def iter_mark_price_klines(self, **_: object):
            yield [["1738368000000", "1", "2", "0.5", "101", "0"]]

        def iter_index_price_klines(self, **_: object):
            yield [["1738368000000", "1", "2", "0.5", "100.5", "0"]]

        def iter_trade_klines(self, **_: object):
            yield [["1738368000000", "1", "2", "0.5", "101", "9", "0", "0", "1"]]

        def iter_funding_rates(self, **_: object):
            yield [{"fundingTime": "1738368000000", "fundingRate": "0.0003", "realizedRate": "0.00029"}]

        def close(self) -> None:
            return None

    captured: dict[str, object] = {}

    class FakeRepository:
        def __init__(self, session: object) -> None:
            self.session = session

        def upsert_spot_prices(self, rows):
            row_list = list(rows)
            captured["spot"] = row_list
            return len(row_list)

        def upsert_perp_prices(self, rows):
            row_list = list(rows)
            captured["perp"] = row_list
            return len(row_list)

        def upsert_funding_rates(self, rows):
            row_list = list(rows)
            captured["funding"] = row_list
            return len(row_list)

    @contextmanager
    def fake_session_scope():
        yield object()

    monkeypatch.setattr(service_module, "FundingBasisRepository", FakeRepository)
    monkeypatch.setattr(service_module, "session_scope", fake_session_scope)

    service = FundingBasisIngestionService(
        settings=Settings(),
        spot_client=FakeSpotClient(),
        futures_client=FakeFuturesClient(),
        okx_client=FakeOkxClient(),
        archive_client=FakeArchiveClient(),
        perp_exchange="okx_swap",
    )

    result = service._ingest_symbol_range(
        symbol="BTC-USDT",
        timeframe="5m",
        start_at=datetime(2025, 2, 1, 0, 0, tzinfo=timezone.utc),
        end_at=datetime(2025, 2, 1, 8, 0, tzinfo=timezone.utc),
    )

    assert result.perp_rows_inserted == 1
    assert result.funding_rows_inserted == 1
    assert result.perp_source == "rest"
    assert result.funding_source == "rest"
    assert "perp_open_interest_not_supported_by_okx_history" in result.notes
    assert captured["perp"][0].exchange == "okx_swap"
    assert captured["perp"][0].index_price == Decimal("100.5")
    assert captured["funding"][0].exchange == "okx_swap"
    assert captured["funding"][0].realized_funding_rate == Decimal("0.00029")
