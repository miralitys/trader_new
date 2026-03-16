# Trader MVP

Trader MVP is a locally runnable scaffold for algorithmic trading research on Binance.US. It includes:

- candle ingestion from Binance.US into Postgres
- a FastAPI backend with strategy, backtest, paper trading, data, logs, and dashboard endpoints
- a Next.js frontend for operating the system
- a background worker that processes paper trading runs against new candles

The system is intentionally LONG-only and SPOT-only. Live trading is not implemented.

## FundingBasisCarry research module

The backend now includes a research-first data layer for `FundingBasisCarry v1`. This is not wired into the live or paper trading engines. It is a standalone research stack for market-neutral carry analysis using:

- Binance Spot for spot price history
- Binance Futures USD-M for perp mark/index price history and funding rates
- OKX SWAP as an alternative perp venue for funding and basis research
- Binance public archive fallback from `data.binance.vision` when direct REST access is blocked or unreliable

### Stored research tables

- `spot_prices`
- `perp_prices`
- `funding_rates`
- `fee_schedules`

### Snapshot alignment rule

Funding observations are aligned to the nearest available spot and perp snapshot within a configurable maximum window.

- exact timestamp match is preferred when present
- otherwise the nearest snapshot by absolute time difference is used
- if no snapshot exists inside `max_snapshot_alignment_seconds`, that funding observation is excluded from the report

This rule is intentionally explicit so missing or sparse data remains visible in research output.

### Archive fallback behavior

The ingestion layer now supports automatic archive fallback for research ingestion.

- default mode is `auto`: try Binance REST first, then fall back to archive files if REST fails
- you can force archive mode with `--prefer-archive`
- archive fallback currently applies to the Binance venue path only
- archive spot/perp price history can be loaded from monthly and daily zip files
- funding archive fallback currently relies on monthly funding archives

Important limitation:

- if REST is blocked and the current month funding archive has not been published yet, funding data may lag the current month
- the ingestion result includes `spot_source`, `perp_source`, `funding_source`, and `notes` so partial or stale archive behavior stays visible

### Funding basis ingestion

Historical backfill:

```bash
docker compose exec backend python -m app.scripts.funding_basis_ingest history \
  --symbols BTC-USDT,ETH-USDT,SOL-USDT,BNB-USDT,ADA-USDT,ALPINE-USDT,XRP-USDT,1INCH-USDT,LTC-USDT,BCH-USDT,AVAX-USDT,LINK-USDT,DOGE-USDT \
  --perp-venue binance_futures \
  --timeframe 5m \
  --start-at 2026-02-01T00:00:00Z \
  --end-at 2026-03-15T00:00:00Z
```

Incremental refresh:

```bash
docker compose exec backend python -m app.scripts.funding_basis_ingest incremental \
  --symbols BTC-USDT,ETH-USDT,SOL-USDT,BNB-USDT,ADA-USDT,ALPINE-USDT,XRP-USDT,1INCH-USDT,LTC-USDT,BCH-USDT,AVAX-USDT,LINK-USDT,DOGE-USDT \
  --perp-venue binance_futures \
  --timeframe 5m
```

Use OKX SWAP instead of Binance Futures:

```bash
docker compose exec backend python -m app.scripts.funding_basis_ingest history \
  --symbols BTC-USDT,ETH-USDT,SOL-USDT,BNB-USDT,ADA-USDT,ALPINE-USDT,XRP-USDT,1INCH-USDT,LTC-USDT,BCH-USDT,AVAX-USDT,LINK-USDT,DOGE-USDT \
  --perp-venue okx_swap \
  --timeframe 5m \
  --start-at 2026-02-01T00:00:00Z \
  --end-at 2026-03-15T00:00:00Z
```

Force archive mode:

```bash
docker compose exec backend python -m app.scripts.funding_basis_ingest history \
  --symbols BTC-USDT,ETH-USDT,SOL-USDT,BNB-USDT,ADA-USDT,ALPINE-USDT,XRP-USDT,1INCH-USDT,LTC-USDT,BCH-USDT,AVAX-USDT,LINK-USDT,DOGE-USDT \
  --timeframe 5m \
  --start-at 2026-02-01T00:00:00Z \
  --end-at 2026-03-15T00:00:00Z \
  --prefer-archive
```

### Funding basis research report

```bash
docker compose exec backend python -m app.scripts.funding_basis_report \
  --symbols BTC-USDT,ETH-USDT,SOL-USDT,BNB-USDT,ADA-USDT,ALPINE-USDT,XRP-USDT,1INCH-USDT,LTC-USDT,BCH-USDT,AVAX-USDT,LINK-USDT,DOGE-USDT \
  --perp-venue binance_futures \
  --timeframe 5m \
  --start-at 2026-02-01T00:00:00Z \
  --end-at 2026-03-15T00:00:00Z \
  --min-funding-rate 0.0001 \
  --min-basis-pct 0.0005 \
  --notional-usd 10000 \
  --spot-fee-pct 0.001 \
  --perp-fee-pct 0.0005 \
  --slippage-pct 0.0003 \
  --max-snapshot-alignment-seconds 600
```

The report computes:

- aligned spot reference price
- aligned perp reference price
- `basis_pct = (perp - spot) / spot`
- expected gross funding carry for fixed notional
- simplified entry/exit fees and slippage
- expected net carry after trading friction

### Funding basis venue comparison

```bash
docker compose exec backend python -m app.scripts.funding_basis_compare \
  --symbols BTC-USDT,ETH-USDT,SOL-USDT,BNB-USDT,ADA-USDT,ALPINE-USDT,XRP-USDT,1INCH-USDT,LTC-USDT,BCH-USDT,AVAX-USDT,LINK-USDT,DOGE-USDT \
  --perp-venues binance_futures,okx_swap \
  --timeframe 5m \
  --start-at 2026-02-01T00:00:00Z \
  --end-at 2026-03-15T00:00:00Z \
  --min-funding-rate 0.0001 \
  --min-basis-pct 0.0005 \
  --notional-usd 10000 \
  --spot-fee-pct 0.001 \
  --perp-fee-pct 0.0005 \
  --slippage-pct 0.0003 \
  --max-snapshot-alignment-seconds 600
```

The comparison report returns:

- one full research report per perp venue
- per-symbol comparison of `best_net_carry_venue`
- per-symbol comparison of `best_funding_rate_venue`
- per-symbol comparison of `best_basis_venue`
- `viable_venues` for each symbol under the current screening config

## Stack

- Frontend: Next.js, TypeScript, App Router, Tailwind CSS, React Query, Recharts
- Backend: FastAPI, SQLAlchemy, Pydantic, Alembic
- Data stores: PostgreSQL, Redis
- Infra: Docker, Docker Compose

## Project structure

```text
.
├── apps
│   ├── api
│   │   ├── alembic
│   │   └── app
│   │       ├── api
│   │       ├── db
│   │       ├── engines
│   │       ├── integrations
│   │       ├── models
│   │       ├── repositories
│   │       ├── schemas
│   │       ├── services
│   │       ├── strategies
│   │       ├── tests
│   │       └── workers
│   └── web
│       ├── app
│       ├── components
│       └── lib
├── docker-compose.yml
└── .env.example
```

## Services

- `frontend`: Next.js UI on `http://localhost:3000`
- `backend`: FastAPI on `http://localhost:8000`
- `postgres`: PostgreSQL on `localhost:5432`
- `redis`: Redis on `localhost:6379`
- `worker`: background paper trading loop

## Deploy on Render

The repository includes a Render Blueprint config at `render.yaml`.

Recommended deployment flow:

1. Push this repository to GitHub.
2. In Render, choose `New +` -> `Blueprint`.
3. Select the GitHub repository.
4. Render will provision:
   - `trader-mvp-db` PostgreSQL
   - `trader-mvp-backend` FastAPI web service
   - `trader-mvp-worker` background worker
   - `trader-mvp-frontend` Next.js web service
5. After the first deploy, if you rename services in Render, update:
   - backend `ALLOWED_ORIGINS`
   - frontend `NEXT_PUBLIC_API_URL`

Notes:

- Backend Render services are pinned to Python 3.12 via `apps/api/.python-version` to avoid `pydantic-core` source builds on newer Python runtimes.
- The backend automatically normalizes Render Postgres URLs from `postgresql://...` to `postgresql+psycopg://...`.
- Backend and worker run `alembic upgrade head` and `python -m app.db.seed` in their startup commands, because Render free-tier Blueprint services do not support `preDeployCommand`.
- Redis is optional for the current MVP deployment and is not required by the active worker/runtime path.
- The worker requires a paid Render worker plan. Frontend/backend can stay on free plans for MVP testing.

## Local startup

1. Copy env file:

```bash
cp .env.example .env
```

2. Build and start everything:

```bash
docker compose up --build
```

3. Open:

- frontend: [http://localhost:3000](http://localhost:3000)
- backend docs: [http://localhost:8000/docs](http://localhost:8000/docs)
- backend health: [http://localhost:8000/api/health](http://localhost:8000/api/health)

The backend container automatically runs migrations and seed data on startup. The worker does the same before entering the processing loop.

## Migrations

Apply migrations manually:

```bash
docker compose exec backend alembic upgrade head
```

Inspect current revision:

```bash
docker compose exec backend alembic current
```

## Seed reference data

Seed exchanges, symbols, timeframes, strategies, default strategy configs, and paper accounts:

```bash
docker compose exec backend python -m app.db.seed
```

## Run sync

### From Swagger UI

Open [http://localhost:8000/docs](http://localhost:8000/docs) and use `POST /api/data/sync`.

### From curl

Manual range sync:

```bash
curl -X POST http://localhost:8000/api/data/sync \
  -H "Content-Type: application/json" \
  -d '{
    "mode": "manual",
    "symbol": "BTC-USDT",
    "timeframe": "5m",
    "start_at": "2026-03-01T00:00:00Z",
    "end_at": "2026-03-02T00:00:00Z"
  }'
```

Incremental sync:

```bash
curl -X POST http://localhost:8000/api/data/sync \
  -H "Content-Type: application/json" \
  -d '{
    "mode": "incremental",
    "symbol": "BTC-USDT",
    "timeframe": "5m"
  }'
```

Check sync status:

```bash
curl http://localhost:8000/api/data/status
```

## Run backtest

```bash
curl -X POST http://localhost:8000/api/backtests/run \
  -H "Content-Type: application/json" \
  -d '{
    "strategy_code": "breakout_retest",
    "symbol": "BTC-USDT",
    "timeframe": "5m",
    "start_at": "2026-03-01T00:00:00Z",
    "end_at": "2026-03-07T00:00:00Z",
    "exchange_code": "binance_us",
    "initial_capital": "10000",
    "fee": "0.001",
    "slippage": "0.0005",
    "position_size_pct": "0.25",
    "strategy_config_override": {}
  }'
```

List backtests:

```bash
curl http://localhost:8000/api/backtests
```

Open one backtest:

```bash
curl http://localhost:8000/api/backtests/1
```

## Start and stop paper trading

Start a paper run:

```bash
curl -X POST http://localhost:8000/api/strategies/breakout_retest/start-paper \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["BTC-USDT"],
    "timeframes": ["5m"],
    "exchange_code": "binance_us",
    "initial_balance": "10000",
    "currency": "USD",
    "fee": "0.001",
    "slippage": "0.0005",
    "strategy_config_override": {},
    "metadata": {}
  }'
```

Stop the active paper run:

```bash
curl -X POST http://localhost:8000/api/strategies/breakout_retest/stop-paper \
  -H "Content-Type: application/json" \
  -d '{"reason": "manual_stop"}'
```

The `worker` service will poll for active paper runs and process newly ingested candles.

## Useful API requests

List strategies:

```bash
curl http://localhost:8000/api/strategies
```

Get dashboard summary:

```bash
curl http://localhost:8000/api/dashboard/summary
```

Get candles for a range:

```bash
curl "http://localhost:8000/api/candles?symbol=BTC-USDT&timeframe=5m&start_at=2026-03-01T00:00:00Z&end_at=2026-03-02T00:00:00Z"
```

List trades:

```bash
curl http://localhost:8000/api/trades
```

List positions:

```bash
curl http://localhost:8000/api/positions
```

## Running tests

### Backend

```bash
cd apps/api
python3 -m pip install -e '.[dev]'
python3 -m pytest -q
```

### Frontend

```bash
cd apps/web
npm install
npm run build
npm run lint
```

## Notes

- Redis is included for local parity and future async expansion, even though the current worker loop is lightweight.
- The backend assumes Binance.US-supported timeframes `5m`, `15m`, and `1h`.
- Frontend API calls use `NEXT_PUBLIC_API_URL`, which defaults to `http://localhost:8000`.
