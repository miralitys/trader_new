# Trader MVP

Trader MVP is a locally runnable scaffold for algorithmic trading research on Coinbase. It includes:

- candle ingestion from Coinbase into Postgres
- a FastAPI backend with strategy, backtest, paper trading, data, logs, and dashboard endpoints
- a Next.js frontend for operating the system
- a background worker that processes paper trading runs against new candles

The system is intentionally LONG-only and SPOT-only. Live trading is not implemented.

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
    "symbol": "BTC-USD",
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
    "symbol": "BTC-USD",
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
    "symbol": "BTC-USD",
    "timeframe": "5m",
    "start_at": "2026-03-01T00:00:00Z",
    "end_at": "2026-03-07T00:00:00Z",
    "exchange_code": "coinbase",
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
    "symbols": ["BTC-USD"],
    "timeframes": ["5m"],
    "exchange_code": "coinbase",
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
curl "http://localhost:8000/api/candles?symbol=BTC-USD&timeframe=5m&start_at=2026-03-01T00:00:00Z&end_at=2026-03-02T00:00:00Z"
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
- The backend assumes Coinbase-supported timeframes `5m`, `15m`, and `1h`.
- Frontend API calls use `NEXT_PUBLIC_API_URL`, which defaults to `http://localhost:8000`.
