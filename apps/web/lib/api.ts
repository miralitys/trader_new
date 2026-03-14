import type {
  ApiErrorPayload,
  AppLog,
  BacktestFilters,
  BacktestListItem,
  BacktestResponse,
  BacktestRunRequest,
  BacktestStopRequest,
  Candle,
  CandleFilters,
  DashboardSummary,
  DataSyncRequest,
  DataSyncResponse,
  HealthResponse,
  LogFilters,
  Position,
  PositionFilters,
  PaperRunResponse,
  Signal,
  SignalFilters,
  StrategyConfigResponse,
  StrategyConfigUpdateRequest,
  StrategyDetail,
  StrategyPaperStartRequest,
  StrategyPaperStopRequest,
  StrategyRunDetail,
  StrategyRunFilters,
  StrategySummary,
  SyncJob,
  SyncJobFilters,
  Trade,
  TradeFilters,
} from "@/lib/types";

const fallbackApiBaseUrl = "http://localhost:8000";
export const publicApiBaseUrl = (process.env.NEXT_PUBLIC_API_URL ?? fallbackApiBaseUrl).replace(/\/$/, "");

type QueryRecord = Record<string, string | number | boolean | undefined | null>;

function buildQueryString(query: QueryRecord) {
  const searchParams = new URLSearchParams();

  Object.entries(query).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") {
      return;
    }
    searchParams.set(key, String(value));
  });

  const serialized = searchParams.toString();
  return serialized ? `?${serialized}` : "";
}

async function apiRequest<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${publicApiBaseUrl}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
  });

  if (!response.ok) {
    let payload: ApiErrorPayload | null = null;

    try {
      payload = (await response.json()) as ApiErrorPayload;
    } catch {
      payload = null;
    }

    throw new Error(payload?.detail ?? `API request failed: ${response.status}`);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  return (await response.json()) as T;
}

export function getHealth() {
  return apiRequest<HealthResponse>("/api/health");
}

export function getDashboardSummary() {
  return apiRequest<DashboardSummary>("/api/dashboard/summary");
}

export function getStrategies() {
  return apiRequest<StrategySummary[]>("/api/strategies");
}

export function getStrategy(code: string) {
  return apiRequest<StrategyDetail>(`/api/strategies/${code}`);
}

export function getStrategyConfig(code: string) {
  return apiRequest<StrategyConfigResponse>(`/api/strategies/${code}/config`);
}

export function updateStrategyConfig(code: string, payload: StrategyConfigUpdateRequest) {
  return apiRequest<StrategyConfigResponse>(`/api/strategies/${code}/config`, {
    method: "PUT",
    body: JSON.stringify(payload),
  });
}

export function startStrategyPaper(code: string, payload: StrategyPaperStartRequest) {
  return apiRequest<PaperRunResponse>(`/api/strategies/${code}/start-paper`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function stopStrategyPaper(code: string, payload: StrategyPaperStopRequest) {
  return apiRequest<PaperRunResponse>(`/api/strategies/${code}/stop-paper`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function getStrategyRuns(filters: StrategyRunFilters = {}) {
  return apiRequest<StrategyRunDetail[]>(
    `/api/strategy-runs${buildQueryString({
      strategy_code: filters.strategyCode,
      status: filters.status,
      mode: filters.mode,
      limit: filters.limit,
    })}`,
  );
}

export function getStrategyRun(id: number) {
  return apiRequest<StrategyRunDetail>(`/api/strategy-runs/${id}`);
}

export function getBacktests(filters: BacktestFilters = {}) {
  return apiRequest<BacktestListItem[]>(
    `/api/backtests${buildQueryString({
      strategy_code: filters.strategyCode,
      status: filters.status,
      limit: filters.limit,
    })}`,
  );
}

export function getBacktest(id: number) {
  return apiRequest<BacktestResponse>(`/api/backtests/${id}`);
}

export function runBacktest(payload: BacktestRunRequest) {
  return apiRequest<BacktestResponse>("/api/backtests/run", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function stopBacktest(id: number, payload: BacktestStopRequest) {
  return apiRequest<BacktestResponse>(`/api/backtests/${id}/stop`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function runDataSync(payload: DataSyncRequest) {
  return apiRequest<DataSyncResponse>("/api/data/sync", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function getSyncJobs(filters: SyncJobFilters = {}) {
  return apiRequest<SyncJob[]>(
    `/api/data/status${buildQueryString({
      status: filters.status,
      symbol: filters.symbol,
      timeframe: filters.timeframe,
      limit: filters.limit,
    })}`,
  );
}

export function getCandles(filters: CandleFilters) {
  return apiRequest<Candle[]>(
    `/api/candles${buildQueryString({
      symbol: filters.symbol,
      timeframe: filters.timeframe,
      start_at: filters.startAt,
      end_at: filters.endAt,
      exchange_code: filters.exchangeCode ?? "coinbase",
      limit: filters.limit,
    })}`,
  );
}

export function getSignals(filters: SignalFilters = {}) {
  return apiRequest<Signal[]>(
    `/api/signals${buildQueryString({
      strategy_run_id: filters.strategyRunId,
      symbol: filters.symbol,
      timeframe: filters.timeframe,
      limit: filters.limit,
    })}`,
  );
}

export function getTrades(filters: TradeFilters = {}) {
  return apiRequest<Trade[]>(
    `/api/trades${buildQueryString({
      strategy_run_id: filters.strategyRunId,
      symbol: filters.symbol,
      limit: filters.limit,
    })}`,
  );
}

export function getPositions(filters: PositionFilters = {}) {
  return apiRequest<Position[]>(
    `/api/positions${buildQueryString({
      strategy_run_id: filters.strategyRunId,
      symbol: filters.symbol,
      status: filters.status,
      limit: filters.limit,
    })}`,
  );
}

export function getLogs(filters: LogFilters = {}) {
  return apiRequest<AppLog[]>(
    `/api/logs${buildQueryString({
      scope: filters.scope,
      level: filters.level,
      limit: filters.limit,
    })}`,
  );
}
