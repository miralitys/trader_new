import type {
  ApiErrorPayload,
  AppLog,
  BacktestListItem,
  BacktestResponse,
  BacktestRunRequest,
  Candle,
  CandleCoverage,
  CandleFilters,
  FeatureCoverage,
  FeatureCoverageFilters,
  FeatureRun,
  FeatureRunFilters,
  FeatureRunRequest,
  DataValidationReport,
  DataValidationRequest,
  ValidationRun,
  DataSyncRequest,
  DataSyncResponse,
  HealthResponse,
  LogFilters,
  PatternScanRequest,
  PatternScanRun,
  PaperRunResponse,
  ResearchSummary,
  StrategyPaperStartRequest,
  StrategyPaperStopRequest,
  StrategyRunSummary,
  StrategySummary,
  SyncJob,
  SyncJobFilters,
} from "@/lib/types";
import { formatApiErrorDetail } from "@/lib/utils";

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

    const message = formatApiErrorDetail(payload?.detail ?? payload) ?? `API request failed: ${response.status}`;
    throw new Error(message);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  return (await response.json()) as T;
}

export function getHealth() {
  return apiRequest<HealthResponse>("/api/health");
}

export function getResearchSummary() {
  return apiRequest<ResearchSummary>("/api/research/summary");
}

export function startPatternScan(payload: PatternScanRequest) {
  return apiRequest<PatternScanRun>("/api/patterns/scan/start", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function getPatternScans(limit = 20) {
  return apiRequest<PatternScanRun[]>(
    `/api/patterns/scans${buildQueryString({
      limit,
    })}`,
  );
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
      exchange_code: filters.exchangeCode ?? "binance_us",
      limit: filters.limit,
    })}`,
  );
}

export function getCandleCoverage(filters: CandleFilters) {
  return apiRequest<CandleCoverage>(
    `/api/candles/coverage${buildQueryString({
      symbol: filters.symbol,
      timeframe: filters.timeframe,
      start_at: filters.startAt,
      end_at: filters.endAt,
      exchange_code: filters.exchangeCode ?? "binance_us",
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

export function runDataValidation(payload: DataValidationRequest) {
  return apiRequest<DataValidationReport>("/api/data/validation-report", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function startDataValidationRun(payload: DataValidationRequest) {
  return apiRequest<ValidationRun>("/api/data/validation-report/start", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function getDataValidationRuns(limit = 20) {
  return apiRequest<ValidationRun[]>(
    `/api/data/validation-report/runs${buildQueryString({
      limit,
    })}`,
  );
}

export function runFeatureLayer(payload: FeatureRunRequest) {
  return apiRequest<FeatureRun>("/api/features/run", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function getFeatureRuns(filters: FeatureRunFilters = {}) {
  return apiRequest<FeatureRun[]>(
    `/api/features/runs${buildQueryString({
      symbol: filters.symbol,
      timeframe: filters.timeframe,
      limit: filters.limit,
    })}`,
  );
}

export function getFeatureCoverage(filters: FeatureCoverageFilters = {}) {
  return apiRequest<FeatureCoverage[]>(
    `/api/features/coverage${buildQueryString({
      exchange_code: filters.exchangeCode ?? "binance_us",
      symbol: filters.symbol,
    })}`,
  );
}

export function getStrategies() {
  return apiRequest<StrategySummary[]>("/api/strategies");
}

export function getStrategyRuns(filters: { strategyCode?: string; status?: string; mode?: string; limit?: number } = {}) {
  return apiRequest<StrategyRunSummary[]>(
    `/api/strategies/runs/list${buildQueryString({
      strategy_code: filters.strategyCode,
      status: filters.status,
      mode: filters.mode,
      limit: filters.limit,
    })}`,
  );
}

export function startStrategyPaperRun(strategyCode: string, payload: StrategyPaperStartRequest) {
  return apiRequest<PaperRunResponse>(`/api/strategies/${strategyCode}/paper/start`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function stopStrategyPaperRun(strategyCode: string, payload: StrategyPaperStopRequest) {
  return apiRequest<PaperRunResponse>(`/api/strategies/${strategyCode}/paper/stop`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function getBacktests(filters: { strategyCode?: string; status?: string; limit?: number } = {}) {
  return apiRequest<BacktestListItem[]>(
    `/api/backtests${buildQueryString({
      strategy_code: filters.strategyCode,
      status: filters.status,
      limit: filters.limit,
    })}`,
  );
}

export function runBacktest(payload: BacktestRunRequest) {
  return apiRequest<BacktestResponse>("/api/backtests/run", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}
