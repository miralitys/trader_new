"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import {
  getBacktest,
  getBacktests,
  getCandles,
  getDashboardSummary,
  getHealth,
  getLogs,
  getPositions,
  getSignals,
  getStrategy,
  getStrategyConfig,
  getStrategyRun,
  getStrategyRuns,
  getStrategies,
  getSyncJobs,
  getTrades,
  runBacktest,
  runDataSync,
  startStrategyPaper,
  stopStrategyPaper,
  updateStrategyConfig,
} from "@/lib/api";
import type {
  BacktestFilters,
  BacktestRunRequest,
  CandleFilters,
  DataSyncRequest,
  LogFilters,
  PositionFilters,
  SignalFilters,
  StrategyConfigUpdateRequest,
  StrategyPaperStartRequest,
  StrategyPaperStopRequest,
  StrategyRunFilters,
  SyncJobFilters,
  TradeFilters,
} from "@/lib/types";

export const queryKeys = {
  health: ["health"] as const,
  dashboard: ["dashboard", "summary"] as const,
  strategies: ["strategies"] as const,
  strategy: (code: string) => ["strategies", code] as const,
  strategyConfig: (code: string) => ["strategies", code, "config"] as const,
  strategyRuns: (filters: StrategyRunFilters) => ["strategy-runs", filters] as const,
  strategyRun: (id: number) => ["strategy-runs", id] as const,
  backtests: (filters: BacktestFilters) => ["backtests", filters] as const,
  backtest: (id: number) => ["backtests", id] as const,
  syncJobs: (filters: SyncJobFilters) => ["sync-jobs", filters] as const,
  candles: (filters: CandleFilters | null) => ["candles", filters] as const,
  signals: (filters: SignalFilters) => ["signals", filters] as const,
  trades: (filters: TradeFilters) => ["trades", filters] as const,
  positions: (filters: PositionFilters) => ["positions", filters] as const,
  logs: (filters: LogFilters) => ["logs", filters] as const,
};

export function useHealth() {
  return useQuery({
    queryKey: queryKeys.health,
    queryFn: getHealth,
  });
}

export function useDashboardSummary() {
  return useQuery({
    queryKey: queryKeys.dashboard,
    queryFn: getDashboardSummary,
  });
}

export function useStrategies() {
  return useQuery({
    queryKey: queryKeys.strategies,
    queryFn: getStrategies,
  });
}

export function useStrategy(code: string) {
  return useQuery({
    queryKey: queryKeys.strategy(code),
    queryFn: () => getStrategy(code),
    enabled: Boolean(code),
  });
}

export function useStrategyConfig(code: string) {
  return useQuery({
    queryKey: queryKeys.strategyConfig(code),
    queryFn: () => getStrategyConfig(code),
    enabled: Boolean(code),
  });
}

export function useStrategyRuns(filters: StrategyRunFilters = {}) {
  return useQuery({
    queryKey: queryKeys.strategyRuns(filters),
    queryFn: () => getStrategyRuns(filters),
  });
}

export function useStrategyRun(id: number) {
  return useQuery({
    queryKey: queryKeys.strategyRun(id),
    queryFn: () => getStrategyRun(id),
    enabled: Number.isFinite(id),
  });
}

export function useBacktests(filters: BacktestFilters = {}) {
  return useQuery({
    queryKey: queryKeys.backtests(filters),
    queryFn: () => getBacktests(filters),
  });
}

export function useBacktest(id: number) {
  return useQuery({
    queryKey: queryKeys.backtest(id),
    queryFn: () => getBacktest(id),
    enabled: Number.isFinite(id),
  });
}

export function useSyncJobs(filters: SyncJobFilters = {}) {
  return useQuery({
    queryKey: queryKeys.syncJobs(filters),
    queryFn: () => getSyncJobs(filters),
  });
}

export function useCandles(filters: CandleFilters | null, enabled = true) {
  return useQuery({
    queryKey: queryKeys.candles(filters),
    queryFn: () => getCandles(filters as CandleFilters),
    enabled: Boolean(filters) && enabled,
  });
}

export function useSignals(filters: SignalFilters = {}, enabled = true) {
  return useQuery({
    queryKey: queryKeys.signals(filters),
    queryFn: () => getSignals(filters),
    enabled,
  });
}

export function useTrades(filters: TradeFilters = {}, enabled = true) {
  return useQuery({
    queryKey: queryKeys.trades(filters),
    queryFn: () => getTrades(filters),
    enabled,
  });
}

export function usePositions(filters: PositionFilters = {}, enabled = true) {
  return useQuery({
    queryKey: queryKeys.positions(filters),
    queryFn: () => getPositions(filters),
    enabled,
  });
}

export function useLogs(filters: LogFilters = {}, enabled = true) {
  return useQuery({
    queryKey: queryKeys.logs(filters),
    queryFn: () => getLogs(filters),
    enabled,
  });
}

export function useUpdateStrategyConfig(code: string) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (payload: StrategyConfigUpdateRequest) => updateStrategyConfig(code, payload),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: queryKeys.strategy(code) }),
        queryClient.invalidateQueries({ queryKey: queryKeys.strategyConfig(code) }),
        queryClient.invalidateQueries({ queryKey: queryKeys.strategies }),
        queryClient.invalidateQueries({ queryKey: queryKeys.dashboard }),
      ]);
    },
  });
}

export function useStartStrategyPaper(code: string) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (payload: StrategyPaperStartRequest) => startStrategyPaper(code, payload),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: queryKeys.strategy(code) }),
        queryClient.invalidateQueries({ queryKey: queryKeys.strategyConfig(code) }),
        queryClient.invalidateQueries({ queryKey: ["strategy-runs"] }),
        queryClient.invalidateQueries({ queryKey: queryKeys.dashboard }),
        queryClient.invalidateQueries({ queryKey: ["positions"] }),
        queryClient.invalidateQueries({ queryKey: ["signals"] }),
        queryClient.invalidateQueries({ queryKey: ["trades"] }),
      ]);
    },
  });
}

export function useStopStrategyPaper(code: string) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (payload: StrategyPaperStopRequest) => stopStrategyPaper(code, payload),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: queryKeys.strategy(code) }),
        queryClient.invalidateQueries({ queryKey: ["strategy-runs"] }),
        queryClient.invalidateQueries({ queryKey: queryKeys.dashboard }),
        queryClient.invalidateQueries({ queryKey: ["positions"] }),
        queryClient.invalidateQueries({ queryKey: ["signals"] }),
        queryClient.invalidateQueries({ queryKey: ["trades"] }),
      ]);
    },
  });
}

export function useRunBacktest() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (payload: BacktestRunRequest) => runBacktest(payload),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["backtests"] }),
        queryClient.invalidateQueries({ queryKey: queryKeys.dashboard }),
      ]);
    },
  });
}

export function useRunDataSync() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (payload: DataSyncRequest) => runDataSync(payload),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["sync-jobs"] }),
        queryClient.invalidateQueries({ queryKey: queryKeys.dashboard }),
        queryClient.invalidateQueries({ queryKey: ["candles"] }),
      ]);
    },
  });
}
