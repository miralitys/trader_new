"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import {
  getBacktests,
  getCandleCoverage,
  getCandles,
  getDataValidationRuns,
  getFeatureCoverage,
  getFeatureRuns,
  getHealth,
  getLogs,
  getPatternScans,
  getResearchSummary,
  getStrategies,
  getStrategyRuns,
  runDataValidation,
  runBacktest,
  startDataValidationRun,
  startPatternScan,
  startStrategyPaperRun,
  stopStrategyPaperRun,
  getSyncJobs,
  runDataSync,
  runFeatureLayer,
} from "@/lib/api";
import type {
  BacktestListItem,
  BacktestRunRequest,
  CandleFilters,
  DataValidationRequest,
  DataSyncRequest,
  FeatureRunFilters,
  FeatureCoverageFilters,
  FeatureRunRequest,
  LogFilters,
  PatternScanRequest,
  PatternScanRun,
  StrategyPaperStartRequest,
  StrategyPaperStopRequest,
  StrategyRunSummary,
  StrategySummary,
  SyncJobFilters,
  ValidationRun,
} from "@/lib/types";

export const queryKeys = {
  health: ["health"] as const,
  research: ["research", "summary"] as const,
  syncJobs: (filters: SyncJobFilters) => ["sync-jobs", filters] as const,
  candles: (filters: CandleFilters | null) => ["candles", filters] as const,
  candleCoverage: (filters: CandleFilters | null) => ["candles", "coverage", filters] as const,
  featureRuns: (filters: FeatureRunFilters) => ["feature-runs", filters] as const,
  featureCoverage: (filters: FeatureCoverageFilters) => ["feature-coverage", filters] as const,
  dataValidationRuns: (limit: number) => ["data-validation-runs", limit] as const,
  patternScans: (limit: number) => ["pattern-scans", limit] as const,
  strategies: ["strategies"] as const,
  strategyRuns: (filters: { strategyCode?: string; status?: string; mode?: string; limit?: number }) => ["strategy-runs", filters] as const,
  backtests: (filters: { strategyCode?: string; status?: string; limit?: number }) => ["backtests", filters] as const,
  logs: (filters: LogFilters) => ["logs", filters] as const,
};

export function useHealth() {
  return useQuery({
    queryKey: queryKeys.health,
    queryFn: getHealth,
  });
}

export function useResearchSummary() {
  return useQuery({
    queryKey: queryKeys.research,
    queryFn: getResearchSummary,
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

export function useCandleCoverage(filters: CandleFilters | null, enabled = true) {
  return useQuery({
    queryKey: queryKeys.candleCoverage(filters),
    queryFn: () => getCandleCoverage(filters as CandleFilters),
    enabled: Boolean(filters) && enabled,
  });
}

export function useLogs(filters: LogFilters = {}, enabled = true) {
  return useQuery({
    queryKey: queryKeys.logs(filters),
    queryFn: () => getLogs(filters),
    enabled,
  });
}

export function useFeatureRuns(filters: FeatureRunFilters = {}, enabled = true) {
  return useQuery({
    queryKey: queryKeys.featureRuns(filters),
    queryFn: () => getFeatureRuns(filters),
    enabled,
    refetchInterval: (query) => {
      const runs = (query.state.data as { status: string }[] | undefined) ?? [];
      if (runs.some((run) => run.status === "queued" || run.status === "running")) {
        return 5000;
      }
      return false;
    },
  });
}

export function useFeatureCoverage(filters: FeatureCoverageFilters = {}, enabled = true) {
  return useQuery({
    queryKey: queryKeys.featureCoverage(filters),
    queryFn: () => getFeatureCoverage(filters),
    enabled,
    refetchInterval: 5000,
  });
}

export function useDataValidationRuns(limit = 20, enabled = true) {
  return useQuery({
    queryKey: queryKeys.dataValidationRuns(limit),
    queryFn: () => getDataValidationRuns(limit),
    enabled,
    refetchInterval: (query) => {
      const runs = (query.state.data as ValidationRun[] | undefined) ?? [];
      const latest = runs[0];
      if (latest && (latest.status === "queued" || latest.status === "running")) {
        return 5000;
      }
      return false;
    },
  });
}

export function usePatternScans(limit = 20, enabled = true) {
  return useQuery({
    queryKey: queryKeys.patternScans(limit),
    queryFn: () => getPatternScans(limit),
    enabled,
    refetchInterval: (query) => {
      const runs = (query.state.data as PatternScanRun[] | undefined) ?? [];
      const latest = runs[0];
      if (latest && (latest.status === "queued" || latest.status === "running")) {
        return 5000;
      }
      return false;
    },
  });
}

export function useStrategies(enabled = true) {
  return useQuery({
    queryKey: queryKeys.strategies,
    queryFn: getStrategies,
    enabled,
    refetchInterval: 10000,
  });
}

export function useStrategyRuns(
  filters: { strategyCode?: string; status?: string; mode?: string; limit?: number } = {},
  enabled = true,
) {
  return useQuery({
    queryKey: queryKeys.strategyRuns(filters),
    queryFn: () => getStrategyRuns(filters),
    enabled,
    refetchInterval: (query) => {
      const runs = (query.state.data as StrategyRunSummary[] | undefined) ?? [];
      if (runs.some((run) => run.status === "running" || run.status === "created")) {
        return 5000;
      }
      return 10000;
    },
  });
}

export function useBacktests(filters: { strategyCode?: string; status?: string; limit?: number } = {}, enabled = true) {
  return useQuery({
    queryKey: queryKeys.backtests(filters),
    queryFn: () => getBacktests(filters),
    enabled,
    refetchInterval: (query) => {
      const runs = (query.state.data as BacktestListItem[] | undefined) ?? [];
      if (runs.some((run) => run.status === "queued" || run.status === "running")) {
        return 5000;
      }
      return 10000;
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
        queryClient.invalidateQueries({ queryKey: queryKeys.research }),
        queryClient.invalidateQueries({ queryKey: ["candles"] }),
        queryClient.invalidateQueries({ queryKey: ["candles", "coverage"] }),
      ]);
    },
  });
}

export function useRunDataValidation() {
  return useMutation({
    mutationFn: (payload: DataValidationRequest) => runDataValidation(payload),
  });
}

export function useStartDataValidationRun() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (payload: DataValidationRequest) => startDataValidationRun(payload),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ["data-validation-runs"] });
    },
  });
}

export function useRunFeatureLayer() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (payload: FeatureRunRequest) => runFeatureLayer(payload),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: queryKeys.featureRuns({}) }),
        queryClient.invalidateQueries({ queryKey: ["feature-coverage"] }),
      ]);
    },
  });
}

export function useStartPatternScan() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (payload: PatternScanRequest) => startPatternScan(payload),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: queryKeys.patternScans(20) }),
        queryClient.invalidateQueries({ queryKey: queryKeys.research }),
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
        queryClient.invalidateQueries({ queryKey: queryKeys.research }),
      ]);
    },
  });
}

export function useStartStrategyPaperRun() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ strategyCode, payload }: { strategyCode: string; payload: StrategyPaperStartRequest }) =>
      startStrategyPaperRun(strategyCode, payload),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: queryKeys.strategies }),
        queryClient.invalidateQueries({ queryKey: ["strategy-runs"] }),
      ]);
    },
  });
}

export function useStopStrategyPaperRun() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ strategyCode, payload }: { strategyCode: string; payload: StrategyPaperStopRequest }) =>
      stopStrategyPaperRun(strategyCode, payload),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: queryKeys.strategies }),
        queryClient.invalidateQueries({ queryKey: ["strategy-runs"] }),
      ]);
    },
  });
}
