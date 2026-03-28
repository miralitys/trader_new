"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import {
  getCandleCoverage,
  getCandles,
  getFeatureCoverage,
  getFeatureRuns,
  getHealth,
  getLogs,
  getResearchSummary,
  runDataValidation,
  getSyncJobs,
  runDataSync,
  runFeatureLayer,
} from "@/lib/api";
import type {
  CandleFilters,
  DataValidationRequest,
  DataSyncRequest,
  FeatureRunFilters,
  FeatureRunRequest,
  LogFilters,
  SyncJobFilters,
} from "@/lib/types";

export const queryKeys = {
  health: ["health"] as const,
  research: ["research", "summary"] as const,
  syncJobs: (filters: SyncJobFilters) => ["sync-jobs", filters] as const,
  candles: (filters: CandleFilters | null) => ["candles", filters] as const,
  candleCoverage: (filters: CandleFilters | null) => ["candles", "coverage", filters] as const,
  featureRuns: (filters: FeatureRunFilters) => ["feature-runs", filters] as const,
  featureCoverage: ["feature-coverage"] as const,
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
  });
}

export function useFeatureCoverage(enabled = true) {
  return useQuery({
    queryKey: queryKeys.featureCoverage,
    queryFn: () => getFeatureCoverage(),
    enabled,
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

export function useRunFeatureLayer() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (payload: FeatureRunRequest) => runFeatureLayer(payload),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: queryKeys.featureRuns({}) }),
        queryClient.invalidateQueries({ queryKey: queryKeys.featureCoverage }),
      ]);
    },
  });
}
