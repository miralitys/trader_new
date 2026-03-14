export type NumericValue = number | string;

export type HealthResponse = {
  status: string;
  app_name: string;
  environment: string;
  version: string;
  timestamp: string;
  services: Record<string, string>;
};

export type StrategySummary = {
  code: string;
  name: string;
  description: string;
  spot_only: boolean;
  long_only: boolean;
  has_saved_config: boolean;
  active_paper_run_id: number | null;
  active_paper_status: string | null;
};

export type StrategyDetail = StrategySummary & {
  default_config: Record<string, unknown>;
  effective_config: Record<string, unknown>;
  config_schema: Record<string, unknown>;
  config_source: string;
};

export type StrategyConfigResponse = {
  strategy_code: string;
  source: string;
  config: Record<string, unknown>;
  default_config: Record<string, unknown>;
  config_schema: Record<string, unknown>;
  updated_at: string | null;
};

export type StrategyConfigUpdateRequest = {
  config: Record<string, unknown>;
};

export type StrategyPaperStartRequest = {
  symbols: string[];
  timeframes: string[];
  exchange_code: string;
  initial_balance: NumericValue;
  currency: string;
  fee: NumericValue;
  slippage: NumericValue;
  strategy_config_override: Record<string, unknown>;
  metadata: Record<string, unknown>;
};

export type StrategyPaperStopRequest = {
  reason: string;
};

export type PaperRunResponse = {
  run_id: number;
  strategy_code: string;
  status: string;
  symbols: string[];
  timeframes: string[];
  exchange_code: string;
  account_balance: NumericValue;
  currency: string;
  last_processed_candle_at: string | null;
  processed_candles: number;
  signals_created: number;
  orders_created: number;
  trades_created: number;
  error_text: string | null;
};

export type StrategyRunSummary = {
  id: number;
  strategy_code: string;
  strategy_name: string;
  mode: string;
  status: string;
  symbols: string[];
  timeframes: string[];
  started_at: string | null;
  stopped_at: string | null;
  last_processed_candle_at: string | null;
  created_at: string;
  metadata: Record<string, unknown>;
  account_balance: NumericValue | null;
  currency: string | null;
  open_positions_count: number;
};

export type StrategyRunDetail = StrategyRunSummary;

export type BacktestListItem = {
  id: number;
  strategy_code: string;
  strategy_name: string;
  status: string;
  symbol: string;
  timeframe: string;
  started_at: string | null;
  completed_at: string | null;
  initial_capital: NumericValue;
  final_equity: NumericValue;
  total_return_pct: NumericValue;
  max_drawdown_pct: NumericValue;
  win_rate_pct: NumericValue;
  total_trades: number;
  error_text: string | null;
};

export type BacktestMetrics = {
  total_return_pct: NumericValue;
  max_drawdown_pct: NumericValue;
  win_rate_pct: NumericValue;
  profit_factor: NumericValue;
  expectancy: NumericValue;
  avg_winner: NumericValue;
  avg_loser: NumericValue;
  total_trades: number;
};

export type BacktestTrade = {
  side: string;
  entry_time: string;
  exit_time: string;
  entry_price: NumericValue;
  exit_price: NumericValue;
  qty: NumericValue;
  gross_pnl: NumericValue;
  pnl: NumericValue;
  pnl_pct: NumericValue;
  fees: NumericValue;
  slippage: NumericValue;
  exit_reason: string;
};

export type EquityPoint = {
  timestamp: string;
  equity: NumericValue;
  cash: NumericValue;
  close_price: NumericValue;
  position_qty: NumericValue;
};

export type BacktestResponse = {
  run_id: number | null;
  strategy_code: string;
  symbol: string;
  timeframe: string;
  exchange_code: string;
  status: string;
  initial_capital: NumericValue;
  final_equity: NumericValue;
  started_at: string;
  completed_at: string | null;
  params: Record<string, unknown>;
  metrics: BacktestMetrics;
  equity_curve: EquityPoint[];
  trades: BacktestTrade[];
  error_text: string | null;
};

export type BacktestRunRequest = {
  strategy_code: string;
  symbol: string;
  timeframe: string;
  start_at: string;
  end_at: string;
  exchange_code: string;
  initial_capital: NumericValue;
  fee: NumericValue;
  slippage: NumericValue;
  position_size_pct: NumericValue;
  strategy_config_override: Record<string, unknown>;
};

export type DataSyncRequest = {
  mode: "initial" | "incremental" | "manual";
  symbol: string;
  timeframe: string;
  start_at?: string;
  end_at?: string;
};

export type DataSyncResponse = {
  job_id: number;
  exchange: string;
  symbol: string;
  timeframe: string;
  start_at: string;
  end_at: string;
  fetched_rows: number;
  normalized_rows: number;
  inserted_rows: number;
  status: string;
};

export type SyncJob = {
  id: number;
  exchange: string;
  symbol: string;
  timeframe: string;
  start_at: string | null;
  end_at: string | null;
  status: string;
  rows_inserted: number;
  error_text: string | null;
  created_at: string;
  updated_at: string;
};

export type Candle = {
  id: number;
  exchange_code: string;
  symbol: string;
  timeframe: string;
  open_time: string;
  open: NumericValue;
  high: NumericValue;
  low: NumericValue;
  close: NumericValue;
  volume: NumericValue;
  created_at: string;
};

export type Signal = {
  id: number;
  strategy_run_id: number;
  strategy_code: string | null;
  symbol: string;
  timeframe: string;
  signal_type: string;
  signal_strength: NumericValue;
  payload: Record<string, unknown>;
  candle_time: string;
  created_at: string;
};

export type Trade = {
  id: number;
  strategy_run_id: number;
  strategy_code: string | null;
  symbol: string;
  entry_price: NumericValue;
  exit_price: NumericValue;
  qty: NumericValue;
  pnl: NumericValue;
  pnl_pct: NumericValue;
  fees: NumericValue;
  slippage: NumericValue;
  opened_at: string;
  closed_at: string;
  metadata: Record<string, unknown>;
};

export type Position = {
  id: number;
  strategy_run_id: number;
  strategy_code: string | null;
  symbol: string;
  side: string;
  qty: NumericValue;
  avg_entry_price: NumericValue;
  stop_price: NumericValue | null;
  take_profit_price: NumericValue | null;
  status: string;
  opened_at: string;
  closed_at: string | null;
};

export type AppLog = {
  id: number;
  scope: string;
  level: string;
  message: string;
  payload: Record<string, unknown>;
  created_at: string;
};

export type DashboardRunStatus = {
  active_paper_runs: number;
  stopped_paper_runs: number;
  failed_paper_runs: number;
  recent_backtests: number;
};

export type DashboardPerformanceSnapshot = {
  backtest_run_id: number;
  strategy_code: string;
  symbol: string;
  timeframe: string;
  total_return_pct: NumericValue;
  win_rate_pct: NumericValue;
  max_drawdown_pct: NumericValue;
  total_trades: number;
};

export type DashboardDataSyncStatus = {
  latest_job: SyncJob | null;
  recent_jobs: SyncJob[];
};

export type DashboardSummary = {
  strategies: StrategySummary[];
  run_status: DashboardRunStatus;
  key_performance_metrics: DashboardPerformanceSnapshot[];
  open_positions_count: number;
  recent_trades: Trade[];
  recent_backtests: BacktestListItem[];
  data_sync_status: DashboardDataSyncStatus;
};

export type StrategyRunFilters = {
  strategyCode?: string;
  status?: string;
  mode?: string;
  limit?: number;
};

export type BacktestFilters = {
  strategyCode?: string;
  status?: string;
  limit?: number;
};

export type SyncJobFilters = {
  status?: string;
  symbol?: string;
  timeframe?: string;
  limit?: number;
};

export type CandleFilters = {
  symbol: string;
  timeframe: string;
  startAt: string;
  endAt: string;
  exchangeCode?: string;
  limit?: number;
};

export type SignalFilters = {
  strategyRunId?: number;
  symbol?: string;
  timeframe?: string;
  limit?: number;
};

export type TradeFilters = {
  strategyRunId?: number;
  symbol?: string;
  limit?: number;
};

export type PositionFilters = {
  strategyRunId?: number;
  symbol?: string;
  status?: string;
  limit?: number;
};

export type LogFilters = {
  scope?: string;
  level?: string;
  limit?: number;
};

export type ApiErrorPayload = {
  detail: string;
};
