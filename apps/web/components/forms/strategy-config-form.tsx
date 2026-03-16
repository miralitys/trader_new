"use client";

import { FormEvent, useEffect, useState } from "react";

import { useUpdateStrategyConfig } from "@/lib/query-hooks";
import { getErrorMessage, parseJsonInput, prettyJson } from "@/lib/utils";

type StrategyConfigFormProps = {
  strategyCode: string;
  initialConfig: Record<string, unknown>;
};

export function StrategyConfigForm({ strategyCode, initialConfig }: StrategyConfigFormProps) {
  const updateConfig = useUpdateStrategyConfig(strategyCode);
  const [configText, setConfigText] = useState(prettyJson(initialConfig));
  const [message, setMessage] = useState<string | null>(null);

  useEffect(() => {
    setConfigText(prettyJson(initialConfig));
  }, [initialConfig]);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setMessage(null);

    try {
      await updateConfig.mutateAsync({
        config: parseJsonInput(configText, {}),
      });
      setMessage("Strategy config saved.");
    } catch (error) {
      setMessage(getErrorMessage(error, "Unable to save config."));
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-5">
      <label className="grid gap-2">
        <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Config JSON</span>
        <textarea
          value={configText}
          onChange={(event) => setConfigText(event.target.value)}
          rows={18}
          className="min-h-[360px] rounded-2xl border border-white/10 bg-slate-950/70 px-3 py-3 font-mono text-sm text-white outline-none transition focus:border-sky-400/40"
        />
      </label>

      <div className="flex flex-col gap-3 border-t border-white/6 pt-4 md:flex-row md:items-center md:justify-between">
        <p className="text-sm text-slate-400">Save validated strategy config to reuse it later in paper trading and backtests.</p>
        <div className="flex flex-col items-start gap-3 sm:flex-row sm:items-center">
          {message ? <span className="text-sm text-slate-300">{message}</span> : null}
          <button
            type="submit"
            disabled={updateConfig.isPending}
            className="rounded-xl bg-sky-400 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-sky-300 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400"
          >
            {updateConfig.isPending ? "Saving..." : "Save config"}
          </button>
        </div>
      </div>
    </form>
  );
}
