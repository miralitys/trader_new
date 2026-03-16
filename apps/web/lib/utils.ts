import clsx, { type ClassValue } from "clsx";

import type { NumericValue } from "@/lib/types";

export function cn(...inputs: ClassValue[]) {
  return clsx(inputs);
}

export function toNumber(value: NumericValue | null | undefined): number {
  if (typeof value === "number") {
    return value;
  }

  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  return 0;
}

export function formatNumber(value: NumericValue | null | undefined, digits = 2) {
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  }).format(toNumber(value));
}

export function formatInteger(value: NumericValue | null | undefined) {
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: 0,
  }).format(toNumber(value));
}

export function formatCurrency(value: NumericValue | null | undefined, currency = "USD") {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency,
    maximumFractionDigits: 2,
  }).format(toNumber(value));
}

export function formatPercent(value: NumericValue | null | undefined, digits = 2) {
  return `${formatNumber(value, digits)}%`;
}

export function formatDateTime(value: string | null | undefined) {
  if (!value) {
    return "N/A";
  }

  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value));
}

export function formatDate(value: string | null | undefined) {
  if (!value) {
    return "N/A";
  }

  return new Intl.DateTimeFormat("en-US", {
    year: "numeric",
    month: "short",
    day: "2-digit",
  }).format(new Date(value));
}

export function formatStatusLabel(value: string | null | undefined) {
  if (!value) {
    return "N/A";
  }

  return value
    .split("_")
    .filter(Boolean)
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
    .join(" ");
}

export function prettyJson(value: unknown) {
  return JSON.stringify(value, null, 2);
}

export function parseJsonInput<T>(value: string, fallback: T): T {
  if (!value.trim()) {
    return fallback;
  }

  return JSON.parse(value) as T;
}

export function toDatetimeLocalInput(value: string | Date) {
  const date = typeof value === "string" ? new Date(value) : value;
  return new Date(date.getTime() - date.getTimezoneOffset() * 60_000).toISOString().slice(0, 16);
}

export function compactList(values: unknown): string[] {
  if (Array.isArray(values)) {
    return values
      .map((value) => String(value).trim())
      .filter(Boolean);
  }

  if (typeof values === "string") {
    return values
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean);
  }

  return [];
}

export function formatApiErrorDetail(detail: unknown): string | null {
  if (typeof detail === "string") {
    const normalized = detail.trim();
    return normalized || null;
  }

  if (Array.isArray(detail)) {
    const messages = detail
      .map((item) => formatApiErrorDetail(item))
      .filter((item): item is string => Boolean(item));
    return messages.length ? messages.join(" | ") : null;
  }

  if (detail && typeof detail === "object") {
    const payload = detail as Record<string, unknown>;

    if (typeof payload.msg === "string") {
      const location = Array.isArray(payload.loc)
        ? payload.loc
            .filter((item): item is string | number => typeof item === "string" || typeof item === "number")
            .map(String)
            .filter((item) => item !== "body" && item !== "query")
            .join(".")
        : "";
      return location ? `${location}: ${payload.msg}` : payload.msg;
    }

    if ("detail" in payload) {
      return formatApiErrorDetail(payload.detail);
    }

    try {
      return JSON.stringify(detail);
    } catch {
      return null;
    }
  }

  return null;
}

export function getErrorMessage(error: unknown, fallback = "Something went wrong.") {
  if (error instanceof Error) {
    const normalized = error.message.trim();
    if (normalized && !normalized.includes("[object Object]")) {
      return normalized;
    }
  }

  return formatApiErrorDetail(error) ?? fallback;
}
