import { Timestamp } from "../../gen/google/protobuf/timestamp";

export function formatNumber(value: number | undefined | null): string {
  if (value === undefined || value === null || Number.isNaN(value)) {
    return "—";
  }
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: 0,
  }).format(value);
}

export function formatPercent(value: number | undefined | null): string {
  if (value === undefined || value === null || Number.isNaN(value)) {
    return "—";
  }
  return `${value.toFixed(1)}%`;
}

export function formatRelativeTime(timestamp?: Date | Timestamp | null): string {
  if (!timestamp) {
    return "Unknown";
  }
  const date = timestamp instanceof Date ? timestamp : timestampToDate(timestamp);
  const diffMs = Date.now() - date.getTime();
  if (!Number.isFinite(diffMs)) {
    return "Unknown";
  }
  if (diffMs < 60 * 1000) {
    return "Just now";
  }
  if (diffMs < 60 * 60 * 1000) {
    const mins = Math.round(diffMs / 60000);
    return `${mins} min${mins === 1 ? "" : "s"} ago`;
  }
  if (diffMs < 24 * 60 * 60 * 1000) {
    const hours = Math.round(diffMs / 3600000);
    return `${hours} hour${hours === 1 ? "" : "s"} ago`;
  }
  return date.toLocaleString();
}

export function formatThroughput(value: number | undefined | null): string {
  if (!value || Number.isNaN(value)) {
    return "—";
  }
  return `${value.toFixed(2)} pkt/s`;
}

function timestampToDate(ts: Timestamp): Date {
  return new Date(ts.seconds * 1000 + ts.nanos / 1_000_000);
}
