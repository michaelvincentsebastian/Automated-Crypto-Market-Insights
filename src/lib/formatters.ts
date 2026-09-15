export function formatUsd(amount: number, compact = false): string {
  if (amount == null || isNaN(amount)) return "$0.00";

  if (compact) {
    if (Math.abs(amount) >= 1e12) {
      return `$${(amount / 1e12).toFixed(2)}T`;
    }
    if (Math.abs(amount) >= 1e9) {
      return `$${(amount / 1e9).toFixed(2)}B`;
    }
    if (Math.abs(amount) >= 1e6) {
      return `$${(amount / 1e6).toFixed(2)}M`;
    }
    if (Math.abs(amount) >= 1e3) {
      return `$${(amount / 1e3).toFixed(2)}K`;
    }
  }

  if (Math.abs(amount) >= 1000) {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(amount);
  }

  if (Math.abs(amount) >= 1) {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      minimumFractionDigits: 2,
      maximumFractionDigits: 4,
    }).format(amount);
  }

  // Small coin prices (e.g. $0.00345)
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 4,
    maximumFractionDigits: 6,
  }).format(amount);
}

export function formatCompactNumber(num: number | null | undefined, suffix = ""): string {
  if (num == null || isNaN(num)) return "N/A";
  if (num >= 1e12) return `${(num / 1e12).toFixed(2)}T ${suffix}`.trim();
  if (num >= 1e9) return `${(num / 1e9).toFixed(2)}B ${suffix}`.trim();
  if (num >= 1e6) return `${(num / 1e6).toFixed(2)}M ${suffix}`.trim();
  if (num >= 1e3) return `${(num / 1e3).toFixed(2)}K ${suffix}`.trim();
  return `${num.toLocaleString("en-US")} ${suffix}`.trim();
}

export function formatPercent(value: number | null | undefined): string {
  if (value == null || isNaN(value)) return "0.00%";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
}

export function formatRelativeTime(dateString: string | null | undefined): string {
  if (!dateString) return "Never";
  const date = new Date(dateString);
  const now = new Date();
  const diffSec = Math.floor((now.getTime() - date.getTime()) / 1000);

  if (diffSec < 45) return "Just now";
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`;
  if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}h ago`;
  return `${Math.floor(diffSec / 86400)}d ago`;
}
