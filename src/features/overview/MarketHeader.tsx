import { RefreshCw, Activity, Clock } from "lucide-react";
import { Button } from "@/components/ui/button";
import { CryptSightLogo } from "@/components/ui/cyber-icons";
import { formatRelativeTime } from "@/lib/formatters";

interface MarketHeaderProps {
  lastRefreshedAt?: string;
  isStale?: boolean;
  isLoading?: boolean;
  onRefresh: () => void;
  onOpenHealth: () => void;
}

export function MarketHeader({
  lastRefreshedAt,
  isStale,
  isLoading,
  onRefresh,
  onOpenHealth,
}: MarketHeaderProps) {
  return (
    <header className="border-b border-[#1a2e22]/80 bg-[#050807]/90 backdrop-blur-xl sticky top-0 z-40">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-3 flex flex-col md:flex-row md:items-center md:justify-between gap-3">
        {/* Left Branding */}
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl bg-[#0b120e] border border-[#00ff88]/30 flex items-center justify-center shadow-[0_0_15px_rgba(0,255,136,0.25)] text-[#00ff88]">
            <CryptSightLogo size={24} />
          </div>
          <div className="flex items-center">
            <h1 className="text-xl font-bold tracking-tight font-display text-white select-none">
              crypt<span className="text-[#00ff88]">sight</span>
            </h1>
          </div>
        </div>

        {/* Right Telemetry & Actions */}
        <div className="flex items-center gap-2.5 flex-wrap">
          {/* Last Updated Telemetry */}
          <div className="flex items-center gap-1.5 text-xs text-[#7e9c8b] bg-[#0b120e] border border-[#1a2e22] px-3 py-1.5 rounded">
            <Clock className="w-3.5 h-3.5 text-[#00ff88]" />
            <span className="font-mono text-[11px]">SYNCED:</span>
            <span className="text-[#f1ffef] font-mono font-medium tabular-nums">
              {formatRelativeTime(lastRefreshedAt)}
            </span>
            {isStale && (
              <span className="w-1.5 h-1.5 rounded-full bg-amber-400 ml-1" title="Data is cached" />
            )}
          </div>

          {/* System Health */}
          <Button
            variant="secondary"
            size="sm"
            onClick={onOpenHealth}
            className="text-xs gap-1.5"
            title="View system health & monthly CMC credits budget"
          >
            <Activity className="w-3.5 h-3.5 text-[#00e5ff]" />
            <span>Telemetry</span>
          </Button>

          {/* Live Sync Refresh */}
          <Button
            variant="primary"
            size="sm"
            onClick={onRefresh}
            disabled={isLoading}
            className="text-xs gap-1.5"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${isLoading ? "animate-spin" : ""}`} />
            <span>{isLoading ? "Syncing..." : "Sync Live CMC"}</span>
          </Button>
        </div>
      </div>
    </header>
  );
}
