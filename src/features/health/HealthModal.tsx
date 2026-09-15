import { useEffect, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { ZeroCopyShieldIcon } from "@/components/ui/cyber-icons";
import { formatRelativeTime } from "@/lib/formatters";
import type { HealthStatusResponse } from "@/types/crypto";
import { Activity, X, CheckCircle2 } from "lucide-react";

interface HealthModalProps {
  isOpen: boolean;
  onClose: () => void;
}

export function HealthModal({ isOpen, onClose }: HealthModalProps) {
  const [health, setHealth] = useState<HealthStatusResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    if (!isOpen) return;
    setIsLoading(true);

    fetch("/api/health")
      .then(res => res.json())
      .then((data: HealthStatusResponse) => {
        setHealth(data);
        setIsLoading(false);
      })
      .catch(err => {
        console.error("Health fetch error:", err);
        setIsLoading(false);
      });
  }, [isOpen]);

  if (!isOpen) return null;

  const used = health?.creditsUsed ?? 0;
  const limit = health?.creditLimit ?? 15000;
  const percentage = health?.creditPercentage ?? 0;
  const isTripped = health?.isCircuitBreakerTripped ?? false;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80 backdrop-blur-md animate-in fade-in duration-150">
      <div className="bg-[#0b120e] border border-[#00ff88]/30 rounded-2xl w-full max-w-lg shadow-[0_0_30px_rgba(0,0,0,0.8)] p-6 relative">
        {/* Top edge glow */}
        <div className="absolute top-0 left-0 right-0 h-[1px] bg-gradient-to-r from-transparent via-[#00ff88]/40 to-transparent" />

        {/* Close Button */}
        <button
          onClick={onClose}
          className="absolute top-4 right-4 p-2 text-[#7e9c8b] hover:text-[#00ff88] rounded-lg hover:bg-[#0e1713] transition-colors"
          aria-label="Close modal"
        >
          <X className="w-5 h-5" />
        </button>

        {/* Modal Header */}
        <div className="flex items-center gap-3 mb-5">
          <div className="w-10 h-10 rounded-xl bg-[#0e1713] border border-[#00ff88]/30 flex items-center justify-center text-[#00ff88] shadow-[0_0_10px_rgba(0,255,136,0.2)]">
            <Activity className="w-5 h-5" />
          </div>
          <div>
            <h3 className="text-base font-bold text-white font-display flex items-center gap-2">
              <span>SYSTEM TELEMETRY & BUDGET</span>
              <Badge variant="cyan">LIVE</Badge>
            </h3>
            <p className="text-xs text-[#7e9c8b]">
              CoinMarketCap Free Tier (15k monthly credits) & Ingestion Monitor
            </p>
          </div>
        </div>

        {/* Content */}
        {isLoading || !health ? (
          <div className="py-8 text-center text-xs text-[#7e9c8b] font-mono animate-pulse">
            Querying Redis cache telemetry and credit counters...
          </div>
        ) : (
          <div className="space-y-4 text-xs">
            {/* Monthly Budget Card */}
            <div className="p-4 rounded-xl bg-[#080d0a] border border-[#1a2e22]">
              <div className="flex items-center justify-between mb-2">
                <span className="font-semibold text-white font-display">
                  Monthly CMC Credit Budget
                </span>
                <Badge variant={isTripped ? "danger" : percentage > 70 ? "warning" : "success"}>
                  {isTripped ? "Circuit Breaker Active" : `${percentage}% Used`}
                </Badge>
              </div>

              {/* Progress Bar */}
              <div className="w-full bg-[#13201a] h-2.5 rounded-full overflow-hidden mb-2 border border-[#1a2e22]">
                <div
                  className={`h-full transition-all duration-500 rounded-full ${
                    isTripped
                      ? "bg-[#ff3366] shadow-[0_0_8px_rgba(255,51,102,0.5)]"
                      : percentage > 70
                      ? "bg-amber-400"
                      : "bg-[#00ff88] shadow-[0_0_8px_rgba(0,255,136,0.5)]"
                  }`}
                  style={{ width: `${Math.min(100, Math.max(2, percentage))}%` }}
                />
              </div>

              <div className="flex justify-between text-[11px] text-[#7e9c8b] font-mono">
                <span>{used.toLocaleString()} credits used</span>
                <span>Limit: {limit.toLocaleString()} / month</span>
              </div>
              <p className="text-[11px] text-[#7e9c8b] mt-2 leading-relaxed">
                🛡️ Circuit breaker otomatis menghentikan ingest pada 13.500 credits (90%) untuk menjaga akun Anda tetap 100% gratis.
              </p>
            </div>

            {/* Ingestion Endpoints Health */}
            <div className="p-4 rounded-xl bg-[#080d0a] border border-[#1a2e22]">
              <span className="font-semibold text-white font-display block mb-3">
                Ingestion & Sync Endpoints
              </span>

              <div className="space-y-2">
                {/* Global Metrics */}
                <div className="flex items-center justify-between p-2 rounded bg-[#0e1713] border border-[#1a2e22]">
                  <div className="flex items-center gap-2">
                    <CheckCircle2 className="w-4 h-4 text-[#00ff88]" />
                    <div>
                      <span className="font-medium text-[#f1ffef] font-mono text-[11px]">/api/ingest/global</span>
                      <span className="text-[10px] text-[#7e9c8b] block">On-demand sync</span>
                    </div>
                  </div>
                  <span className="text-[11px] font-mono text-[#7e9c8b]">
                    {formatRelativeTime(health.endpoints.global.lastSuccess)}
                  </span>
                </div>

                {/* Listings */}
                <div className="flex items-center justify-between p-2 rounded bg-[#0e1713] border border-[#1a2e22]">
                  <div className="flex items-center gap-2">
                    <CheckCircle2 className="w-4 h-4 text-[#00ff88]" />
                    <div>
                      <span className="font-medium text-[#f1ffef] font-mono text-[11px]">/api/ingest/listings</span>
                      <span className="text-[10px] text-[#7e9c8b] block">On-demand (Top 100)</span>
                    </div>
                  </div>
                  <span className="text-[11px] font-mono text-[#7e9c8b]">
                    {formatRelativeTime(health.endpoints.listings.lastSuccess)}
                  </span>
                </div>

                {/* Fear & Greed */}
                <div className="flex items-center justify-between p-2 rounded bg-[#0e1713] border border-[#1a2e22]">
                  <div className="flex items-center gap-2">
                    <CheckCircle2 className="w-4 h-4 text-[#00ff88]" />
                    <div>
                      <span className="font-medium text-[#f1ffef] font-mono text-[11px]">/api/ingest/feargreed</span>
                      <span className="text-[10px] text-[#7e9c8b] block">On-demand sync</span>
                    </div>
                  </div>
                  <span className="text-[11px] font-mono text-[#7e9c8b]">
                    {formatRelativeTime(health.endpoints.feargreed.lastSuccess)}
                  </span>
                </div>
              </div>
            </div>

            {/* Zero-copy architectural guarantee */}
            <div className="p-3 rounded-lg bg-[#00ff88]/5 border border-[#00ff88]/20 flex items-start gap-2.5">
              <ZeroCopyShieldIcon size={18} className="text-[#00ff88] shrink-0 mt-0.5" />
              <div className="text-[11px] text-[#00ff88] leading-snug">
                <span className="font-semibold block mb-0.5 font-display">Zero-Copy Ingestion Active</span>
                Data dialirkan langsung dari API ke Upstash Redis KV Store tanpa file perantara atau commit Git otomatis.
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
