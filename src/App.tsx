import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { MarketHeader } from "@/features/overview/MarketHeader";
import { StaleBanner } from "@/features/overview/StaleBanner";
import { StatCards } from "@/features/overview/StatCards";
import { FearGreedGauge } from "@/features/overview/FearGreedGauge";
import { MoversCards } from "@/features/overview/MoversCards";
import { CoinTable } from "@/features/listings/CoinTable";
import { CoinDetailSheet } from "@/features/detail/CoinDetailSheet";
import { HealthModal } from "@/features/health/HealthModal";
import { useWatchlist } from "@/lib/watchlist";
import type { OverviewResponse, ListingsResponse } from "@/types/crypto";

export function App() {
  const [selectedCoinId, setSelectedCoinId] = useState<number | null>(null);
  const [isDetailOpen, setIsDetailOpen] = useState(false);
  const [isHealthOpen, setIsHealthOpen] = useState(false);
  const [isSyncingLive, setIsSyncingLive] = useState(false);

  const { watchlist, toggleWatchlist, isWatchlisted } = useWatchlist();

  // 1. Fetch Overview (Global metrics, Fear/Greed, Top movers)
  const overviewQuery = useQuery<OverviewResponse>({
    queryKey: ["dashboard-overview"],
    queryFn: async () => {
      const res = await fetch("/api/dashboard/overview");
      if (!res.ok) throw new Error("Failed to load overview");
      return res.json();
    },
    refetchInterval: 60 * 1000,
  });

  // 2. Fetch Top 100 Listings
  const listingsQuery = useQuery<ListingsResponse>({
    queryKey: ["dashboard-listings"],
    queryFn: async () => {
      const res = await fetch("/api/dashboard/listings");
      if (!res.ok) throw new Error("Failed to load listings");
      return res.json();
    },
    refetchInterval: 60 * 1000,
  });

  const handleSelectCoin = (id: number) => {
    setSelectedCoinId(id);
    setIsDetailOpen(true);
  };

  // Explicit Live CMC Ingestion & Sync
  const handleRefresh = async () => {
    setIsSyncingLive(true);
    try {
      await fetch("/api/dashboard/refresh");
    } catch (err) {
      console.error("Failed to trigger live refresh:", err);
    }
    await Promise.all([overviewQuery.refetch(), listingsQuery.refetch()]);
    setIsSyncingLive(false);
  };

  const isRefreshing = isSyncingLive || overviewQuery.isFetching || listingsQuery.isFetching;
  const lastRefreshedAt =
    overviewQuery.data?.meta.lastRefreshedAt || listingsQuery.data?.meta.lastRefreshedAt;
  const isStale =
    overviewQuery.data?.meta.stale || listingsQuery.data?.meta.stale;
  const circuitBreakerTripped =
    overviewQuery.data?.meta.circuitBreakerTripped || false;

  return (
    <div className="min-h-screen bg-[#050807] text-[#e1ede6] flex flex-col selection:bg-[#00ff88]/30 selection:text-white">
      {/* Top Header */}
      <MarketHeader
        lastRefreshedAt={lastRefreshedAt}
        isStale={isStale}
        isLoading={isRefreshing}
        onRefresh={handleRefresh}
        onOpenHealth={() => setIsHealthOpen(true)}
      />

      {/* Stale / Circuit Breaker Banner */}
      <StaleBanner
        isStale={isStale}
        circuitBreakerTripped={circuitBreakerTripped}
        lastRefreshedAt={lastRefreshedAt}
      />

      {/* Main Container */}
      <main className="flex-1 max-w-7xl w-full mx-auto px-4 sm:px-6 lg:px-8 py-5 space-y-5">
        {/* Section 1: Global Market Stat Cards */}
        <section aria-label="Global Market Stats">
          <StatCards
            data={overviewQuery.data?.global}
            isLoading={overviewQuery.isLoading}
          />
        </section>

        {/* Section 2: Sentiment (Fear & Greed) & Movers (Gainers/Losers) */}
        <section aria-label="Market Sentiment & Movers" className="grid grid-cols-1 lg:grid-cols-3 gap-5">
          <div className="lg:col-span-1">
            <FearGreedGauge
              data={overviewQuery.data?.fearGreed}
              isLoading={overviewQuery.isLoading}
            />
          </div>
          <div className="lg:col-span-2">
            <MoversCards
              gainers={overviewQuery.data?.topGainers}
              losers={overviewQuery.data?.topLosers}
              isLoading={overviewQuery.isLoading}
              onSelectCoin={handleSelectCoin}
            />
          </div>
        </section>

        {/* Section 3: Top 100 Coin Rankings Table */}
        <section aria-label="Top 100 Cryptocurrencies">
          <CoinTable
            coins={listingsQuery.data?.coins || []}
            isLoading={listingsQuery.isLoading}
            watchlist={watchlist}
            onToggleWatchlist={toggleWatchlist}
            onSelectCoin={handleSelectCoin}
          />
        </section>
      </main>

      {/* Footer */}
      <footer className="border-t border-[#1a2e22]/80 bg-[#050807] py-6 mt-12 text-xs text-[#7e9c8b] text-center">
        <div className="max-w-7xl mx-auto px-4 flex flex-col sm:flex-row items-center justify-between gap-3 font-mono text-[11px]">
          <p className="font-mono text-[#f1ffef]">
            crypt<span className="text-[#00ff88]">sight</span>
          </p>
          <div className="flex items-center gap-4">
            <button
              onClick={() => setIsHealthOpen(true)}
              className="text-[#00e5ff] hover:underline transition-colors"
            >
              System Telemetry & Budget
            </button>
            <span className="text-[#1a2e22]">•</span>
            <span>Live Data powered by CoinMarketCap API</span>
          </div>
        </div>
      </footer>

      {/* Coin Detail Drawer */}
      <CoinDetailSheet
        coinId={selectedCoinId}
        isOpen={isDetailOpen}
        onClose={() => setIsDetailOpen(false)}
        isWatchlisted={selectedCoinId != null ? isWatchlisted(selectedCoinId) : false}
        onToggleWatchlist={toggleWatchlist}
      />

      {/* System Health Modal */}
      <HealthModal
        isOpen={isHealthOpen}
        onClose={() => setIsHealthOpen(false)}
      />
    </div>
  );
}
