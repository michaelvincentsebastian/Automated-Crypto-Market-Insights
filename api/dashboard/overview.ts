import type { ApiRequest, ApiResponse, OverviewResponse, GlobalMarketData, FearGreedData, MarketMoverCoin } from "../_lib/types";
import { redis } from "../_lib/redis";
import { ingestGlobalData, ingestFearGreedData } from "../_lib/ingest-helper";

export default async function handler(req: ApiRequest, res: ApiResponse) {
  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const shouldRefresh = req.query.refresh === "true";

  try {
    let globalCached = await redis.get<{ data: GlobalMarketData; lastRefreshedAt: string }>("market:global:latest");
    let fearGreedCached = await redis.get<{ data: FearGreedData; lastRefreshedAt: string }>("market:feargreed:latest");
    let gainersCached = await redis.get<{ movers: MarketMoverCoin[]; lastRefreshedAt: string }>("market:movers:gainers");
    let losersCached = await redis.get<{ movers: MarketMoverCoin[]; lastRefreshedAt: string }>("market:movers:losers");

    // If cache is missing or refresh requested, fetch live from CMC API
    const hasApiKey = !!(process.env.CMC_API_KEY || process.env.CMC_PRO_API_KEY);
    if (hasApiKey && (shouldRefresh || !globalCached || !fearGreedCached)) {
      try {
        if (shouldRefresh || !globalCached) {
          globalCached = await ingestGlobalData();
        }
        if (shouldRefresh || !fearGreedCached) {
          fearGreedCached = await ingestFearGreedData();
        }
        // Also re-read movers if listings were refreshed
        gainersCached = await redis.get<{ movers: MarketMoverCoin[]; lastRefreshedAt: string }>("market:movers:gainers");
        losersCached = await redis.get<{ movers: MarketMoverCoin[]; lastRefreshedAt: string }>("market:movers:losers");
      } catch (err) {
        console.warn("[dashboard/overview] Live fetch failed, falling back to cache:", err);
      }
    }

    // Fallback values only if live fetch was impossible
    const global: GlobalMarketData = globalCached?.data || {
      totalMarketCapUsd: 2650000000000,
      totalVolume24hUsd: 78500000000,
      btcDominancePct: 58.9,
      ethDominancePct: 14.8,
      activeCryptocurrencies: 10420,
    };

    const fearGreed: FearGreedData = fearGreedCached?.data || {
      value: 68,
      classification: "Greed",
      updatedAt: new Date().toISOString(),
    };

    const topGainers: MarketMoverCoin[] = gainersCached?.movers || [];
    const topLosers: MarketMoverCoin[] = losersCached?.movers || [];

    const latestRefreshedAt =
      globalCached?.lastRefreshedAt ||
      fearGreedCached?.lastRefreshedAt ||
      new Date().toISOString();

    const ageMs = Date.now() - new Date(latestRefreshedAt).getTime();
    const stale = ageMs > 35 * 60 * 1000;

    const responseData: OverviewResponse = {
      global,
      fearGreed,
      topGainers,
      topLosers,
      meta: {
        lastRefreshedAt: latestRefreshedAt,
        stale,
        source: globalCached ? "cache" : "fallback",
      },
    };

    return res.status(200).json(responseData);
  } catch (error: any) {
    console.error("[dashboard/overview] Error:", error);
    return res.status(500).json({ error: "Failed to load overview data" });
  }
}
