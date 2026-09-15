import type { ApiRequest, ApiResponse, GlobalMarketData } from "../_lib/types";
import { redis } from "../_lib/redis";
import { fetchCmcWithCircuitBreaker } from "../_lib/cmc";

export default async function handler(req: ApiRequest, res: ApiResponse) {
  try {
    const { data, creditCount, isMock } = await fetchCmcWithCircuitBreaker<any>(
      "/v1/global-metrics/quotes/latest",
      "global"
    );

    const quoteUsd = data.quote?.USD || data;
    const globalData: GlobalMarketData = {
      totalMarketCapUsd: quoteUsd.total_market_cap ?? 0,
      totalVolume24hUsd: quoteUsd.total_volume_24h ?? 0,
      btcDominancePct: data.btc_dominance ?? 0,
      ethDominancePct: data.eth_dominance ?? 0,
      activeCryptocurrencies: data.active_cryptocurrencies ?? 0,
    };

    const payload = {
      data: globalData,
      lastRefreshedAt: new Date().toISOString(),
    };

    // TTL 7 days (604800 seconds) until manual sync
    await redis.set("market:global:latest", payload, { ex: 604800 });

    return res.status(200).json({
      success: true,
      endpoint: "global",
      creditCount,
      isMock,
      timestamp: payload.lastRefreshedAt,
    });
  } catch (error: any) {
    console.error("[ingest/global] Error:", error);
    return res.status(500).json({
      success: false,
      error: error.message || String(error),
    });
  }
}
