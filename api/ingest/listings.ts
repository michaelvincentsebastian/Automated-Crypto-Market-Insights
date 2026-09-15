import type { ApiRequest, ApiResponse, CoinItem, MarketMoverCoin } from "../_lib/types.js";
import { redis } from "../_lib/redis.js";
import { fetchCmcWithCircuitBreaker } from "../_lib/cmc.js";

export default async function handler(req: ApiRequest, res: ApiResponse) {
  try {
    const { data, creditCount, isMock } = await fetchCmcWithCircuitBreaker<any[]>(
      "/v1/cryptocurrency/listings/latest",
      "listings",
      { limit: 100 }
    );

    const rawListings = Array.isArray(data) ? data : [];
    const now = Date.now();
    const nowIso = new Date(now).toISOString();
    const sevenDaysAgoSeconds = Math.floor((now - 7 * 24 * 60 * 60 * 1000) / 1000);
    const nowSeconds = Math.floor(now / 1000);

    // Transform into clean CoinItem array
    const coins: CoinItem[] = rawListings.map((c: any) => {
      const quoteUsd = c.quote?.USD || {};
      return {
        id: Number(c.id),
        cmcRank: Number(c.cmc_rank),
        name: String(c.name),
        symbol: String(c.symbol),
        slug: String(c.slug || ""),
        quote: {
          priceUsd: Number(quoteUsd.price ?? 0),
          percentChange1h: Number(quoteUsd.percent_change_1h ?? 0),
          percentChange24h: Number(quoteUsd.percent_change_24h ?? 0),
          percentChange7d: Number(quoteUsd.percent_change_7d ?? 0),
          marketCapUsd: Number(quoteUsd.market_cap ?? 0),
          volume24hUsd: Number(quoteUsd.volume_24h ?? 0),
        },
        circulatingSupply: Number(c.circulating_supply ?? 0),
        totalSupply: c.total_supply != null ? Number(c.total_supply) : null,
        maxSupply: c.max_supply != null ? Number(c.max_supply) : null,
        lastUpdated: c.last_updated || nowIso,
      };
    });

    // 1. Cache Top 100 listings (TTL 7 days = 604800 s until manual sync)
    const listingsPayload = {
      coins,
      lastRefreshedAt: nowIso,
    };
    await redis.set("market:listings:top100", listingsPayload, { ex: 604800 });

    // 2. Compute Top 5 Gainers & Top 5 Losers (in-memory zero copy)
    const validMovers = coins.filter(c => !isNaN(c.quote.percentChange24h));
    
    // Sort descending for gainers
    const sortedGainers = [...validMovers]
      .sort((a, b) => b.quote.percentChange24h - a.quote.percentChange24h)
      .slice(0, 5)
      .map(
        (c): MarketMoverCoin => ({
          id: c.id,
          symbol: c.symbol,
          name: c.name,
          priceUsd: c.quote.priceUsd,
          changePct24h: c.quote.percentChange24h,
        })
      );

    // Sort ascending for losers
    const sortedLosers = [...validMovers]
      .sort((a, b) => a.quote.percentChange24h - b.quote.percentChange24h)
      .slice(0, 5)
      .map(
        (c): MarketMoverCoin => ({
          id: c.id,
          symbol: c.symbol,
          name: c.name,
          priceUsd: c.quote.priceUsd,
          changePct24h: c.quote.percentChange24h,
        })
      );

    await redis.set("market:movers:gainers", { movers: sortedGainers, lastRefreshedAt: nowIso }, { ex: 604800 });
    await redis.set("market:movers:losers", { movers: sortedLosers, lastRefreshedAt: nowIso }, { ex: 604800 });

    // 3. Self-collected historical sparkline rolling window (top 20 coins to conserve storage)
    const topTrackedCoins = coins.slice(0, 20);
    for (const coin of topTrackedCoins) {
      const historyKey = `history:${coin.id}:price`;
      // Append current point: score = unix timestamp seconds, member = `${timestamp}:${price}`
      await redis.zadd(historyKey, {
        score: nowSeconds,
        member: `${nowSeconds}:${coin.quote.priceUsd}`,
      });
      // Trim entries older than 7 days
      await redis.zremrangebyscore(historyKey, "-inf", sevenDaysAgoSeconds);
    }

    return res.status(200).json({
      success: true,
      endpoint: "listings",
      count: coins.length,
      creditCount,
      isMock,
      timestamp: nowIso,
    });
  } catch (error: any) {
    console.error("[ingest/listings] Error:", error);
    return res.status(500).json({
      success: false,
      error: error.message || String(error),
    });
  }
}
