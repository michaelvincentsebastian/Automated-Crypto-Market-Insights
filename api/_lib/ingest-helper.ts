import { redis } from "./redis";
import { fetchCmcWithCircuitBreaker } from "./cmc";
import type { GlobalMarketData, FearGreedData, CoinItem, MarketMoverCoin } from "./types";

export async function ingestGlobalData() {
  const { data } = await fetchCmcWithCircuitBreaker<any>(
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

  await redis.set("market:global:latest", payload, { ex: 2100 });
  return payload;
}

export async function ingestFearGreedData() {
  const { data } = await fetchCmcWithCircuitBreaker<any>(
    "/v3/fear-and-greed/latest",
    "feargreed"
  );

  let classification: FearGreedData["classification"] = "Neutral";
  const rawClass = String(data?.value_classification || "").toLowerCase();
  if (rawClass.includes("extreme fear")) classification = "Extreme Fear";
  else if (rawClass.includes("fear")) classification = "Fear";
  else if (rawClass.includes("extreme greed")) classification = "Extreme Greed";
  else if (rawClass.includes("greed")) classification = "Greed";

  const fearGreedData: FearGreedData = {
    value: Number(data?.value ?? 50),
    classification,
    updatedAt: data?.update_time || new Date().toISOString(),
  };

  const payload = {
    data: fearGreedData,
    lastRefreshedAt: new Date().toISOString(),
  };

  await redis.set("market:feargreed:latest", payload, { ex: 2100 });
  return payload;
}

export async function ingestListingsData() {
  const { data } = await fetchCmcWithCircuitBreaker<any[]>(
    "/v1/cryptocurrency/listings/latest",
    "listings",
    { limit: 100 }
  );

  const rawListings = Array.isArray(data) ? data : [];
  const now = Date.now();
  const nowIso = new Date(now).toISOString();
  const sevenDaysAgoSeconds = Math.floor((now - 7 * 24 * 60 * 60 * 1000) / 1000);
  const nowSeconds = Math.floor(now / 1000);

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

  const listingsPayload = {
    coins,
    lastRefreshedAt: nowIso,
  };
  await redis.set("market:listings:top100", listingsPayload, { ex: 900 });

  // Gainers & Losers
  const validMovers = coins.filter(c => !isNaN(c.quote.percentChange24h));
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

  await redis.set("market:movers:gainers", { movers: sortedGainers, lastRefreshedAt: nowIso }, { ex: 900 });
  await redis.set("market:movers:losers", { movers: sortedLosers, lastRefreshedAt: nowIso }, { ex: 900 });

  // Sparkline history
  const topTrackedCoins = coins.slice(0, 20);
  for (const coin of topTrackedCoins) {
    const historyKey = `history:${coin.id}:price`;
    await redis.zadd(historyKey, {
      score: nowSeconds,
      member: `${nowSeconds}:${coin.quote.priceUsd}`,
    });
    await redis.zremrangebyscore(historyKey, "-inf", sevenDaysAgoSeconds);
  }

  return { coins, sortedGainers, sortedLosers, lastRefreshedAt: nowIso };
}
