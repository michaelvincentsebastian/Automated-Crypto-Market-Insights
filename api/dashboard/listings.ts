import type { ApiRequest, ApiResponse, ListingsResponse, CoinItem } from "../_lib/types.js";
import { redis } from "../_lib/redis.js";
import { ingestListingsData } from "../_lib/ingest-helper.js";

export default async function handler(req: ApiRequest, res: ApiResponse) {
  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const shouldRefresh = req.query.refresh === "true";

  try {
    let cached = await redis.get<{ coins: CoinItem[]; lastRefreshedAt: string }>("market:listings:top100");

    const hasApiKey = !!(process.env.CMC_API_KEY || process.env.CMC_PRO_API_KEY);
    if (hasApiKey && (shouldRefresh || !cached || !cached.coins || cached.coins.length === 0)) {
      try {
        const ingested = await ingestListingsData();
        cached = {
          coins: ingested.coins,
          lastRefreshedAt: ingested.lastRefreshedAt,
        };
      } catch (err) {
        console.warn("[dashboard/listings] Live listings fetch failed:", err);
      }
    }

    let coins: CoinItem[] = cached?.coins || [];
    let lastRefreshedAt = cached?.lastRefreshedAt || new Date().toISOString();
    const ageMs = Date.now() - new Date(lastRefreshedAt).getTime();
    const stale = ageMs > 15 * 60 * 1000;

    const response: ListingsResponse = {
      coins,
      meta: {
        lastRefreshedAt,
        stale,
        source: cached ? "cache" : "fallback",
      },
    };

    return res.status(200).json(response);
  } catch (error: any) {
    console.error("[dashboard/listings] Error:", error);
    return res.status(500).json({ error: "Failed to load listings" });
  }
}
