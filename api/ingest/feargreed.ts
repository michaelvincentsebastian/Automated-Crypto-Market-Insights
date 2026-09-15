import type { ApiRequest, ApiResponse, FearGreedData } from "../_lib/types.js";
import { redis } from "../_lib/redis.js";
import { fetchCmcWithCircuitBreaker } from "../_lib/cmc.js";

export default async function handler(req: ApiRequest, res: ApiResponse) {
  try {
    const { data, creditCount, isMock } = await fetchCmcWithCircuitBreaker<any>(
      "/v3/fear-and-greed/latest",
      "feargreed"
    );

    // Normalize classification
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

    // TTL 7 days (604800 seconds) until manual sync
    await redis.set("market:feargreed:latest", payload, { ex: 604800 });

    return res.status(200).json({
      success: true,
      endpoint: "feargreed",
      creditCount,
      isMock,
      timestamp: payload.lastRefreshedAt,
    });
  } catch (error: any) {
    console.error("[ingest/feargreed] Error:", error);
    return res.status(500).json({
      success: false,
      error: error.message || String(error),
    });
  }
}
