import type { ApiRequest, ApiResponse, FearGreedData } from "../_lib/types";
import { redis } from "../_lib/redis";
import { fetchCmcWithCircuitBreaker } from "../_lib/cmc";

export default async function handler(req: ApiRequest, res: ApiResponse) {
  const cronSecret = process.env.CRON_SECRET;
  if (cronSecret && req.headers.authorization !== `Bearer ${cronSecret}`) {
    return res.status(401).json({ error: "Unauthorized cron trigger" });
  }

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

    // TTL 35 minutes (2100 seconds)
    await redis.set("market:feargreed:latest", payload, { ex: 2100 });

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
