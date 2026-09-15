import type { ApiRequest, ApiResponse } from "../_lib/types.js";
import { ingestGlobalData, ingestFearGreedData, ingestListingsData } from "../_lib/ingest-helper.js";

export default async function handler(req: ApiRequest, res: ApiResponse) {
  try {
    const [globalRes, fearGreedRes, listingsRes] = await Promise.all([
      ingestGlobalData().catch(err => {
        console.error("Failed to refresh global data:", err);
        return null;
      }),
      ingestFearGreedData().catch(err => {
        console.error("Failed to refresh fear greed data:", err);
        return null;
      }),
      ingestListingsData().catch(err => {
        console.error("Failed to refresh listings data:", err);
        return null;
      }),
    ]);

    return res.status(200).json({
      success: true,
      message: "Data refreshed from live CoinMarketCap API and cached to Upstash Redis",
      timestamp: new Date().toISOString(),
      listingsCount: listingsRes?.coins?.length ?? 0,
      hasGlobal: !!globalRes,
      hasFearGreed: !!fearGreedRes,
    });
  } catch (error: any) {
    console.error("[dashboard/refresh] Error:", error);
    return res.status(500).json({ success: false, error: error.message || "Failed to refresh" });
  }
}
