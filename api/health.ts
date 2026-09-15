import type { ApiRequest, ApiResponse, HealthStatusResponse } from "./_lib/types";
import { redis } from "./_lib/redis";
import { getCreditsStatus } from "./_lib/cmc";

export default async function handler(req: ApiRequest, res: ApiResponse) {
  try {
    const credits = await getCreditsStatus();

    const [
      globalSuccess,
      globalError,
      listingsSuccess,
      listingsError,
      feargreedSuccess,
      feargreedError,
    ] = await Promise.all([
      redis.get<string>("meta:ingest:last_success:global"),
      redis.get<string>("meta:ingest:last_error:global"),
      redis.get<string>("meta:ingest:last_success:listings"),
      redis.get<string>("meta:ingest:last_error:listings"),
      redis.get<string>("meta:ingest:last_success:feargreed"),
      redis.get<string>("meta:ingest:last_error:feargreed"),
    ]);

    const lastRefreshedAt =
      listingsSuccess || globalSuccess || feargreedSuccess || null;

    let status: HealthStatusResponse["status"] = "ok";
    if (credits.isCircuitBreakerTripped || globalError || listingsError) {
      status = credits.isCircuitBreakerTripped ? "warning" : "error";
    }

    const response: HealthStatusResponse = {
      status,
      lastRefreshedAt,
      creditsUsed: credits.used,
      creditLimit: credits.limit,
      creditPercentage: credits.percentage,
      isCircuitBreakerTripped: credits.isCircuitBreakerTripped,
      endpoints: {
        global: { lastSuccess: globalSuccess, lastError: globalError },
        listings: { lastSuccess: listingsSuccess, lastError: listingsError },
        feargreed: { lastSuccess: feargreedSuccess, lastError: feargreedError },
      },
    };

    return res.status(200).json(response);
  } catch (error: any) {
    console.error("[api/health] Error:", error);
    return res.status(500).json({ status: "error", error: error.message || String(error) });
  }
}
