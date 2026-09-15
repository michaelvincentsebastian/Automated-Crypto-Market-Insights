import { redis } from "./redis";

const CMC_BASE_URL = "https://pro-api.coinmarketcap.com";
const MONTHLY_BUDGET_LIMIT = 15000;
const CIRCUIT_BREAKER_THRESHOLD = 13500; // 90% of 15,000

export function getCurrentMonthKey(): string {
  const d = new Date();
  const year = d.getUTCFullYear();
  const month = String(d.getUTCMonth() + 1).padStart(2, "0");
  return `meta:credits:used:${year}-${month}`;
}

export async function getCreditsStatus() {
  const key = getCurrentMonthKey();
  const used = Number((await redis.get<number>(key)) || 0);
  return {
    used,
    limit: MONTHLY_BUDGET_LIMIT,
    isCircuitBreakerTripped: used >= CIRCUIT_BREAKER_THRESHOLD,
    percentage: Math.min(100, Math.round((used / MONTHLY_BUDGET_LIMIT) * 100)),
  };
}

export async function fetchCmcWithCircuitBreaker<T = any>(
  endpointPath: string,
  endpointLabel: string,
  params: Record<string, string | number> = {}
): Promise<{ data: T; creditCount: number; isMock?: boolean }> {
  const apiKey = process.env.CMC_API_KEY || process.env.CMC_PRO_API_KEY;

  // 1. Circuit breaker check
  const budget = await getCreditsStatus();
  if (budget.isCircuitBreakerTripped) {
    const errorMsg = `Circuit breaker active: Monthly CMC credit limit reached (${budget.used}/${budget.limit}). Ingestion halted.`;
    await redis.set(`meta:ingest:last_error:${endpointLabel}`, errorMsg, { ex: 172800 });
    throw new Error(errorMsg);
  }

  // 2. If no API key is provided, return mock seed data for development
  if (!apiKey || apiKey.trim() === "") {
    return {
      data: getMockCmcData(endpointPath, params) as T,
      creditCount: 0,
      isMock: true,
    };
  }

  // 3. Make real HTTP request to CoinMarketCap
  const url = new URL(`${CMC_BASE_URL}${endpointPath}`);
  Object.entries(params).forEach(([k, v]) => url.searchParams.append(k, String(v)));

  try {
    const response = await fetch(url.toString(), {
      headers: {
        "X-CMC_PRO_API_KEY": apiKey,
        "Accept": "application/json",
      },
    });

    if (!response.ok) {
      const errText = await response.text();
      const errMsg = `CMC API Error (${response.status}): ${errText}`;
      await redis.set(`meta:ingest:last_error:${endpointLabel}`, errMsg, { ex: 172800 });
      throw new Error(errMsg);
    }

    const json = await response.json();
    const creditCount = Number(json?.status?.credit_count ?? 1);

    // Increment monthly credit counter
    await redis.incrby(getCurrentMonthKey(), creditCount);

    // Record last success
    const nowIso = new Date().toISOString();
    await redis.set(`meta:ingest:last_success:${endpointLabel}`, nowIso, { ex: 172800 });

    return {
      data: json.data as T,
      creditCount,
      isMock: false,
    };
  } catch (err: any) {
    await redis.set(`meta:ingest:last_error:${endpointLabel}`, err.message || String(err), { ex: 172800 });
    throw err;
  }
}

// Fallback seed data for development without API key
function getMockCmcData(endpoint: string, params: Record<string, any> = {}): any {
  if (endpoint.includes("global-metrics/quotes/latest")) {
    return {
      total_market_cap: 2430000000000,
      total_volume_24h: 78500000000,
      btc_dominance: 56.4,
      eth_dominance: 14.2,
      active_cryptocurrencies: 10420,
    };
  }

  if (endpoint.includes("fear-and-greed/latest")) {
    return {
      value: 64,
      value_classification: "Greed",
      update_time: new Date().toISOString(),
    };
  }

  if (endpoint.includes("cryptocurrency/listings/latest")) {
    return generateMockListings(100);
  }

  if (endpoint.includes("cryptocurrency/info")) {
    const id = params.id || 1;
    return {
      [id]: {
        id: Number(id),
        name: "Bitcoin",
        symbol: "BTC",
        logo: "https://s2.coinmarketcap.com/static/img/coins/64x64/1.png",
        description: "Bitcoin is a decentralized cryptocurrency originally described in a 2008 whitepaper by a person, or group of people, using the alias Satoshi Nakamoto. It is the world's first and largest digital asset.",
        category: "coin",
        urls: {
          website: ["https://bitcoin.org/"],
          technical_doc: ["https://bitcoin.org/bitcoin.pdf"],
          explorer: ["https://blockchain.info/"],
          source_code: ["https://github.com/bitcoin/bitcoin"],
        },
      },
    };
  }

  return {};
}

function generateMockListings(count: number) {
  const baseCoins = [
    { id: 1, name: "Bitcoin", symbol: "BTC", slug: "bitcoin", price: 68420.5, cap: 1350000000000, vol: 32000000000, ch1h: 0.12, ch24h: 2.45, ch7d: 5.18, circ: 19750000, max: 21000000 },
    { id: 1027, name: "Ethereum", symbol: "ETH", slug: "ethereum", price: 2640.8, cap: 317000000000, vol: 15400000000, ch1h: -0.25, ch24h: 3.12, ch7d: 8.41, circ: 120200000, max: null },
    { id: 825, name: "Tether USDt", symbol: "USDT", slug: "tether", price: 1.0, cap: 119000000000, vol: 48000000000, ch1h: 0.01, ch24h: 0.02, ch7d: -0.01, circ: 119000000000, max: null },
    { id: 1839, name: "BNB", symbol: "BNB", slug: "bnb", price: 585.3, cap: 85400000000, vol: 1100000000, ch1h: 0.45, ch24h: 1.15, ch7d: 2.89, circ: 145800000, max: 200000000 },
    { id: 5426, name: "Solana", symbol: "SOL", slug: "solana", price: 154.2, cap: 72100000000, vol: 3800000000, ch1h: 1.12, ch24h: 6.84, ch7d: 14.25, circ: 468000000, max: null },
    { id: 3408, name: "USDC", symbol: "USDC", slug: "usd-coin", price: 1.0, cap: 35000000000, vol: 5400000000, ch1h: -0.01, ch24h: 0.0, ch7d: 0.02, circ: 35000000000, max: null },
    { id: 52, name: "XRP", symbol: "XRP", slug: "xrp", price: 0.54, cap: 30500000000, vol: 1150000000, ch1h: -0.32, ch24h: -1.45, ch7d: 1.25, circ: 56500000000, max: 100000000000 },
    { id: 74, name: "Dogecoin", symbol: "DOGE", slug: "dogecoin", price: 0.125, cap: 18200000000, vol: 950000000, ch1h: 0.85, ch24h: 4.62, ch7d: 12.8, circ: 146000000000, max: null },
    { id: 2010, name: "Cardano", symbol: "ADA", slug: "cardano", price: 0.36, cap: 12900000000, vol: 280000000, ch1h: -0.15, ch24h: -0.85, ch7d: 3.14, circ: 35700000000, max: 45000000000 },
    { id: 1958, name: "TRON", symbol: "TRX", slug: "tron", price: 0.158, cap: 13700000000, vol: 380000000, ch1h: 0.08, ch24h: 1.04, ch7d: 4.5, circ: 86800000000, max: null },
    { id: 5805, name: "Avalanche", symbol: "AVAX", slug: "avalanche", price: 28.4, cap: 11500000000, vol: 410000000, ch1h: 1.45, ch24h: 8.92, ch7d: 18.5, circ: 405000000, max: 720000000 },
    { id: 6636, name: "Polkadot", symbol: "DOT", slug: "polkadot-new", price: 4.35, cap: 6200000000, vol: 175000000, ch1h: -0.42, ch24h: -2.85, ch7d: -4.12, circ: 1430000000, max: null },
    { id: 1975, name: "Chainlink", symbol: "LINK", slug: "chainlink", price: 11.85, cap: 7200000000, vol: 245000000, ch1h: 0.65, ch24h: 3.42, ch7d: 9.15, circ: 608000000, max: 1000000000 },
    { id: 3890, name: "Polygon", symbol: "MATIC", slug: "polygon", price: 0.38, cap: 3800000000, vol: 120000000, ch1h: -0.82, ch24h: -4.15, ch7d: -7.8, circ: 10000000000, max: 10000000000 },
    { id: 7083, name: "Uniswap", symbol: "UNI", slug: "uniswap", price: 7.82, cap: 4700000000, vol: 160000000, ch1h: 0.22, ch24h: 5.12, ch7d: 11.4, circ: 600000000, max: 1000000000 },
  ];

  const results = [];
  for (let i = 0; i < count; i++) {
    const base = baseCoins[i % baseCoins.length];
    const rank = i + 1;
    const priceMult = 1 - (i * 0.008);
    results.push({
      id: i < baseCoins.length ? base.id : 10000 + i,
      name: i < baseCoins.length ? base.name : `Crypto Asset ${rank}`,
      symbol: i < baseCoins.length ? base.symbol : `COIN${rank}`,
      slug: i < baseCoins.length ? base.slug : `coin-${rank}`,
      cmc_rank: rank,
      circulating_supply: base.circ * priceMult,
      total_supply: base.circ * priceMult * 1.1,
      max_supply: base.max,
      last_updated: new Date().toISOString(),
      quote: {
        USD: {
          price: Math.max(0.001, base.price * priceMult),
          volume_24h: Math.max(100000, base.vol * priceMult),
          percent_change_1h: base.ch1h + ((i % 5) - 2) * 0.2,
          percent_change_24h: base.ch24h + ((i % 7) - 3) * 0.8,
          percent_change_7d: base.ch7d + ((i % 11) - 5) * 1.2,
          market_cap: Math.max(5000000, base.cap * priceMult),
        },
      },
    });
  }
  return results;
}
