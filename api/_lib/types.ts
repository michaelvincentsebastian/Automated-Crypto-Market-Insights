export interface GlobalMarketData {
  totalMarketCapUsd: number;
  totalVolume24hUsd: number;
  btcDominancePct: number;
  ethDominancePct: number;
  activeCryptocurrencies: number;
}

export interface FearGreedData {
  value: number;
  classification: "Extreme Fear" | "Fear" | "Neutral" | "Greed" | "Extreme Greed";
  updatedAt: string;
}

export interface MarketMoverCoin {
  id: number;
  symbol: string;
  name: string;
  priceUsd: number;
  changePct24h: number;
}

export interface CoinQuote {
  priceUsd: number;
  percentChange1h: number;
  percentChange24h: number;
  percentChange7d: number;
  marketCapUsd: number;
  volume24hUsd: number;
}

export interface CoinItem {
  id: number;
  cmcRank: number;
  name: string;
  symbol: string;
  slug: string;
  quote: CoinQuote;
  circulatingSupply: number;
  totalSupply: number | null;
  maxSupply: number | null;
  lastUpdated: string;
}

export interface CoinDetailInfo {
  id: number;
  name: string;
  symbol: string;
  logo: string;
  description: string;
  category?: string;
  urls: {
    website?: string[];
    technicalDoc?: string[];
    sourceCode?: string[];
    explorer?: string[];
  };
}

export interface PriceHistoryPoint {
  timestamp: number;
  price: number;
}

export interface ApiMeta {
  lastRefreshedAt: string;
  stale: boolean;
  circuitBreakerTripped?: boolean;
  source?: "cache" | "fallback" | "live";
}

export interface OverviewResponse {
  global: GlobalMarketData;
  fearGreed: FearGreedData;
  topGainers: MarketMoverCoin[];
  topLosers: MarketMoverCoin[];
  meta: ApiMeta;
}

export interface ListingsResponse {
  coins: CoinItem[];
  meta: ApiMeta;
}

export interface CoinDetailResponse {
  coin: CoinItem | null;
  info: CoinDetailInfo | null;
  history: PriceHistoryPoint[];
  meta: ApiMeta;
}

export interface HealthStatusResponse {
  status: "ok" | "warning" | "error";
  lastRefreshedAt: string | null;
  creditsUsed: number;
  creditLimit: number;
  creditPercentage: number;
  isCircuitBreakerTripped: boolean;
  endpoints: {
    global: { lastSuccess: string | null; lastError: string | null };
    listings: { lastSuccess: string | null; lastError: string | null };
    feargreed: { lastSuccess: string | null; lastError: string | null };
  };
}

export interface ApiRequest {
  method?: string;
  query: Record<string, any>;
  headers: Record<string, any>;
  body?: any;
}

export interface ApiResponse {
  status: (code: number) => ApiResponse;
  json: (data: any) => void;
  setHeader?: (name: string, value: string) => void;
  end?: (data?: any) => void;
}

