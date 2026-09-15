import { useEffect, useState } from "react";
import { Sheet } from "@/components/ui/sheet";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { InfoTooltip } from "@/components/ui/tooltip";
import { NeonUpIcon, NeonDownIcon } from "@/components/ui/cyber-icons";
import { formatUsd, formatPercent, formatCompactNumber } from "@/lib/formatters";
import { GLOSSARY } from "@/lib/glossary";
import type { CoinDetailResponse } from "@/types/crypto";
import { Star, ExternalLink, Globe, FileText, Code, TrendingUp } from "lucide-react";
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip as RechartsTooltip,
} from "recharts";

interface CoinDetailSheetProps {
  coinId: number | null;
  isOpen: boolean;
  onClose: () => void;
  isWatchlisted: boolean;
  onToggleWatchlist: (id: number) => void;
}

export function CoinDetailSheet({
  coinId,
  isOpen,
  onClose,
  isWatchlisted,
  onToggleWatchlist,
}: CoinDetailSheetProps) {
  const [data, setData] = useState<CoinDetailResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    if (!isOpen || !coinId) return;

    let isMounted = true;
    setIsLoading(true);

    fetch(`/api/dashboard/coin?id=${coinId}`)
      .then(res => res.json())
      .then((resData: CoinDetailResponse) => {
        if (isMounted) {
          setData(resData);
          setIsLoading(false);
        }
      })
      .catch(err => {
        console.error("Error fetching coin detail:", err);
        if (isMounted) setIsLoading(false);
      });

    return () => {
      isMounted = false;
    };
  }, [coinId, isOpen]);

  const coin = data?.coin;
  const info = data?.info;
  const history = data?.history || [];
  const isPositive24h = (coin?.quote.percentChange24h ?? 0) >= 0;

  const prices = history.map(h => h.price);
  const minPrice = prices.length ? Math.min(...prices) * 0.995 : 0;
  const maxPrice = prices.length ? Math.max(...prices) * 1.005 : 1;

  const fdv =
    coin && coin.maxSupply
      ? coin.quote.priceUsd * coin.maxSupply
      : null;

  const supplyPercentage =
    coin && coin.maxSupply && coin.maxSupply > 0
      ? Math.min(100, (coin.circulatingSupply / coin.maxSupply) * 100)
      : null;

  return (
    <Sheet isOpen={isOpen} onClose={onClose}>
      {isLoading || !coin ? (
        <div className="space-y-6 animate-pulse">
          <div className="flex items-center gap-4">
            <Skeleton className="w-12 h-12 rounded-full bg-[#0e1713]" />
            <div className="space-y-2">
              <Skeleton className="w-32 h-6 bg-[#0e1713]" />
              <Skeleton className="w-20 h-4 bg-[#0e1713]" />
            </div>
          </div>
          <Skeleton className="w-48 h-10 bg-[#0e1713]" />
          <Skeleton className="w-full h-44 rounded-xl bg-[#0e1713]" />
        </div>
      ) : (
        <div className="space-y-5 text-slate-200">
          {/* Header */}
          <div className="flex items-start justify-between">
            <div className="flex items-center gap-3.5">
              <img
                src={info?.logo || `https://s2.coinmarketcap.com/static/img/coins/64x64/${coin.id}.png`}
                alt={coin.name}
                className="w-11 h-11 rounded-full bg-[#080d0a] p-0.5 border border-[#00ff88]/30 shadow-[0_0_12px_rgba(0,255,136,0.2)]"
                onError={e => {
                  (e.target as HTMLElement).style.display = "none";
                }}
              />
              <div>
                <div className="flex items-center gap-2">
                  <h2 className="text-lg font-bold text-white font-display">{coin.name}</h2>
                  <span className="font-mono text-xs uppercase px-2 py-0.5 rounded bg-[#0e1713] text-[#00ff88] border border-[#00ff88]/20 font-semibold">
                    {coin.symbol}
                  </span>
                </div>
                <div className="flex items-center gap-2 mt-1">
                  <Badge variant="secondary" className="font-mono text-[10px]">
                    RANK #{coin.cmcRank}
                  </Badge>
                  {info?.category && (
                    <span className="text-[11px] text-[#7e9c8b] capitalize font-mono">
                      {info.category}
                    </span>
                  )}
                </div>
              </div>
            </div>

            {/* Watchlist Pin Action */}
            <Button
              variant={isWatchlisted ? "primary" : "secondary"}
              size="sm"
              onClick={() => onToggleWatchlist(coin.id)}
              className="text-xs gap-1.5 shrink-0"
            >
              <Star className={`w-3.5 h-3.5 ${isWatchlisted ? "fill-[#050807]" : "text-amber-400"}`} />
              <span>{isWatchlisted ? "PINNED" : "PIN"}</span>
            </Button>
          </div>

          {/* Price Banner */}
          <div className="bg-[#0e1713] border border-[#1a2e22] p-4 rounded-xl relative overflow-hidden">
            <div className="absolute top-0 right-0 p-3 opacity-10 font-mono text-3xl font-bold text-[#00ff88]">
              {coin.symbol}
            </div>
            <div className="text-[10px] font-mono text-[#7e9c8b] uppercase tracking-wider mb-1">
              CURRENT LIVE PRICE
            </div>
            <div className="flex items-baseline gap-3">
              <span className="text-3xl font-extrabold text-white font-mono tracking-tight tabular-nums drop-shadow-[0_0_12px_rgba(0,255,136,0.15)]">
                {formatUsd(coin.quote.priceUsd)}
              </span>
              <span
                className={`inline-flex items-center text-xs font-mono font-bold px-2 py-0.5 rounded tabular-nums ${
                  isPositive24h
                    ? "text-[#00ff88] bg-[#00ff88]/10 border border-[#00ff88]/20 shadow-[0_0_8px_rgba(0,255,136,0.15)]"
                    : "text-[#ff3366] bg-[#ff3366]/10 border border-[#ff3366]/20 shadow-[0_0_8px_rgba(255,51,102,0.15)]"
                }`}
              >
                {isPositive24h ? (
                  <NeonUpIcon size={12} className="mr-0.5" />
                ) : (
                  <NeonDownIcon size={12} className="mr-0.5" />
                )}
                {formatPercent(coin.quote.percentChange24h)} (24h)
              </span>
            </div>
          </div>

          {/* Sparkline / Price History */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-semibold text-[#7e9c8b] font-display flex items-center gap-1.5 uppercase tracking-wider">
                <TrendingUp className="w-3.5 h-3.5 text-[#00ff88]" />
                TELEMETRY SPARKLINE TREND
              </span>
              <span className="text-[10px] font-mono text-[#00ff88] bg-[#00ff88]/10 px-1.5 py-0.5 rounded border border-[#00ff88]/20">
                ZERO-COPY CACHE
              </span>
            </div>

            <div className="h-40 w-full bg-[#080d0a] border border-[#1a2e22] rounded-xl p-2 pt-3 shadow-inner">
              {history.length > 1 ? (
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={history}>
                    <defs>
                      <linearGradient id="emeraldSparkGradient" x1="0" y1="0" x2="0" y2="1">
                        <stop
                          offset="5%"
                          stopColor={isPositive24h ? "#00ff88" : "#ff3366"}
                          stopOpacity={0.4}
                        />
                        <stop
                          offset="95%"
                          stopColor={isPositive24h ? "#00ff88" : "#ff3366"}
                          stopOpacity={0.0}
                        />
                      </linearGradient>
                    </defs>
                    <XAxis dataKey="timestamp" hide />
                    <YAxis domain={[minPrice, maxPrice]} hide />
                    <RechartsTooltip
                      content={({ active, payload }) => {
                        if (active && payload && payload.length) {
                          const item = payload[0].payload;
                          return (
                            <div className="bg-[#050807] border border-[#00ff88]/30 p-2 rounded shadow-xl text-xs font-mono">
                              <p className="text-[#7e9c8b] text-[10px]">
                                {new Date(item.timestamp).toLocaleTimeString([], {
                                  hour: "2-digit",
                                  minute: "2-digit",
                                })}
                              </p>
                              <p className="text-[#00ff88] font-bold tabular-nums">
                                {formatUsd(item.price)}
                              </p>
                            </div>
                          );
                        }
                        return null;
                      }}
                    />
                    <Area
                      type="monotone"
                      dataKey="price"
                      stroke={isPositive24h ? "#00ff88" : "#ff3366"}
                      strokeWidth={2}
                      fillOpacity={1}
                      fill="url(#emeraldSparkGradient)"
                    />
                  </AreaChart>
                </ResponsiveContainer>
              ) : (
                <div className="h-full flex items-center justify-center text-xs text-[#7e9c8b] font-mono">
                  Mengumpulkan titik data sparkline...
                </div>
              )}
            </div>
          </div>

          {/* Tokenomics & Supply Progress */}
          {supplyPercentage !== null && (
            <div className="p-3.5 bg-[#0e1713] border border-[#1a2e22] rounded-xl">
              <div className="flex justify-between text-xs font-mono mb-1.5">
                <span className="text-[#7e9c8b]">CIRCULATING / MAX SUPPLY</span>
                <span className="text-[#00ff88] font-bold">{supplyPercentage.toFixed(1)}%</span>
              </div>
              <div className="w-full bg-[#080d0a] h-2 rounded-full overflow-hidden border border-[#1a2e22]">
                <div
                  className="h-full bg-[#00ff88] shadow-[0_0_8px_rgba(0,255,136,0.5)] rounded-full transition-all duration-500"
                  style={{ width: `${supplyPercentage}%` }}
                />
              </div>
            </div>
          )}

          {/* Key Supply & Valuation Stats */}
          <div>
            <h3 className="text-[10px] font-mono font-semibold text-[#7e9c8b] uppercase tracking-wider mb-2.5">
              VALUATION & LIQUIDITY MATRIX
            </h3>
            <div className="grid grid-cols-2 gap-2.5 text-xs">
              {/* Market Cap */}
              <div className="p-3 bg-[#0e1713] border border-[#1a2e22] rounded-lg">
                <div className="text-[#7e9c8b] text-[11px] flex items-center justify-between font-display">
                  <span>Market Cap</span>
                  <InfoTooltip
                    title={GLOSSARY.marketCap.title}
                    content={GLOSSARY.marketCap.shortDesc}
                  />
                </div>
                <div className="text-sm font-bold text-white font-mono mt-1 tabular-nums">
                  {formatUsd(coin.quote.marketCapUsd)}
                </div>
              </div>

              {/* 24h Volume */}
              <div className="p-3 bg-[#0e1713] border border-[#1a2e22] rounded-lg">
                <div className="text-[#7e9c8b] text-[11px] flex items-center justify-between font-display">
                  <span>24h Volume</span>
                  <InfoTooltip
                    title={GLOSSARY.volume24h.title}
                    content={GLOSSARY.volume24h.shortDesc}
                  />
                </div>
                <div className="text-sm font-bold text-white font-mono mt-1 tabular-nums">
                  {formatUsd(coin.quote.volume24hUsd)}
                </div>
              </div>

              {/* Circulating Supply */}
              <div className="p-3 bg-[#0e1713] border border-[#1a2e22] rounded-lg">
                <div className="text-[#7e9c8b] text-[11px] flex items-center justify-between font-display">
                  <span>Circulating Supply</span>
                  <InfoTooltip
                    title={GLOSSARY.circulatingSupply.title}
                    content={GLOSSARY.circulatingSupply.shortDesc}
                    detail={GLOSSARY.circulatingSupply.detail}
                  />
                </div>
                <div className="text-sm font-bold text-[#b9cbb9] font-mono mt-1 tabular-nums">
                  {formatCompactNumber(coin.circulatingSupply, coin.symbol)}
                </div>
              </div>

              {/* Max Supply */}
              <div className="p-3 bg-[#0e1713] border border-[#1a2e22] rounded-lg">
                <div className="text-[#7e9c8b] text-[11px] flex items-center justify-between font-display">
                  <span>Max Supply</span>
                  <InfoTooltip
                    title={GLOSSARY.maxSupply.title}
                    content={GLOSSARY.maxSupply.shortDesc}
                  />
                </div>
                <div className="text-sm font-bold text-[#b9cbb9] font-mono mt-1 tabular-nums">
                  {coin.maxSupply ? formatCompactNumber(coin.maxSupply, coin.symbol) : "Unlimited / ∞"}
                </div>
              </div>

              {/* FDV */}
              {fdv && (
                <div className="col-span-2 p-3 bg-[#0e1713] border border-[#1a2e22] rounded-lg">
                  <div className="text-[#7e9c8b] text-[11px] flex items-center justify-between font-display">
                    <span>Fully Diluted Valuation (FDV)</span>
                    <InfoTooltip
                      title={GLOSSARY.fdv.title}
                      content={GLOSSARY.fdv.shortDesc}
                      detail={GLOSSARY.fdv.detail}
                    />
                  </div>
                  <div className="text-sm font-bold text-[#00ff88] font-mono mt-1 tabular-nums">
                    {formatUsd(fdv)}
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* Description */}
          {info?.description && (
            <div>
              <h3 className="text-[10px] font-mono font-semibold text-[#7e9c8b] uppercase tracking-wider mb-2">
                ASSET PROTOCOL OVERVIEW
              </h3>
              <p className="text-xs text-[#b9cbb9] leading-relaxed bg-[#080d0a] p-3.5 rounded-lg border border-[#1a2e22]">
                {info.description}
              </p>
            </div>
          )}

          {/* Official External Links */}
          {info?.urls && (
            <div>
              <h3 className="text-[10px] font-mono font-semibold text-[#7e9c8b] uppercase tracking-wider mb-2">
                VERIFIED NETWORK RESOURCES
              </h3>
              <div className="flex flex-wrap gap-2 text-xs">
                {info.urls.website?.[0] && (
                  <a
                    href={info.urls.website[0]}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded bg-[#0e1713] border border-[#1a2e22] text-[#b9cbb9] hover:text-[#00ff88] hover:border-[#00ff88]/40 transition-colors font-mono text-[11px]"
                  >
                    <Globe className="w-3.5 h-3.5 text-[#00ff88]" />
                    Website
                    <ExternalLink className="w-3 h-3 ml-0.5 opacity-60" />
                  </a>
                )}
                {info.urls.technicalDoc?.[0] && (
                  <a
                    href={info.urls.technicalDoc[0]}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded bg-[#0e1713] border border-[#1a2e22] text-[#b9cbb9] hover:text-[#00ff88] hover:border-[#00ff88]/40 transition-colors font-mono text-[11px]"
                  >
                    <FileText className="w-3.5 h-3.5 text-[#00ff88]" />
                    Whitepaper
                    <ExternalLink className="w-3 h-3 ml-0.5 opacity-60" />
                  </a>
                )}
                {info.urls.sourceCode?.[0] && (
                  <a
                    href={info.urls.sourceCode[0]}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded bg-[#0e1713] border border-[#1a2e22] text-[#b9cbb9] hover:text-[#00ff88] hover:border-[#00ff88]/40 transition-colors font-mono text-[11px]"
                  >
                    <Code className="w-3.5 h-3.5 text-[#00ff88]" />
                    Source Code
                    <ExternalLink className="w-3 h-3 ml-0.5 opacity-60" />
                  </a>
                )}
              </div>
            </div>
          )}
        </div>
      )}
    </Sheet>
  );
}
