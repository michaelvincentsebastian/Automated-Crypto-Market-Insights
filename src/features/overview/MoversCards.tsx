import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { NeonUpIcon, NeonDownIcon } from "@/components/ui/cyber-icons";
import { formatUsd, formatPercent } from "@/lib/formatters";
import type { MarketMoverCoin } from "@/types/crypto";

interface MoversCardsProps {
  gainers?: MarketMoverCoin[];
  losers?: MarketMoverCoin[];
  isLoading?: boolean;
  onSelectCoin: (id: number) => void;
}

export function MoversCards({
  gainers = [],
  losers = [],
  isLoading,
  onSelectCoin,
}: MoversCardsProps) {
  if (isLoading) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3.5 h-full">
        <Card className="p-5">
          <Skeleton className="h-5 w-32 mb-4 bg-[#0e1713]" />
          {[...Array(5)].map((_, i) => (
            <Skeleton key={i} className="h-8 w-full mb-2 bg-[#0e1713]" />
          ))}
        </Card>
        <Card className="p-5">
          <Skeleton className="h-5 w-32 mb-4 bg-[#0e1713]" />
          {[...Array(5)].map((_, i) => (
            <Skeleton key={i} className="h-8 w-full mb-2 bg-[#0e1713]" />
          ))}
        </Card>
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-3.5 h-full">
      {/* Top Gainers Card */}
      <Card className="hover:border-[#00ff88]/30 transition-all duration-200">
        <CardHeader className="p-4 pb-2">
          <CardTitle className="text-xs font-semibold text-[#7e9c8b] flex items-center justify-between font-display">
            <span className="flex items-center gap-2">
              <span className="p-1 rounded bg-[#0e1713] border border-[#1a2e22] text-[#00ff88]">
                <NeonUpIcon size={14} />
              </span>
              <span>TOP 24H GAINERS</span>
            </span>
            <Badge variant="success">TOP 5</Badge>
          </CardTitle>
        </CardHeader>
        <CardContent className="p-4 pt-0">
          <div className="divide-y divide-[#1a2e22]/60">
            {gainers.map((coin, index) => (
              <div
                key={coin.id}
                onClick={() => onSelectCoin(coin.id)}
                className="py-2 flex items-center justify-between hover:bg-[#0e1713] px-2 rounded cursor-pointer transition-colors group"
              >
                <div className="flex items-center gap-2.5">
                  <span className="text-[11px] font-mono text-[#7e9c8b] w-4">
                    {index + 1}
                  </span>
                  <div>
                    <span className="font-semibold text-xs text-[#f1ffef] group-hover:text-[#00ff88] transition-colors font-display">
                      {coin.symbol}
                    </span>
                    <span className="text-[11px] text-[#7e9c8b] ml-1.5 hidden sm:inline">
                      {coin.name}
                    </span>
                  </div>
                </div>

                <div className="text-right flex items-center gap-2.5">
                  <span className="text-xs font-mono text-slate-300 tabular-nums">
                    {formatUsd(coin.priceUsd)}
                  </span>
                  <span className="inline-flex items-center text-[11px] font-mono font-semibold text-[#00ff88] bg-[#00ff88]/10 border border-[#00ff88]/20 px-1.5 py-0.5 rounded tabular-nums shadow-[0_0_8px_rgba(0,255,136,0.1)]">
                    <NeonUpIcon size={12} className="mr-0.5" />
                    {formatPercent(coin.changePct24h)}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Top Losers Card */}
      <Card className="hover:border-[#ff3366]/30 transition-all duration-200">
        <CardHeader className="p-4 pb-2">
          <CardTitle className="text-xs font-semibold text-[#7e9c8b] flex items-center justify-between font-display">
            <span className="flex items-center gap-2">
              <span className="p-1 rounded bg-[#0e1713] border border-[#1a2e22] text-[#ff3366]">
                <NeonDownIcon size={14} />
              </span>
              <span>TOP 24H LOSERS</span>
            </span>
            <Badge variant="danger">TOP 5</Badge>
          </CardTitle>
        </CardHeader>
        <CardContent className="p-4 pt-0">
          <div className="divide-y divide-[#1a2e22]/60">
            {losers.map((coin, index) => (
              <div
                key={coin.id}
                onClick={() => onSelectCoin(coin.id)}
                className="py-2 flex items-center justify-between hover:bg-[#0e1713] px-2 rounded cursor-pointer transition-colors group"
              >
                <div className="flex items-center gap-2.5">
                  <span className="text-[11px] font-mono text-[#7e9c8b] w-4">
                    {index + 1}
                  </span>
                  <div>
                    <span className="font-semibold text-xs text-[#f1ffef] group-hover:text-[#ff3366] transition-colors font-display">
                      {coin.symbol}
                    </span>
                    <span className="text-[11px] text-[#7e9c8b] ml-1.5 hidden sm:inline">
                      {coin.name}
                    </span>
                  </div>
                </div>

                <div className="text-right flex items-center gap-2.5">
                  <span className="text-xs font-mono text-slate-300 tabular-nums">
                    {formatUsd(coin.priceUsd)}
                  </span>
                  <span className="inline-flex items-center text-[11px] font-mono font-semibold text-[#ff3366] bg-[#ff3366]/10 border border-[#ff3366]/20 px-1.5 py-0.5 rounded tabular-nums shadow-[0_0_8px_rgba(255,51,102,0.1)]">
                    <NeonDownIcon size={12} className="mr-0.5" />
                    {formatPercent(coin.changePct24h)}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
