import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { InfoTooltip } from "@/components/ui/tooltip";
import { Skeleton } from "@/components/ui/skeleton";
import {
  MarketCapIcon,
  VolumeSignalIcon,
  BtcDominanceIcon,
  EthDominanceIcon,
  ActiveClusterIcon,
} from "@/components/ui/cyber-icons";
import { formatUsd } from "@/lib/formatters";
import { GLOSSARY } from "@/lib/glossary";
import type { GlobalMarketData } from "@/types/crypto";

interface StatCardsProps {
  data?: GlobalMarketData;
  isLoading?: boolean;
}

export function StatCards({ data, isLoading }: StatCardsProps) {
  if (isLoading || !data) {
    return (
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-3.5">
        {[...Array(5)].map((_, i) => (
          <Card key={i} className="p-4">
            <Skeleton className="h-3.5 w-24 mb-3 bg-[#0e1713]" />
            <Skeleton className="h-7 w-32 mb-1 bg-[#0e1713]" />
            <Skeleton className="h-3 w-16 bg-[#0e1713]" />
          </Card>
        ))}
      </div>
    );
  }

  const cards = [
    {
      title: "Total Market Cap",
      value: formatUsd(data.totalMarketCapUsd, true),
      sub: "Global Crypto Valuation",
      icon: <MarketCapIcon size={18} className="text-[#00ff88]" />,
      tooltip: GLOSSARY.marketCap,
      highlight: true,
    },
    {
      title: "24h Volume",
      value: formatUsd(data.totalVolume24hUsd, true),
      sub: "24h Spot Turnover",
      icon: <VolumeSignalIcon size={18} className="text-[#00ff88]" />,
      tooltip: GLOSSARY.volume24h,
      highlight: false,
    },
    {
      title: "BTC Dominance",
      value: `${data.btcDominancePct.toFixed(1)}%`,
      sub: "Bitcoin Market Share",
      icon: <BtcDominanceIcon size={18} className="text-[#00ff88]" />,
      tooltip: GLOSSARY.btcDominance,
      highlight: false,
    },
    {
      title: "ETH Dominance",
      value: `${data.ethDominancePct.toFixed(1)}%`,
      sub: "Ethereum Ecosystem Share",
      icon: <EthDominanceIcon size={18} className="text-[#00e5ff]" />,
      tooltip: GLOSSARY.ethDominance,
      highlight: false,
    },
    {
      title: "Active Cryptos",
      value: data.activeCryptocurrencies.toLocaleString(),
      sub: "Tracked Assets on CMC",
      icon: <ActiveClusterIcon size={18} className="text-[#7e9c8b]" />,
      tooltip: {
        title: "Active Cryptocurrencies",
        shortDesc: "Jumlah seluruh koin & token crypto aktif yang diverifikasi dan dilacak CoinMarketCap.",
        detail: "Mencerminkan luasnya ekosistem crypto dunia saat ini.",
      },
      highlight: false,
    },
  ];

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-3.5">
      {cards.map((card, idx) => (
        <Card
          key={idx}
          className={`hover:border-[#00ff88]/40 transition-all duration-200 group ${
            card.highlight ? "border-[#00ff88]/30 shadow-[0_0_16px_rgba(0,255,136,0.06)]" : ""
          }`}
        >
          <CardHeader className="p-4 pb-1">
            <CardTitle className="text-[11px] text-[#7e9c8b] font-medium font-display tracking-wider">
              <span className="flex items-center gap-2">
                <span className="p-1 rounded bg-[#0e1713] border border-[#1a2e22] group-hover:border-[#00ff88]/30 transition-colors">
                  {card.icon}
                </span>
                {card.title}
              </span>
              <InfoTooltip
                title={card.tooltip.title}
                content={card.tooltip.shortDesc}
                detail={card.tooltip.detail}
                align="right"
              />
            </CardTitle>
          </CardHeader>
          <CardContent className="p-4 pt-1.5">
            <div className="text-xl font-bold text-white font-mono tracking-tight tabular-nums group-hover:text-[#00ff88] transition-colors">
              {card.value}
            </div>
            <div className="text-[11px] text-[#7e9c8b] mt-0.5 font-sans">
              {card.sub}
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
