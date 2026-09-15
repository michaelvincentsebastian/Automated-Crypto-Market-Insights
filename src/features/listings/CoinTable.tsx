import { useState, useMemo } from "react";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { InfoTooltip } from "@/components/ui/tooltip";
import { NeonUpIcon, NeonDownIcon } from "@/components/ui/cyber-icons";
import { formatUsd, formatPercent } from "@/lib/formatters";
import { GLOSSARY } from "@/lib/glossary";
import type { CoinItem } from "@/types/crypto";
import { Star, ArrowUpDown, ArrowUp, ArrowDown, Layers } from "lucide-react";

interface CoinTableProps {
  coins: CoinItem[];
  isLoading?: boolean;
  watchlist: number[];
  onToggleWatchlist: (id: number) => void;
  onSelectCoin: (id: number) => void;
}

type SortField = "rank" | "name" | "price" | "change1h" | "change24h" | "change7d" | "marketCap" | "volume";
type SortDirection = "asc" | "desc";

export function CoinTable({
  coins,
  isLoading,
  watchlist,
  onToggleWatchlist,
  onSelectCoin,
}: CoinTableProps) {
  const [search, setSearch] = useState("");
  const [showWatchlistOnly, setShowWatchlistOnly] = useState(false);
  const [sortField, setSortField] = useState<SortField>("rank");
  const [sortDirection, setSortDirection] = useState<SortDirection>("asc");

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(prev => (prev === "asc" ? "desc" : "asc"));
    } else {
      setSortField(field);
      setSortDirection(field === "rank" || field === "name" ? "asc" : "desc");
    }
  };

  const filteredCoins = useMemo(() => {
    return coins.filter(coin => {
      const matchesSearch =
        coin.name.toLowerCase().includes(search.toLowerCase()) ||
        coin.symbol.toLowerCase().includes(search.toLowerCase());

      const matchesWatchlist = showWatchlistOnly ? watchlist.includes(coin.id) : true;

      return matchesSearch && matchesWatchlist;
    });
  }, [coins, search, showWatchlistOnly, watchlist]);

  const sortedCoins = useMemo(() => {
    const list = [...filteredCoins];
    list.sort((a, b) => {
      let valA: any = 0;
      let valB: any = 0;

      switch (sortField) {
        case "rank":
          valA = a.cmcRank;
          valB = b.cmcRank;
          break;
        case "name":
          valA = a.name.toLowerCase();
          valB = b.name.toLowerCase();
          break;
        case "price":
          valA = a.quote.priceUsd;
          valB = b.quote.priceUsd;
          break;
        case "change1h":
          valA = a.quote.percentChange1h;
          valB = b.quote.percentChange1h;
          break;
        case "change24h":
          valA = a.quote.percentChange24h;
          valB = b.quote.percentChange24h;
          break;
        case "change7d":
          valA = a.quote.percentChange7d;
          valB = b.quote.percentChange7d;
          break;
        case "marketCap":
          valA = a.quote.marketCapUsd;
          valB = b.quote.marketCapUsd;
          break;
        case "volume":
          valA = a.quote.volume24hUsd;
          valB = b.quote.volume24hUsd;
          break;
      }

      if (valA < valB) return sortDirection === "asc" ? -1 : 1;
      if (valA > valB) return sortDirection === "asc" ? 1 : -1;
      return 0;
    });
    return list;
  }, [filteredCoins, sortField, sortDirection]);

  const renderSortIcon = (field: SortField) => {
    if (sortField !== field) {
      return <ArrowUpDown className="w-3 h-3 ml-1 text-[#3a5245] opacity-60 group-hover:opacity-100" />;
    }
    return sortDirection === "asc" ? (
      <ArrowUp className="w-3 h-3 ml-1 text-[#00ff88]" />
    ) : (
      <ArrowDown className="w-3 h-3 ml-1 text-[#00ff88]" />
    );
  };

  return (
    <Card className="overflow-hidden border-[#1a2e22]/80 shadow-2xl">
      {/* Table Header & Controls */}
      <CardHeader className="p-4 sm:p-5 pb-3 border-b border-[#1a2e22]/80 bg-[#070c09]/60">
        <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-3">
          <div className="flex items-center gap-3">
            <CardTitle className="text-sm font-bold text-white flex items-center gap-2 font-display">
              <Layers className="w-4 h-4 text-[#00ff88]" />
              TOP 100 CRYPTO RANKINGS
            </CardTitle>
            <Badge variant="cyan" className="font-mono text-[10px]">
              {sortedCoins.length} ASSETS
            </Badge>
          </div>

          <div className="flex items-center gap-2.5 flex-wrap">
            {/* Search Input */}
            <div className="w-48 sm:w-64">
              <Input
                withSearchIcon
                placeholder="Filter coin or ticker..."
                value={search}
                onChange={e => setSearch(e.target.value)}
                className="bg-[#080d0a] border-[#1a2e22] text-xs text-[#f1ffef] focus:border-[#00ff88]/50"
              />
            </div>

            {/* Watchlist Filter Toggle */}
            <Button
              variant={showWatchlistOnly ? "primary" : "secondary"}
              size="sm"
              onClick={() => setShowWatchlistOnly(!showWatchlistOnly)}
              className="text-xs gap-1.5"
            >
              <Star className={`w-3.5 h-3.5 ${showWatchlistOnly ? "fill-[#050807] text-[#050807]" : "text-amber-400"}`} />
              <span>Watchlist</span>
              {watchlist.length > 0 && (
                <span className="ml-1 px-1.5 py-0.2 bg-black/40 rounded text-[10px] font-mono">
                  {watchlist.length}
                </span>
              )}
            </Button>
          </div>
        </div>
      </CardHeader>

      {/* Table Content */}
      <CardContent className="p-0">
        <div className="overflow-x-auto">
          <table className="w-full text-left border-collapse">
            <thead>
              <tr className="border-b border-[#1a2e22]/80 bg-[#070c09] text-[10px] font-mono font-semibold text-[#7e9c8b] uppercase tracking-wider">
                <th className="py-2.5 px-3 w-10 text-center">★</th>
                <th
                  className="py-2.5 px-3 cursor-pointer select-none group"
                  onClick={() => handleSort("rank")}
                >
                  <span className="inline-flex items-center">
                    # {renderSortIcon("rank")}
                  </span>
                </th>
                <th
                  className="py-2.5 px-3 cursor-pointer select-none group"
                  onClick={() => handleSort("name")}
                >
                  <span className="inline-flex items-center">
                    ASSET {renderSortIcon("name")}
                  </span>
                </th>
                <th
                  className="py-2.5 px-3 text-right cursor-pointer select-none group"
                  onClick={() => handleSort("price")}
                >
                  <span className="inline-flex items-center justify-end w-full">
                    PRICE (USD) {renderSortIcon("price")}
                  </span>
                </th>
                <th
                  className="py-2.5 px-3 text-right cursor-pointer select-none group hidden lg:table-cell"
                  onClick={() => handleSort("change1h")}
                >
                  <span className="inline-flex items-center justify-end w-full">
                    1H % {renderSortIcon("change1h")}
                  </span>
                </th>
                <th
                  className="py-2.5 px-3 text-right cursor-pointer select-none group"
                  onClick={() => handleSort("change24h")}
                >
                  <span className="inline-flex items-center justify-end w-full">
                    24H % {renderSortIcon("change24h")}
                  </span>
                </th>
                <th
                  className="py-2.5 px-3 text-right cursor-pointer select-none group hidden sm:table-cell"
                  onClick={() => handleSort("change7d")}
                >
                  <span className="inline-flex items-center justify-end w-full">
                    7D % {renderSortIcon("change7d")}
                  </span>
                </th>
                <th
                  className="py-2.5 px-3 text-right cursor-pointer select-none group hidden md:table-cell"
                  onClick={() => handleSort("volume")}
                >
                  <span className="inline-flex items-center justify-end w-full">
                    24H VOLUME {renderSortIcon("volume")}
                    <InfoTooltip
                      title={GLOSSARY.volume24h.title}
                      content={GLOSSARY.volume24h.shortDesc}
                      align="right"
                    />
                  </span>
                </th>
                <th
                  className="py-2.5 px-4 text-right cursor-pointer select-none group"
                  onClick={() => handleSort("marketCap")}
                >
                  <span className="inline-flex items-center justify-end w-full">
                    MARKET CAP {renderSortIcon("marketCap")}
                    <InfoTooltip
                      title={GLOSSARY.marketCap.title}
                      content={GLOSSARY.marketCap.shortDesc}
                      align="right"
                    />
                  </span>
                </th>
              </tr>
            </thead>

            <tbody className="divide-y divide-[#1a2e22]/50 text-xs">
              {isLoading ? (
                [...Array(10)].map((_, i) => (
                  <tr key={i} className="animate-pulse">
                    <td className="py-3 px-3 text-center">
                      <Skeleton className="w-4 h-4 mx-auto rounded bg-[#0e1713]" />
                    </td>
                    <td className="py-3 px-3">
                      <Skeleton className="w-5 h-4 bg-[#0e1713]" />
                    </td>
                    <td className="py-3 px-3">
                      <Skeleton className="w-28 h-4 bg-[#0e1713]" />
                    </td>
                    <td className="py-3 px-3 text-right">
                      <Skeleton className="w-16 h-4 ml-auto bg-[#0e1713]" />
                    </td>
                    <td className="py-3 px-3 text-right hidden lg:table-cell">
                      <Skeleton className="w-12 h-4 ml-auto bg-[#0e1713]" />
                    </td>
                    <td className="py-3 px-3 text-right">
                      <Skeleton className="w-12 h-4 ml-auto bg-[#0e1713]" />
                    </td>
                    <td className="py-3 px-3 text-right hidden sm:table-cell">
                      <Skeleton className="w-12 h-4 ml-auto bg-[#0e1713]" />
                    </td>
                    <td className="py-3 px-3 text-right hidden md:table-cell">
                      <Skeleton className="w-20 h-4 ml-auto bg-[#0e1713]" />
                    </td>
                    <td className="py-3 px-4 text-right">
                      <Skeleton className="w-24 h-4 ml-auto bg-[#0e1713]" />
                    </td>
                  </tr>
                ))
              ) : sortedCoins.length === 0 ? (
                <tr>
                  <td colSpan={9} className="py-12 text-center text-[#7e9c8b]">
                    {showWatchlistOnly
                      ? "Belum ada koin di watchlist. Klik ikon bintang ★ di baris koin untuk menambahkannya."
                      : "Tidak ada koin yang sesuai dengan pencarian."}
                  </td>
                </tr>
              ) : (
                sortedCoins.map(coin => {
                  const isPinned = watchlist.includes(coin.id);
                  const isPositive1h = coin.quote.percentChange1h >= 0;
                  const isPositive24h = coin.quote.percentChange24h >= 0;
                  const isPositive7d = coin.quote.percentChange7d >= 0;

                  return (
                    <tr
                      key={coin.id}
                      onClick={() => onSelectCoin(coin.id)}
                      className="hover:bg-[#0e1713]/80 cursor-pointer transition-colors group"
                    >
                      {/* Pin to Watchlist */}
                      <td
                        className="py-2.5 px-3 text-center"
                        onClick={e => {
                          e.stopPropagation();
                          onToggleWatchlist(coin.id);
                        }}
                      >
                        <button
                          type="button"
                          className="p-1 hover:scale-110 transition-transform focus:outline-none"
                          aria-label={isPinned ? "Unpin from watchlist" : "Pin to watchlist"}
                        >
                          <Star
                            className={`w-3.5 h-3.5 ${
                              isPinned
                                ? "fill-amber-400 text-amber-400"
                                : "text-[#3a5245] group-hover:text-[#7e9c8b]"
                            }`}
                          />
                        </button>
                      </td>

                      {/* Rank */}
                      <td className="py-2.5 px-3 font-mono text-[#7e9c8b] tabular-nums text-[11px]">
                        {coin.cmcRank}
                      </td>

                      {/* Name & Symbol */}
                      <td className="py-2.5 px-3 font-medium">
                        <div className="flex items-center gap-2">
                          <img
                            src={`https://s2.coinmarketcap.com/static/img/coins/64x64/${coin.id}.png`}
                            alt={coin.name}
                            className="w-5 h-5 rounded-full bg-[#080d0a] border border-[#1a2e22]"
                            onError={e => {
                              (e.target as HTMLElement).style.display = "none";
                            }}
                          />
                          <span className="text-[#f1ffef] font-display font-semibold group-hover:text-[#00ff88] transition-colors">
                            {coin.name}
                          </span>
                          <span className="text-[#7e9c8b] font-mono text-[10px] uppercase">
                            {coin.symbol}
                          </span>
                        </div>
                      </td>

                      {/* Price */}
                      <td className="py-2.5 px-3 text-right font-mono font-medium text-white tabular-nums">
                        {formatUsd(coin.quote.priceUsd)}
                      </td>

                      {/* 1h % */}
                      <td
                        className={`py-2.5 px-3 text-right font-mono tabular-nums text-[11px] hidden lg:table-cell ${
                          isPositive1h ? "text-[#00ff88]" : "text-[#ff3366]"
                        }`}
                      >
                        {formatPercent(coin.quote.percentChange1h)}
                      </td>

                      {/* 24h % */}
                      <td className="py-2.5 px-3 text-right font-mono tabular-nums">
                        <span
                          className={`inline-flex items-center font-semibold px-1.5 py-0.5 rounded text-[11px] ${
                            isPositive24h
                              ? "text-[#00ff88] bg-[#00ff88]/10 border border-[#00ff88]/20 shadow-[0_0_6px_rgba(0,255,136,0.1)]"
                              : "text-[#ff3366] bg-[#ff3366]/10 border border-[#ff3366]/20 shadow-[0_0_6px_rgba(255,51,102,0.1)]"
                          }`}
                        >
                          {isPositive24h ? (
                            <NeonUpIcon size={11} className="mr-0.5" />
                          ) : (
                            <NeonDownIcon size={11} className="mr-0.5" />
                          )}
                          {formatPercent(coin.quote.percentChange24h)}
                        </span>
                      </td>

                      {/* 7d % */}
                      <td
                        className={`py-2.5 px-3 text-right font-mono tabular-nums text-[11px] hidden sm:table-cell ${
                          isPositive7d ? "text-[#00ff88]" : "text-[#ff3366]"
                        }`}
                      >
                        {formatPercent(coin.quote.percentChange7d)}
                      </td>

                      {/* 24h Volume */}
                      <td className="py-2.5 px-3 text-right font-mono text-[#b9cbb9] tabular-nums hidden md:table-cell text-[11px]">
                        {formatUsd(coin.quote.volume24hUsd, true)}
                      </td>

                      {/* Market Cap */}
                      <td className="py-2.5 px-4 text-right font-mono font-semibold text-[#f1ffef] tabular-nums text-[11px]">
                        {formatUsd(coin.quote.marketCapUsd, true)}
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  );
}
