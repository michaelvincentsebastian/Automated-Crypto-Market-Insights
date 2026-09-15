import { useState, useEffect } from "react";

const STORAGE_KEY = "crypto_watchlist_ids";

export function getStoredWatchlist(): number[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return [1, 1027, 5426]; // Defaults: BTC, ETH, SOL
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed.map(Number) : [1, 1027, 5426];
  } catch {
    return [1, 1027, 5426];
  }
}

export function useWatchlist() {
  const [watchlist, setWatchlist] = useState<number[]>(getStoredWatchlist);

  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(watchlist));
    } catch (e) {
      console.error("Failed to save watchlist to localStorage", e);
    }
  }, [watchlist]);

  const toggleWatchlist = (id: number) => {
    setWatchlist(prev => {
      if (prev.includes(id)) {
        return prev.filter(x => x !== id);
      } else {
        return [...prev, id];
      }
    });
  };

  const isWatchlisted = (id: number) => watchlist.includes(id);

  return {
    watchlist,
    toggleWatchlist,
    isWatchlisted,
  };
}
