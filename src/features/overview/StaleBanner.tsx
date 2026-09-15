import { Alert } from "@/components/ui/alert";
import { formatRelativeTime } from "@/lib/formatters";

interface StaleBannerProps {
  isStale?: boolean;
  circuitBreakerTripped?: boolean;
  lastRefreshedAt?: string;
}

export function StaleBanner({
  isStale,
  circuitBreakerTripped,
  lastRefreshedAt,
}: StaleBannerProps) {
  if (circuitBreakerTripped) {
    return (
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pt-4">
        <Alert
          variant="warning"
          title="Circuit Breaker Aktif (Batas Budget CMC Tercapai)"
          description="Pengambilan data otomatis dijeda untuk menjaga kuota credit bulanan CoinMarketCap tetap di bawah 15.000 credits. Dashboard tetap menyajikan data snapshot terakhir dari cache Redis."
        />
      </div>
    );
  }

  if (isStale) {
    return (
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pt-4">
        <Alert
          variant="info"
          title="Menampilkan Data Cache"
          description={`Data terakhir diperbarui ${formatRelativeTime(
            lastRefreshedAt
          )}. Anda dapat menekan tombol "Sync Live CMC" di header kapan saja untuk memperbarui data langsung dari CoinMarketCap.`}
        />
      </div>
    );
  }

  return null;
}
