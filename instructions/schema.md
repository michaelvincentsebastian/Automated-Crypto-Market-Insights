# Schema — Crypto Market Dashboard (Revamp)

**Terakhir diperbarui**: 15 September 2026

---

## 1. Sumber Data: CoinMarketCap API (Basic/Free Tier)

Endpoint yang dipakai di v1, semua termasuk dalam cakupan Basic/Free (15.000 credits/bulan, 50 req/menit, tanpa historical data):

| Endpoint | Dipakai untuk | Field kunci yang kita ambil |
|---|---|---|
| `GET /v1/global-metrics/quotes/latest` | F1 Market Overview | `total_market_cap`, `total_volume_24h`, `btc_dominance`, `eth_dominance`, `active_cryptocurrencies` |
| `GET /v3/fear-and-greed/latest` | F2 Fear & Greed | `value`, `value_classification`, `update_time` |
| `GET /v1/cryptocurrency/listings/latest` | F3 Top Coins, F5 Gainers/Losers, F6 Sparkline source | `id`, `name`, `symbol`, `cmc_rank`, `quote.USD.price`, `quote.USD.percent_change_1h/24h/7d`, `quote.USD.market_cap`, `quote.USD.volume_24h`, `circulating_supply`, `total_supply`, `max_supply` |
| `GET /v2/cryptocurrency/info` | F4 Coin Detail | `logo`, `description`, `urls`, `category` |

Catatan: nama/versi endpoint (v1/v2/v3) mengikuti dokumentasi resmi CMC — **cek ulang dokumentasi terbaru saat implementasi**, karena versi endpoint bisa berubah.

## 2. Struktur Data di Redis (Cache Layer)

Prinsip key naming: `namespace:resource:qualifier`, semua dengan TTL eksplisit — **tidak ada key tanpa TTL** (aturan wajib, lihat `rules.md`).

### 2.1 Snapshot Data (hasil transform langsung dari API, siap-serve)

| Key | Isi | TTL | Ditulis oleh |
|---|---|---|---|
| `market:global:latest` | JSON hasil transform `/global-metrics/quotes/latest` | 35 menit (interval 30 menit + buffer) | Cron ingest (30 menit) |
| `market:feargreed:latest` | JSON `{ value, classification, updated_at }` | 35 menit | Cron ingest (30 menit) |
| `market:listings:top100` | Array 100 coin, sudah termasuk rank/harga/%change/market cap/volume | 15 menit (interval 10 menit + buffer) | Cron ingest (10 menit) |
| `market:movers:gainers` | Top 5 gainer 24h, dihitung dari `market:listings:top100` (tidak fetch API terpisah) | 15 menit | Diturunkan saat ingest listings, bukan fetch baru |
| `market:movers:losers` | Top 5 loser 24h, sama seperti di atas | 15 menit | Sama seperti di atas |
| `coin:info:{coin_id}` | Metadata satu coin (logo, deskripsi, urls) | 24 jam | On-demand (dipicu saat user buka detail, kalau belum ada di cache) |

### 2.2 Rolling Window (Self-collected Historical — lihat `architecture.md` §5)

| Key | Isi | Struktur | Retensi |
|---|---|---|---|
| `history:{coin_id}:price` | Time series harga untuk sparkline | Redis Sorted Set — `score` = unix timestamp, `member` = `"{timestamp}:{price}"` | Trim ke maksimal 7 hari (hapus entry lebih lama tiap kali ada entry baru masuk, via `ZREMRANGEBYSCORE`) |

Hanya coin yang **pernah dibuka user** (dicatat sebagai "tracked coin") yang dikumpulkan history-nya secara aktif, supaya tidak nyimpen history 100 coin sekaligus yang kebanyakan tidak akan pernah dilihat — hemat Redis storage.

### 2.3 Metadata Operasional (untuk budget & observability, §6 di `architecture.md`)

| Key | Isi | TTL |
|---|---|---|
| `meta:credits:used:{YYYY-MM}` | Counter integer, credit terpakai bulan berjalan | Reset otomatis tiap bulan baru (key baru per bulan, key lama boleh expire ~35 hari) |
| `meta:ingest:last_success:{endpoint}` | Timestamp terakhir ingest endpoint tsb sukses | 48 jam |
| `meta:ingest:last_error:{endpoint}` | Pesan error terakhir (kalau ada) untuk debugging | 48 jam |

## 3. Bentuk Data yang Diserve ke Frontend (API kita sendiri)

Endpoint internal (`/api/dashboard/*`) mengembalikan bentuk yang **sudah rapi untuk UI**, bukan raw response CMC (biar frontend tidak perlu tahu bentuk asli API vendor — ini juga memudahkan kalau suatu saat ganti data provider).

Contoh bentuk `GET /api/dashboard/overview`:

```json
{
  "global": {
    "totalMarketCapUsd": 0,
    "totalVolume24hUsd": 0,
    "btcDominancePct": 0,
    "ethDominancePct": 0,
    "activeCryptocurrencies": 0
  },
  "fearGreed": {
    "value": 0,
    "classification": "Neutral",
    "updatedAt": "ISO-8601"
  },
  "topGainers": [ { "id": 0, "symbol": "", "name": "", "priceUsd": 0, "changePct24h": 0 } ],
  "topLosers": [ { "..." : "sama seperti topGainers" } ],
  "meta": {
    "lastRefreshedAt": "ISO-8601",
    "stale": false
  }
}
```

Field `meta.stale` wajib ada di **setiap** response — dihitung dari apakah timestamp cache masih dalam window "segar" yang diharapkan. Ini yang dipakai UI untuk nampilin `Alert` "data belum update" (lihat `design.md` §5).

## 4. Watchlist (Client-side, v1)

Tidak ada schema server untuk ini di v1 (sesuai PRD F7). Disimpan di `localStorage` browser:

```json
{
  "watchlist": ["bitcoin", "ethereum", "solana"]
}
```

Kalau nanti dikembangkan jadi server-side (butuh akun user), baru perlu tabel/collection terpisah — dicatat sebagai future work, bukan bagian schema v1.
