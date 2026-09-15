# cryptsight

[![Live Demo](https://img.shields.io/badge/Live_Demo-crypsight.vercel.app-00FF88?style=for-the-badge&logo=vercel&logoColor=black)](https://crypsight.vercel.app/)

[![Vite](https://img.shields.io/badge/Vite-5.4-646CFF?logo=vite&logoColor=white)](https://vitejs.dev/)
[![React](https://img.shields.io/badge/React-18.3-61DAFB?logo=react&logoColor=black)](https://reactjs.org/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.6-3178C6?logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![TailwindCSS](https://img.shields.io/badge/TailwindCSS-3.4-38B2AC?logo=tailwind-css&logoColor=white)](https://tailwindcss.com/)
[![Upstash Redis](https://img.shields.io/badge/Upstash-Redis-00E699?logo=redis&logoColor=white)](https://upstash.com/)
[![Vercel](https://img.shields.io/badge/Vercel-Serverless-000000?logo=vercel&logoColor=white)](https://vercel.com/)

> 🌐 **Live Dashboard**: [https://crypsight.vercel.app/](https://crypsight.vercel.app/)

A modern, high-performance cryptocurrency market intelligence dashboard built with a **zero-copy ingestion architecture**, integrated credit circuit breaker, educational tooltips for domain learners, and automated scheduler decoupled from Git history.

---

## 💡 Mengapa Versi 2 (v2 Revamp)?

Versi pertama project ini (Python + Streamlit + GitHub Actions cron tiap 6 menit) memiliki dua kelemahan mendasar:
1. **Histori Commit Palsu**: GitHub Actions melakukan auto-commit file CSV setiap 6 menit, membuat grafik kontribusi Git menjadi sangat padat secara semu.
2. **Pipeline Redundan (Multi-Hop)**: `API → Script Python → CSV → SQLite → Streamlit` (5 hop) menghasilkan duplikasi representasi data yang tidak efisien.

### Perbandingan Arsitektur:

| Aspek | v1 (Legacy) | v2 (Zero-Copy) |
|---|---|---|
| **Alur Data** | API → CSV → SQLite → Streamlit (5 hop) | API → Redis Cache → Frontend (2 hop) |
| **Ingestion & Sync** | GitHub Actions (auto-commit tiap 6 mnt) | On-Demand UI Sync & Auto-Cache (di luar Git) |
| **Penyimpanan** | File CSV & SQLite di disk/repo git | Upstash Redis KV Store dengan TTL |
| **Frontend** | Streamlit (Python server-rendered) | React + Vite + TypeScript (SPA) |
| **UI & Desain** | Default Streamlit widgets | Custom shadcn/ui + Tailwind Dark Crypto theme |
| **Domain Learning** | Hanya angka tanpa penjelasan | Micro-explanation tooltips di setiap metrik pasar |
| **Budget Safety** | Tanpa kontrol kuota | Circuit breaker otomatis saat credit ≥ 90% |

---

## 🚀 Fitur Utama

- **⚡ Zero-Copy Ingestion**: Data dari CoinMarketCap API langsung masuk ke cache Redis siap pakai dengan TTL eksplisit. Tidak ada perantara CSV/SQLite.
- **📊 Global Market Overview**: Total Market Cap, 24h Trading Volume, BTC Dominance, ETH Dominance, dan Active Cryptocurrencies.
- **🧭 Fear & Greed Index**: Gauge visual SVG (skor 0–100) dengan label sentimen (*Extreme Fear* s/d *Extreme Greed*) dan analisis sentimen pasar.
- **🔥 Top Gainers & Losers**: 5 koin dengan kenaikan dan penurunan harga tertinggi dalam 24 jam terakhir (dihitung in-memory dari data listings).
- **🏆 Top 100 Coin Rankings**: Tabel interaktif lengkap dengan logo koin, rank, harga, % perubahan (1h, 24h, 7d), volume 24 jam, dan kapitalisasi pasar.
  - *Sortable*: Urutkan berdasarkan rank, nama, harga, persentase, volume, atau market cap.
  - *Searchable*: Filter instan berdasarkan nama atau simbol koin.
  - *Responsive*: Kolom sekunder disesuaikan otomatis untuk tampilan mobile.
- **⭐ Local Watchlist**: Pin koin favorit langsung dari tabel atau panel detail, disimpan secara persisten di browser (`localStorage`) tanpa perlu register/login.
- **🔍 Coin Detail Sheet**: Panel drawer samping menampilkan harga besar, grafik sparkline (*self-collected price trend*), metrik pasokan (*Circulating*, *Total*, *Max Supply*, *FDV*), deskripsi resmi, serta link eksplorer & whitepaper.
- **🎓 Domain Education Tooltips**: Micro-explanation ramah pemula pada setiap istilah finansial crypto (Market Cap, Dominance, Circulating Supply, FDV, dll).
- **🛡️ Budget Circuit Breaker & Health Modal**: Pelacakan penggunaan credit bulanan CMC (15.000 credit/bulan) dengan auto-halt pada 90% (13.500 credits) untuk menjaga budget free tier tetap aman.

---

## 🛠️ Tech Stack

- **Frontend**: React 18, Vite 5, TypeScript, Tailwind CSS, shadcn/ui, Lucide Icons, Recharts
- **State & Data Fetching**: TanStack Query (React Query v5)
- **Backend / Serverless**: Vercel Serverless Functions (Node.js / TypeScript)
- **Cache / Storage**: Upstash Redis (REST API)
- **Data Sync**: On-Demand Live Sync via UI & Automatic Initial Ingestion (Vercel Hobby friendly)
- **Hosting**: Vercel

---

## 📁 Struktur Direktori

```
Automated-Crypto-Market-Insights/
├── api/                           # Vercel Serverless Functions
│   ├── _lib/
│   │   ├── cmc.ts                 # CMC client, budget tracker & circuit breaker
│   │   ├── redis.ts               # Upstash Redis client + dev fallback
│   │   └── types.ts               # Shared backend/frontend TypeScript types
│   ├── dashboard/                 # Endpoint baca data untuk frontend (Read-only)
│   │   ├── coin.ts                # GET /api/dashboard/coin?id=...
│   │   ├── listings.ts            # GET /api/dashboard/listings (Top 100)
│   │   └── overview.ts            # GET /api/dashboard/overview (Global, FGI, Movers)
│   ├── ingest/                    # Endpoint ingest data CMC ke Redis (on-demand)
│   │   ├── feargreed.ts           # Ingest Fear & Greed Index
│   │   ├── global.ts              # Ingest Global Metrics
│   │   └── listings.ts            # Ingest Top 100 Listings & Sparklines
│   └── health.ts                  # GET /api/health (status credit & ingest)
├── src/                           # Frontend React SPA
│   ├── components/
│   │   └── ui/                    # Komponen UI (card, table, sheet, tooltip, badge, dll)
│   ├── features/
│   │   ├── detail/                # CoinDetailSheet & sparkline chart
│   │   ├── health/                # HealthModal & credit monitor
│   │   ├── listings/              # CoinTable & search/sort/filter
│   │   └── overview/              # MarketHeader, StatCards, FearGreedGauge, MoversCards
│   ├── lib/                       # Formatters, glossary edukasi, watchlist hook
│   ├── types/                     # Frontend types
│   ├── App.tsx                    # Root App component
│   ├── index.css                  # Tailwind styles & dark mode palette
│   └── main.tsx                   # React root entrypoint
├── instructions/                  # Dokumen spesifikasi refaktor
│   ├── architecture.md
│   ├── design.md
│   ├── prd.md
│   ├── rules.md
│   └── schema.md
├── .env.example                   # Template environment variables
├── package.json
├── tailwind.config.js
├── tsconfig.json
├── vercel.json                    # Konfigurasi Vercel rewrites
└── vite.config.ts                 # Vite config + local API dev middleware
```

---

## ⚙️ Panduan Menjalankan Project

### 1. Prasyarat
- Node.js v18 atau lebih baru
- npm v9 atau lebih baru

### 2. Instalasi Dependensi
```bash
npm install
```

### 3. Konfigurasi Environment Variable
Salin template `.env.example` menjadi `.env`:
```bash
cp .env.example .env
```

Isi variabel di `.env` (opsional untuk dev lokal; jika dikosongkan, aplikasi akan menggunakan mock data otomatis):
```env
CMC_API_KEY=your_coinmarketcap_api_key
UPSTASH_REDIS_REST_URL=https://your-database.upstash.io
UPSTASH_REDIS_REST_TOKEN=your_upstash_token
```

### 4. Menjalankan di Lingkungan Lokal (Local Development)
```bash
npm run dev
```
Buka browser di `http://localhost:3000/`. Server dev Vite sudah dilengkapi middleware otomatis yang melayani endpoint `/api/*` secara transparan.

### 5. Membangun untuk Produksi (Production Build)
```bash
npm run build
```

---

## ☁️ Deployment ke Vercel

1. Push kode ke repository GitHub Anda (cabang `main`).
2. Import repository di dashboard [Vercel](https://vercel.com/).
3. Hubungkan integrasi **Upstash Redis** melalui Vercel Marketplace (variabel `UPSTASH_REDIS_REST_URL` dan `UPSTASH_REDIS_REST_TOKEN` akan otomatis terkonfigurasi).
4. Tambahkan `CMC_API_KEY` pada menu **Project Settings > Environment Variables** di Vercel.
5. Klik **Deploy**.
   - Data akan di-cache ke Upstash Redis saat aplikasi pertama kali diakses, dan dapat diperbarui secara live kapan saja lewat tombol **"Sync Live CMC"** di header dashboard.
   - Tidak memerlukan cron job harian/berulang, 100% kompatibel dan aman dengan tier gratis **Vercel Hobby** tanpa batasan cron.
   - Dashboard dapat langsung diakses secara live di: **[https://crypsight.vercel.app/](https://crypsight.vercel.app/)**

---

## 📜 Lisensi
MIT License. Dibuat oleh Vincent.
