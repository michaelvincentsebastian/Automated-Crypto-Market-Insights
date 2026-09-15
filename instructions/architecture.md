# Architecture — Crypto Market Dashboard (Revamp)

**Terakhir diperbarui**: 15 September 2026

---

## 1. Prinsip Desain

1. **Zero-copy ingestion**: data dari CoinMarketCap API ditulis langsung ke cache (Redis/KV) dalam bentuk sudah-siap-pakai. Tidak ada tahap "tulis CSV → baca ulang CSV → proses" seperti versi lama. Satu fetch, satu transform ringan (kalau perlu), satu write ke cache.
2. **Scheduler terpisah dari Git**: refresh data dijalankan oleh **Vercel Cron Jobs**, bukan GitHub Actions yang bikin commit/activity palsu. Repo hanya berubah kalau memang ada perubahan kode.
3. **Cache-first read**: dashboard **tidak pernah** memanggil CMC API langsung dari browser atau on-demand per page-load. Semua request user dilayani dari cache. Ini juga alasan teknis, bukan cuma gaya — API key CMC tidak boleh exposed ke client.
4. **Budget-aware oleh desain**, bukan oleh kedisiplinan — interval refresh dihitung dari awal supaya mustahil melebihi credit bulanan meski lupa dimatikan.

## 2. Stack

| Layer | Pilihan | Alasan |
|---|---|---|
| Frontend framework | **React + Vite** (bukan Next.js) | Project ini murni dashboard SPA yang datanya di-serve dari API sendiri — tidak butuh SSR/SEO. Vite lebih ringan & cepat untuk kebutuhan ini. (Kalau nanti mau SSR/edge caching per-request, gampang migrasi ke Next — dicatat sebagai opsi masa depan, bukan keputusan sekarang.) |
| UI components | **shadcn/ui** + Tailwind CSS | Sesuai requirement kamu; komponen accessible, gampang di-restyle jadi tema crypto tanpa lawan default styling seperti library berat (MUI, dst). |
| Design prototyping | **Google Stitch** | Dipakai di tahap desain (lihat `design.md`) sebelum implementasi komponen shadcn. |
| Data fetching (client → backend sendiri) | **TanStack Query (React Query)** | Handle cache-in-browser, background refetch, loading/error state — pas untuk data yang "hampir real-time tapi bukan". |
| Charts | **Recharts** atau **visx** | Ringan, cocok untuk sparkline & line chart sederhana; tidak butuh TradingView widget yang berat kalau cuma untuk sparkline internal. |
| Backend | **Vercel Serverless Functions** (Node/TypeScript) | Satu tempat: endpoint yang di-hit cron untuk fetch+cache, dan endpoint yang di-hit frontend untuk baca cache. |
| Cache / KV store | **Upstash Redis** (via Vercel Marketplace integration) | Serverless-native, ada free tier, cocok untuk key-value + TTL yang kita butuh (lihat `schema.md`). |
| Scheduler | **Vercel Cron Jobs** | Trigger endpoint fetch di interval tertentu, tanpa sentuh Git sama sekali. |
| Hosting | **Vercel** | Satu platform untuk frontend + serverless functions + cron, minim moving parts. |

## 3. Alur Data (High-Level)

```
[Vercel Cron: tiap 5-30 menit, beda2 per jenis data]
        │
        ▼
[Serverless Function: /api/ingest/*]
        │  1. fetch ke CoinMarketCap API
        │  2. transform minimal (pilih field yang dipakai, hitung gainers/losers)
        │  3. tulis langsung ke Upstash Redis (SET dengan TTL)
        │  4. append 1 data point ke rolling window (untuk sparkline) — trim kalau kepanjangan
        ▼
[Upstash Redis]  ← ini "cache", bukan database historis penuh
        ▲
        │  baca (GET), TIDAK PERNAH nulis dari sini
        │
[Serverless Function: /api/dashboard/*]  ← dipanggil frontend
        ▲
        │  HTTPS (JSON)
        │
[React SPA di browser, via React Query]
```

Poin penting: **frontend tidak pernah tahu soal CoinMarketCap API key**. Frontend hanya bicara ke serverless function milik kita sendiri, yang baca dari Redis. Ini juga menutup celah rate-limit abuse dari sisi client.

## 4. Kenapa Ini "Zero-Copy" Dibanding Versi Lama

Versi lama: `CMC API → Python script → CSV file → baca CSV lagi → SQLite → Streamlit baca SQLite`. Lima hop, tiga representasi berbeda dari data yang sama.

Versi baru: `CMC API → transform in-memory → Redis (satu representasi) → dibaca langsung oleh endpoint yang serve frontend`. Dua hop, satu representasi. Tidak ada file di disk yang perlu di-commit, di-sync, atau bisa "kacau" antara server dan GitHub seperti masalah yang pernah kamu alami di project lakehouse/healthcare-bridge kamu.

## 5. Menyiasati "No Historical Data" di Free Tier

CMC Basic/Free **tidak** include endpoint historical OHLCV. Solusinya: kita **bikin historical data sendiri**, sekecil mungkin, dari snapshot yang sudah kita fetch untuk keperluan lain (jadi tidak nambah credit cost):

- Setiap kali cron fetch `listings/latest`, kita simpan juga 1 titik data ringkas (harga + timestamp) per coin yang di-track ke rolling window di Redis (misal list dengan max length, atau sorted set by timestamp).
- Window dibatasi (contoh: 7 hari terakhir, 1 titik tiap fetch interval) supaya ukuran data di Redis tetap kecil dan sesuai free tier Upstash.
- Konsekuensi jujur: sparkline "7 hari" hanya benar-benar 7 hari **setelah dashboard live 7 hari**. Sebelum itu, chart menampilkan data sejak dashboard mulai — ini dikomunikasikan di UI (lihat F6 & F8 di `prd.md`), bukan disamarkan.

## 6. Budget Credit — Perhitungan

Basic/Free tier: **15.000 call credits/bulan** (~500/hari), **50 request/menit**.

Estimasi harian dengan interval yang diajukan di `prd.md` §6:

| Endpoint | Interval | Panggilan/hari | Credit/panggilan (estimasi) | Credit/hari |
|---|---|---|---|---|
| `/global-metrics/quotes/latest` | 30 menit | 48 | 1 | 48 |
| `/fear-and-greed/latest` | 30 menit | 48 | 1 | 48 |
| `/cryptocurrency/listings/latest` (100 coin) | 10 menit | 144 | 1 | 144 |
| `/cryptocurrency/info` (metadata, on-demand + cache 24 jam) | jarang | ~20 (asumsi eksplorasi) | 1 | ~20 |
| **Total estimasi** | | | | **~260/hari (~7.800/bulan)** |

Masih ada headroom cukup besar (>7.000 credits) dari limit bulanan — sengaja disisakan buffer untuk nice-to-have (F9–F13) dan untuk hari-hari kamu banyak testing/development yang ikut manggil API sungguhan.

**Aturan keras**: implementasi wajib punya circuit breaker — kalau counter credit bulanan (disimpan di Redis, direset tiap awal bulan) mendekati ambang batas (misal 90%), endpoint ingest berhenti fetch dan dashboard menampilkan data cache terakhir + banner "data belum update, mendekati limit bulanan". Detail di `rules.md`.

## 7. Environment & Secrets

- `CMC_API_KEY` — hanya di environment variable serverless function, tidak pernah di client bundle.
- `UPSTASH_REDIS_REST_URL` / `UPSTASH_REDIS_REST_TOKEN` — via Vercel integration, auto-injected.
- Tidak ada secret di repo, tidak ada `.env` ke-commit (lihat `rules.md` untuk `.gitignore` wajib).

## 8. Deployment

- `main` branch → auto-deploy ke production Vercel.
- Cron job didefinisikan di `vercel.json`, bukan di GitHub Actions — supaya jelas: **GitHub = kode saja, Vercel = eksekusi berjadwal**.
- Preview deployment untuk PR/branch lain tidak menjalankan cron (supaya tidak dobel-fetch dan boros credit dari environment preview).

## 9. Observability Minimal (v1)

- Log tiap ingest run: sukses/gagal, credit terpakai, durasi — cukup lewat Vercel's built-in function logs dulu, belum perlu tool eksternal (Sentry/Datadog) di v1 karena ini personal project dengan traffic kecil.
- Endpoint `/api/health` sederhana yang menampilkan: kapan data terakhir sukses di-refresh, sisa estimasi credit bulan ini.
