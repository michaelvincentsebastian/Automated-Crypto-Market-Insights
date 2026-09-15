# PRD — Crypto Market Dashboard (Revamp)

**Project**: Automated Crypto Market Insights v2
**Owner**: Vincent
**Status**: Draft
**Terakhir diperbarui**: 15 September 2026

---

## 1. Latar Belakang

Versi pertama project ini (Python + Streamlit + GitHub Actions, deploy di Streamlit Cloud) punya dua masalah utama:

1. **GitHub Actions cron setiap 6 menit** membuat commit/activity graph repo jadi sangat tinggi tapi fiktif — bukan cerminan kerja aktual, dan berpotensi menyesatkan siapa pun yang menilai repo ini sebagai portfolio.
2. **Pipeline data tidak perlu**: data API di-convert ke CSV dulu sebelum dipakai dashboard, padahal untuk data "latest snapshot" itu bisa langsung difetch → cache → render tanpa file perantara.

Versi kedua ini dibangun ulang dari nol dengan filosofi **zero-copy ingestion**: data dari CoinMarketCap API masuk ke cache/KV store, dashboard baca dari cache, tidak ada file CSV/SQLite sebagai perantara wajib. Refresh data dijalankan oleh scheduler di luar Git (Vercel Cron), jadi histori commit repo tetap bersih dan mencerminkan kerja pengembangan yang sesungguhnya.

## 2. Tujuan (Goals)

- G1: Dashboard crypto modern, cepat, dan enak dipakai untuk memantau market harian (top coins, market movers, sentiment).
- G2: Menjadi portfolio piece yang menunjukkan kemampuan full-stack: React modern, design system (shadcn/ui), API integration, caching strategy, dan disiplin arsitektur (bukan sekadar "nge-hit API terus tampilkan").
- G3: Berjalan sepenuhnya di dalam budget free tier CoinMarketCap (15.000 credits/bulan) dan free tier hosting (Vercel + Upstash Redis).
- G4: Cocok dipakai orang yang masih belajar domain crypto — istilah-istilah pasar (market cap, volume, dominance, Fear & Greed, dst) dijelaskan langsung di UI, bukan diasumsikan sudah dipahami user.

## 3. Non-Goals (Sengaja Tidak Dikerjakan Dulu)

- **Bukan** platform trading — tidak ada eksekusi order, tidak connect ke exchange account user.
- **Bukan** portfolio tracker pribadi (tidak menyimpan wallet/holding user) di v1 — ini murni market intelligence dashboard, bukan personal finance app.
- **Bukan** on-chain analytics (whale tracking, gas fee, dsb) di v1 — di luar cakupan free tier CMC dan menambah kompleksitas domain yang belum dikuasai.
- Historical multi-year charting **tidak** pakai endpoint historical CMC (karena berbayar) — akan disiasati dengan self-collected time series (lihat `architecture.md` §5).

## 4. Target User

Persona utama: **Vincent sendiri** — seorang data/analytics engineer, pemula di domain crypto, ingin dashboard yang membantu memahami market sambil belajar istilah-istilahnya. Persona sekunder: recruiter/klien yang melihat repo ini sebagai bukti kemampuan teknis.

Implikasi desain:
- UI perlu punya micro-explanation (tooltip/info icon) di setiap metrik non-trivial (market cap, dominance, FGI, dsb).
- Prioritaskan clarity di atas kepadatan data ala terminal trading profesional (Bloomberg-style bukan target).

## 5. Fitur — Prioritas & Cakupan

### 5.1 Must-have (v1)

| # | Fitur | Deskripsi |
|---|---|---|
| F1 | **Market Overview** | Total market cap, total volume 24h, BTC dominance, ETH dominance, jumlah koin aktif — dari `/global-metrics/quotes/latest`. |
| F2 | **Fear & Greed Index** | Gauge visual 0–100 + label (Extreme Fear → Extreme Greed), dari `/fear-and-greed/latest`. |
| F3 | **Top Coins Table** | Tabel top 100 coin: rank, nama, harga, %change 1h/24h/7d, market cap, volume 24h. Sortable & searchable. Dari `/cryptocurrency/listings/latest`. |
| F4 | **Coin Detail Panel** | Klik satu coin → detail: harga, supply (circulating/total/max), ATH-ish stats yang tersedia, deskripsi singkat dari `/cryptocurrency/info`. |
| F5 | **Top Gainers & Losers** | Turunan dari data listings (dihitung sendiri di backend, bukan endpoint khusus karena itu masuk fitur berbayar) — top 5 gainer & loser 24h dari 100 coin yang di-track. |
| F6 | **Self-collected Mini Chart** | Sparkline harga berdasarkan snapshot yang kita simpan sendiri di cache tiap interval (bukan historical API CMC yang berbayar). Coverage terbatas sejak dashboard mulai jalan. |
| F7 | **Watchlist (local, tanpa akun)** | User pin beberapa coin favorit, disimpan di localStorage — bukan fitur server-side dulu di v1. |
| F8 | **Last Updated Indicator** | Setiap section jelas menampilkan kapan data terakhir di-refresh (karena ini bukan real-time murni, harus transparan ke user). |

### 5.2 Nice-to-have (v1.x, kalau budget credit & waktu ada)

| # | Fitur | Catatan |
|---|---|---|
| F9 | Convert currency (USD/IDR) | `convert` param CMC bisa langsung IDR — cek credit cost. |
| F10 | Altcoin Season Index | Endpoint keyless CMC indicator tambahan. |
| F11 | CMC100 Index card | Indicator tambahan dari keyless set. |
| F12 | Dark/Light theme toggle | shadcn/ui sudah support, tinggal wiring. |
| F13 | Exchange leaderboard | Dari `/exchange/*` — ranking exchange by volume. |

### 5.3 Out of scope (dicoret sadar, bukan lupa)

- Trading/order execution
- Wallet connect / on-chain wallet tracking
- Push notification / price alert (butuh backend job + user accounts, kompleksitas naik signifikan)
- Multi-user auth (v1 = single-user personal dashboard)

## 6. Update Frequency Data (ringkasan — detail di `architecture.md`)

Karena Basic/Free tier CMC **tidak** refresh per detik dan dibatasi credit bulanan, update frequency dirancang berjenjang berdasarkan seberapa cepat data itu berubah secara meaningful dan seberapa mahal endpoint-nya:

- Global metrics & Fear/Greed: tiap **15–30 menit** (data-nya sendiri di sisi CMC juga update ~15 menit).
- Top 100 listings: tiap **5–10 menit**.
- Coin detail/info (metadata jarang berubah): cache **24 jam**, refresh manual/on-demand kalau perlu.

## 7. Metrik Sukses

- Dashboard bisa jalan 30 hari penuh tanpa melebihi 15.000 credits CMC (lihat perhitungan budget di `architecture.md`).
- Time-to-first-render < 1.5 detik dari cache (bukan nunggu API CMC tiap kali user buka halaman).
- Repo commit history mencerminkan kerja aktual — tidak ada auto-commit dari cron data fetch.
- Bisa dipakai orang yang belum tahu apa itu "market cap" tanpa harus buka Google dulu (tooltip mencukupi).

## 8. Risiko & Mitigasi

| Risiko | Mitigasi |
|---|---|
| Kehabisan credit bulanan CMC | Rate budget dihitung eksplisit + circuit breaker di backend kalau mendekati limit (lihat `rules.md`). |
| Free tier Upstash/Vercel KV limit terlampaui | Simpan data seminimal mungkin (hanya snapshot terbaru + rolling window kecil untuk sparkline), TTL agresif. |
| Data historical terbatas (baru mulai dari kapan dashboard live) | Jelas dikomunikasikan di UI ("chart 7 hari terakhir sejak dashboard aktif"), bukan dipalsukan. |
| Scope creep ke trading platform | Non-goals ditulis eksplisit di dokumen ini, direview tiap mau nambah fitur besar. |
