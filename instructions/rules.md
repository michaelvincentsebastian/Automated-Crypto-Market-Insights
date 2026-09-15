# Rules — Crypto Market Dashboard (Revamp)

Aturan teknis & konvensi kerja untuk project ini. Dokumen ini yang paling sering dirujuk saat implementasi — pelanggaran terhadap aturan di sini adalah hal pertama yang dicek kalau ada bug atau biaya API meledak.

**Terakhir diperbarui**: 15 September 2026

---

## 1. Aturan "Zero-Copy" (Wajib)

1. **Dilarang** menulis data mentah dari CoinMarketCap API ke file (CSV/JSON di disk/git-tracked) sebagai representasi utama. Satu-satunya representasi persisten adalah Redis.
2. Transform data dari API → bentuk cache **hanya boleh terjadi sekali**, di dalam fungsi ingest. Endpoint yang serve frontend **tidak boleh** transform ulang dari raw API — ia hanya baca apa yang sudah ada di Redis.
3. Kalau butuh reprocessing (misal ubah struktur cache), jalankan lewat script ingest ulang, bukan dengan menambah tahap transform baru di jalur baca.

## 2. Aturan Cron & Git (Wajib — ini akar masalah versi lama)

1. Scheduler untuk fetch data **hanya** lewat Vercel Cron (`vercel.json`), **dilarang** pakai GitHub Actions untuk tugas berulang yang menghasilkan commit otomatis.
2. Tidak ada commit otomatis dari sistem manapun. Setiap commit di repo ini harus merepresentasikan perubahan kode/dokumentasi yang dibuat manusia (kamu), bukan hasil job terjadwal.
3. Kalau butuh audit trail data historis, itu tanggung jawab Redis (rolling window) atau logging platform (Vercel logs), **bukan** Git.

## 3. Aturan Budget Credit CMC (Wajib)

1. Setiap kali endpoint ingest berhasil manggil CMC API, **wajib** increment `meta:credits:used:{YYYY-MM}` di Redis sesuai `credit_count` dari response CMC (jangan asumsikan selalu 1 — baca dari field response).
2. Sebelum melakukan panggilan API, endpoint ingest **wajib** cek counter tersebut. Kalau sudah ≥ 90% dari 15.000 (yaitu 13.500), **hentikan** semua ingest sampai bulan berikutnya — jangan menunggu sampai API benar-benar menolak request.
3. Saat circuit breaker aktif, dashboard tetap harus bisa diakses (serve dari cache terakhir), dengan `meta.stale = true` dan banner yang jujur ke user, bukan silent failure.
4. Interval refresh yang didefinisikan di `architecture.md` §6 adalah **maksimum frequency**, bukan target — kalau ada cara memenuhi kebutuhan user dengan interval lebih jarang, pilih yang lebih jarang.

## 4. Aturan Keamanan

1. `CMC_API_KEY` hanya boleh dibaca di server-side (serverless function environment variable). **Dilarang keras** expose ke bundle frontend dalam bentuk apapun (termasuk lewat `NEXT_PUBLIC_*`/`VITE_*` prefix atau sejenisnya).
2. Endpoint internal yang serve frontend (`/api/dashboard/*`) **read-only** dari sisi client — tidak ada endpoint yang menerima input dari client dan meneruskannya ke CMC API (mencegah client jadi proxy bebas yang bisa disalahgunakan orang lain untuk boros credit kamu).
3. `.env*` selalu di `.gitignore`. Secrets hanya lewat Vercel Environment Variables dashboard.

## 5. Aturan Struktur Repo

```
crypto-dashboard/
├── src/                    # React app (Vite)
│   ├── components/         # komponen shadcn/ui + custom
│   ├── features/           # per-fitur: overview, coin-detail, watchlist
│   ├── lib/                # query client, format helpers, dsb
│   └── ...
├── api/                     # Vercel Serverless Functions
│   ├── ingest/              # dipanggil cron — fetch CMC → tulis Redis
│   │   ├── global.ts
│   │   ├── listings.ts
│   │   └── feargreed.ts
│   ├── dashboard/            # dipanggil frontend — baca Redis
│   │   ├── overview.ts
│   │   └── coin/[id].ts
│   └── health.ts
├── vercel.json               # definisi cron schedule
├── .env.example               # dokumentasi env var yang dibutuhkan (tanpa value asli)
└── README.md
```

Tidak ada folder `data/`, `csv/`, atau `*.db` yang di-commit — kalau muncul, itu tanda ada pelanggaran aturan §1.

## 6. Aturan Penamaan & Konsistensi

1. Semua key Redis pakai format `namespace:resource:qualifier` (lihat contoh lengkap di `schema.md` §2) — konsisten, lowercase, dipisah `:`.
2. Semua response dari `/api/dashboard/*` pakai camelCase (konsisten dengan konvensi JS/TS), **bukan** snake_case mentah dari CMC — transform field name terjadi sekali di ingest layer.
3. Setiap response dari `/api/dashboard/*` **wajib** menyertakan `meta.lastRefreshedAt` dan `meta.stale` — tidak ada endpoint yang mengembalikan data tanpa konteks kesegarannya.

## 7. Aturan Development Workflow

1. Development lokal pakai CMC API key sendiri dengan interval fetch yang **lebih jarang** dari production (misal manual trigger, bukan cron aktif) supaya tidak dobel-pakai credit dari environment dev + prod.
2. Sebelum merge fitur baru yang menambah panggilan API baru, **wajib** update tabel budget di `architecture.md` §6 supaya total tetap terlacak.
3. Preview deployment (branch/PR di Vercel) **tidak** menjalankan cron ingest — hanya baca cache production/staging yang sudah ada, supaya testing UI tidak butuh trigger fetch API sungguhan berulang kali.

## 8. Aturan Konten & Kejujuran Data (khusus dashboard finansial)

1. **Dilarang** menampilkan data seolah real-time kalau sebenarnya cache berumur belasan menit — selalu tampilkan `lastRefreshedAt` di UI (bukan cuma di response API).
2. **Dilarang** memalsukan atau mengekstrapolasi data historis untuk mengisi rentang waktu sebelum dashboard live — chart menunjukkan apa adanya, sependek apapun datanya (lihat `architecture.md` §5 dan `prd.md` §7).
3. Istilah domain crypto yang ditampilkan ke user (Fear & Greed classification, dominance, dsb) harus pakai definisi resmi dari sumbernya (CMC), tidak diparafrase bebas yang berisiko mengubah makna — terutama karena target user (kamu sendiri) juga sedang belajar dari dashboard ini.
