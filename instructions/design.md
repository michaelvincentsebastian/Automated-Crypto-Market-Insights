# Design — Crypto Market Dashboard (Revamp)

**Terakhir diperbarui**: 15 September 2026

---

## 1. Alur Kerja Desain

1. **Google Stitch** dipakai di tahap awal untuk eksplorasi layout & mood — hasilnya berupa prototype visual/screenshot, bukan kode final.
2. Prototype dari Stitch diterjemahkan manual ke komponen **shadcn/ui** (bukan copy langsung style dari Stitch) supaya tetap konsisten dengan design token Tailwind yang dipakai di seluruh app.
3. Setiap layout baru dicek dulu terhadap prinsip §2–§4 di bawah sebelum dibangun jadi komponen React.

## 2. Prinsip Visual — Tema Crypto

- **Base**: dark theme sebagai default (konvensi umum dashboard finansial/crypto — kontras tinggi, enak dipakai lama, dan data numerik lebih "pop" di background gelap). Light theme sebagai toggle opsional (F12 di PRD), bukan prioritas v1.
- **Warna semantik** (paling penting di dashboard crypto — jangan sampai ambigu):
  - Hijau → kenaikan harga / sentiment positif (Fear & Greed "Greed")
  - Merah → penurunan harga / sentiment negatif ("Fear")
  - Netral (abu-abu/biru muda) → data yang tidak menyiratkan naik/turun (nama coin, volume, dsb)
  - **Konsisten di seluruh app** — jangan hijau berarti "naik" di satu tempat dan "positif secara umum" di tempat lain.
- **Aksen warna brand**: satu warna aksen non-merah/hijau (misal ungu/biru elektrik) untuk elemen interaktif (button, link, active state) — supaya tidak bentrok makna dengan hijau/merah harga.
- **Tipografi**: font monospace atau tabular-nums untuk semua angka (harga, persentase, market cap) supaya kolom di tabel rapi sejajar — ini detail kecil yang bikin dashboard finansial terasa "profesional" vs terasa amatir.
- **Densitas informasi**: sedang — bukan tabel padat ala Bloomberg Terminal (target user pemula), tapi juga bukan card kosong ala consumer app. Tabel tetap jadi elemen utama untuk listing coin.

## 3. Struktur Halaman (v1)

### 3.1 Landing / Overview (halaman utama)

```
┌─────────────────────────────────────────────────────┐
│ Header: judul, last-updated indicator, theme toggle  │
├─────────────────┬─────────────────┬─────────────────┤
│ Total Market Cap │ Total Vol 24h    │ BTC Dominance   │  ← 3-4 stat cards
├─────────────────┴─────────────────┴─────────────────┤
│ Fear & Greed Gauge          │ Top Gainers/Losers      │  ← 2 kolom
│ (visual gauge + label)      │ (mini list 5+5)         │
├───────────────────────────────────────────────────────┤
│ Top 100 Coins Table                                    │
│ (search, sort by column, klik row → buka detail panel) │
└───────────────────────────────────────────────────────┘
```

### 3.2 Coin Detail (panel/drawer, bukan halaman terpisah — biar konteks list tidak hilang)

- Header: logo, nama, simbol, rank
- Harga besar + %change 24h (warna semantik)
- Sparkline mini (self-collected data, lihat `architecture.md` §5)
- Grid stat: circulating supply, max supply, market cap, volume 24h, FDV kalau tersedia
- Deskripsi singkat coin (dari `/cryptocurrency/info`)
- Tombol "Pin ke Watchlist" (localStorage)

### 3.3 Watchlist (section di dalam Overview atau tab terpisah)

- List coin yang di-pin user, versi ringkas dari row tabel utama
- Kalau kosong: empty state yang mengarahkan user pin coin dari tabel utama

## 4. Edukasi Domain (Khusus untuk User Pemula)

Karena kamu sendiri masih belajar domain crypto, UI ini didesain supaya **mengajar sambil dipakai**:

- Setiap istilah non-trivial (market cap, circulating supply, dominance, Fear & Greed Index, FDV) punya **info icon kecil** di sebelah label → hover/tap menampilkan tooltip 1-2 kalimat penjelasan, bukan link keluar.
- Fear & Greed gauge menampilkan label kata (Extreme Fear/Fear/Neutral/Greed/Extreme Greed), bukan cuma angka — supaya langsung dimengerti tanpa perlu tahu skalanya.
- Warna hijau/merah dipakai konsisten sebagai bahasa visual utama, jadi meski user belum paham angka persisnya, arah "baik/buruk" langsung kebaca.

## 5. Pemetaan ke Komponen shadcn/ui

| Elemen UI | Komponen shadcn/ui |
|---|---|
| Stat cards (market cap, volume, dominance) | `Card` |
| Top 100 table | `Table` + custom sorting logic, `Input` untuk search |
| Coin detail | `Sheet` (drawer dari kanan) atau `Dialog` |
| Fear & Greed gauge | Custom SVG/Recharts di dalam `Card` |
| Tooltip edukasi | `Tooltip` / `HoverCard` |
| Watchlist pin button | `Button` (variant ghost/icon) + `Toast` untuk konfirmasi |
| Theme toggle | `DropdownMenu` atau `Switch` |
| Loading state | `Skeleton` (penting — dashboard ini cache-based, jadi ada jeda antara render awal dan data siap) |
| Last-updated / stale data banner | `Alert` |

## 6. Responsive

- Desktop-first (dashboard finansial lebih sering dibuka di layar besar), tapi tabel top coins harus tetap bisa dipakai di mobile — strategi: di breakpoint kecil, beberapa kolom (misal %change 1h) disembunyikan dulu, hanya rank/nama/harga/%change 24h yang wajib tampil.
- Stat cards di overview: grid 3-4 kolom di desktop → stack 1-2 kolom di mobile.

## 7. Yang Sengaja Tidak Ditiru dari TradingView-style Dashboard

Versi lama pakai TradingView embed untuk technical analysis chart. Di v1 revamp ini **sengaja tidak dipakai dulu** karena:
- Menambah dependency eksternal berat untuk fitur yang belum tentu dipakai (technical analysis butuh pemahaman domain yang kamu sendiri masih bangun).
- Sparkline sederhana dari data sendiri sudah cukup untuk tujuan "market overview", sesuai scope PRD.
- Bisa jadi nice-to-have masa depan kalau memang dibutuhkan.
