export interface GlossaryTerm {
  title: string;
  shortDesc: string;
  detail: string;
}

export const GLOSSARY: Record<string, GlossaryTerm> = {
  marketCap: {
    title: "Market Capitalization (Kapitalisasi Pasar)",
    shortDesc: "Nilai total seluruh koin yang beredar saat ini (Harga × Circulating Supply).",
    detail: "Metrik utama untuk mengukur ukuran relatif dan valuasi keseluruhan suatu aset crypto dibandingkan aset lainnya.",
  },
  volume24h: {
    title: "24-Hour Trading Volume",
    shortDesc: "Total nilai dollar dari transaksi jual-beli koin ini dalam 24 jam terakhir.",
    detail: "Menunjukkan likuiditas dan aktivitas pasar. Volume tinggi biasanya menandakan minat pasar yang kuat dan spread bid-ask yang lebih ketat.",
  },
  btcDominance: {
    title: "Bitcoin Dominance (BTC.D)",
    shortDesc: "Persentase pangsa pasar Bitcoin terhadap total kapitalisasi pasar crypto global.",
    detail: "Jika BTC Dominance naik, modal cenderung mengalir ke Bitcoin. Jika turun sementara pasar naik, sering disebut awal 'Altcoin Season'.",
  },
  ethDominance: {
    title: "Ethereum Dominance (ETH.D)",
    shortDesc: "Persentase pangsa pasar Ethereum terhadap total seluruh industri crypto.",
    detail: "Menggambarkan kekuatan ekosistem smart contract terbesar dunia di luar Bitcoin.",
  },
  fearGreedIndex: {
    title: "Fear & Greed Index",
    shortDesc: "Indeks sentimen pasar dari 0 (Ketakutan Ekstrem) hingga 100 (Keserakahan Ekstrem).",
    detail: "Dihitung dari volatilitas pasar, volume trading, media sosial, dan tren pencarian. Ketakutan ekstrem sering kali menandakan peluang beli akibat oversold.",
  },
  circulatingSupply: {
    title: "Circulating Supply (Pasokan Beredar)",
    shortDesc: "Jumlah koin yang saat ini beredar dan dapat diperjualbelikan oleh publik.",
    detail: "Berbeda dengan total supply, ini hanya menghitung koin yang benar-benar ada di tangan pasar dan tidak terkunci/terbakar.",
  },
  maxSupply: {
    title: "Max Supply (Pasokan Maksimum)",
    shortDesc: "Batas mutlak jumlah koin yang akan pernah dibuat sepanjang sejarah blockchain tersebut.",
    detail: "Contohnya Bitcoin memiliki batas maksimum 21 juta koin. Koin tanpa batasan pasokan (seperti Ethereum) bersifat tanpa batas maksimum terprogram.",
  },
  fdv: {
    title: "Fully Diluted Valuation (FDV)",
    shortDesc: "Estimasi kapitalisasi pasar jika seluruh pasokan maksimum koin sudah beredar (Harga × Max Supply).",
    detail: "Penting untuk mendeteksi potensi inflasi harga jika sebagian besar koin masih terkunci dan akan di-unlock ke pasar di masa depan.",
  },
  percentChange: {
    title: "Price Change (1h / 24h / 7d)",
    shortDesc: "Persentase kenaikan atau penurunan harga dalam rentang waktu 1 jam, 24 jam, atau 7 hari terakhir.",
    detail: "Warna hijau menandakan tren apresiasi harga, sedangkan warna merah menandakan penurunan harga.",
  },
};
