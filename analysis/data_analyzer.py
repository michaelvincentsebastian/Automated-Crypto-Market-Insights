import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime, timedelta

# --- 0. Muat Variabel Lingkungan ---
load_dotenv()

# --- Konfigurasi ---
# Database untuk data mentah
RAW_DATABASE_URL = os.getenv('database-url')
# Database untuk data yang sudah dibersihkan dan analisis
# Kita akan gunakan nama file DB yang berbeda
CLEANED_DATABASE_PATH = "./analysis/crypto_analysis.db"
CLEANED_DATABASE_URL = f"sqlite:///{CLEANED_DATABASE_PATH}"

# Ambil timezone dari environment variable
TIMEZONE = os.getenv('TIMEZONE', 'Asia/Jakarta')

if not RAW_DATABASE_URL:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: DATABASE_URL (raw) tidak ditemukan di file .env. Harap tambahkan.")
    exit()

# Inisialisasi SQLAlchemy Engine untuk RAW database
try:
    raw_engine = create_engine(RAW_DATABASE_URL)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Koneksi RAW database siap.")
except Exception as e:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Gagal membuat engine RAW database: {e}")
    exit()

# Inisialisasi SQLAlchemy Engine untuk CLEANED database
try:
    # Pastikan direktori database ada
    os.makedirs(os.path.dirname(CLEANED_DATABASE_PATH), exist_ok=True)
    cleaned_engine = create_engine(CLEANED_DATABASE_URL)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Koneksi CLEANED database siap.")
except Exception as e:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Gagal membuat engine CLEANED database: {e}")
    exit()

def transfer_raw_to_cleaned_db():
    """
    Mengambil data dari RAW DB dan menyimpannya ke CLEANED DB.
    Ini bisa dilakukan dengan overwrite atau append jika ada Primary Key.
    Untuk kesederhanaan, kita akan mengambil semua dari RAW dan menulis ulang ke CLEANED.
    """
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Memulai transfer data dari RAW ke CLEANED database...")
    try:
        # 1. Ambil data dari RAW DB
        with raw_engine.connect() as raw_conn:
            raw_df = pd.read_sql("SELECT * FROM crypto_prices;", raw_conn)
            print(f"  - Berhasil mengambil {len(raw_df)} baris data dari RAW database.")

        if raw_df.empty:
            print("  - RAW database kosong, tidak ada data untuk ditransfer.")
            return

        # 2. Simpan data ke CLEANED DB
        # Pastikan tabel 'crypto_prices_cleaned' (atau nama lain) dibuat
        # Kita bisa membuat skema tabel yang sama atau membiarkan pandas menentukannya
        with cleaned_engine.connect() as cleaned_conn:
            # Drop tabel lama jika ada dan buat yang baru untuk memastikan data selalu fresh
            # Ini adalah pendekatan sederhana untuk "refresh" data di DB cleaned
            try:
                cleaned_conn.execute(text("DROP TABLE IF EXISTS crypto_prices_cleaned;"))
                cleaned_conn.commit()
                print("  - Tabel 'crypto_prices_cleaned' di CLEANED database berhasil dihapus (jika ada).")
            except Exception as e:
                print(f"  - Peringatan: Gagal menghapus tabel 'crypto_prices_cleaned' (mungkin tidak ada): {e}")

            # Simpan DataFrame ke tabel baru di CLEANED DB
            # Pastikan nama tabel di sini konsisten
            raw_df.to_sql('crypto_prices_cleaned', cleaned_conn, if_exists='append', index=False)
            cleaned_conn.commit()
            print(f"  - Berhasil menulis {len(raw_df)} baris data ke tabel 'crypto_prices_cleaned' di CLEANED database.")
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Transfer data selesai.")

    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Gagal transfer data dari RAW ke CLEANED database: {e}")


def get_data_from_cleaned_db():
    """Mengambil data dari tabel yang sudah ditransfer di CLEANED database."""
    try:
        with cleaned_engine.connect() as connection:
            # Ambil dari tabel yang sudah disalin
            query = "SELECT * FROM crypto_prices_cleaned ORDER BY timestamp ASC;"
            df = pd.read_sql(query, connection)
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Berhasil mengambil {len(df)} baris data dari CLEANED database.")
            return df
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Gagal mengambil data dari CLEANED database: {e}")
        return pd.DataFrame() # Mengembalikan DataFrame kosong jika ada error

def clean_and_prepare_data(df):
    """
    Melakukan cleaning dan persiapan data untuk analisis.
    Fungsi ini sekarang akan beroperasi pada DataFrame yang diambil dari CLEANED DB.
    """
    if df.empty:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] DataFrame kosong, tidak ada data untuk di-clean.")
        return pd.DataFrame()

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Memulai proses data cleaning dan persiapan...")

    # 1. Konversi Kolom Datetime
    # `last_updated` dari API (UTC) dan `timestamp` dari sistem Anda
    try:
        df['last_updated'] = pd.to_datetime(df['last_updated'], errors='coerce', utc=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
        # Konversi ke timezone lokal (misal: Asia/Jakarta) untuk visualisasi yang lebih mudah dipahami audiens
        df['last_updated_local'] = df['last_updated'].dt.tz_convert(TIMEZONE)
        df['timestamp_local'] = df['timestamp'].dt.tz_convert(TIMEZONE)
        print("  - Konversi kolom datetime berhasil.")
    except Exception as e:
        print(f"  - ERROR konversi datetime: {e}")
        # Lanjutkan meskipun ada error, tapi kolom datetime mungkin None/NaT

    # 2. Konversi Kolom Numerik
    numeric_cols = [
        'price', 'volume_24h', 'market_cap',
        'percent_change_1h', 'percent_change_24h', 'percent_change_7d'
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce') # 'coerce' akan mengubah nilai non-numerik menjadi NaN
    print("  - Konversi kolom numerik berhasil.")

    # 3. Penanganan Nilai Hilang (NaN/NaT)
    initial_rows = len(df)
    # Hapus baris jika identitas utama koin (id, name, symbol) atau timestamp hilang
    # Ini sangat penting karena tanpa ini, baris tidak berguna
    df.dropna(subset=['id', 'name', 'symbol', 'timestamp'], inplace=True) 
    if len(df) < initial_rows:
        print(f"  - Menghapus {initial_rows - len(df)} baris dengan ID/Nama/Timestamp hilang.")

    # Anda bisa memutuskan untuk mengisi NaN di kolom numerik dengan 0 atau median
    # Untuk percent_change, NaN bisa berarti tidak ada perubahan yang signifikan atau data tidak tersedia,
    # jadi membiarkannya NaN atau mengisi dengan 0 (jika memang 0) perlu pertimbangan
    # Contoh: df['volume_24h'].fillna(0, inplace=True)

    print("  - Penanganan nilai hilang dasar selesai.")

    # 4. Penanganan Duplikasi
    # Jika ada entri yang sama persis untuk ID dan TIMESTAMP (duplikasi tidak disengaja), ambil yang terakhir (paling baru)
    # Ini penting jika ada masalah dalam proses pengumpulan data yang menghasilkan duplikat identik
    df.sort_values(by=['timestamp', 'id'], ascending=True, inplace=True)
    df.drop_duplicates(subset=['id', 'timestamp'], keep='last', inplace=True)
    print("  - Penanganan duplikasi (id, timestamp) selesai.")

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Proses data cleaning dan persiapan selesai. Total baris setelah cleaning: {len(df)}")
    return df

def analyze_and_get_insights(df):
    """
    Melakukan analisis data untuk mendapatkan insight bermakna.
    """
    if df.empty:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Tidak ada data bersih untuk dianalisis.")
        return {}

    insights = {}
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Memulai analisis data...")

    # --- Ambil snapshot data terakhir ---
    # Pastikan data terakhir valid dan lengkap
    if 'timestamp_local' not in df.columns or df['timestamp_local'].isnull().all():
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Peringatan: Kolom timestamp_local tidak valid. Analisis mungkin tidak akurat.")
        return {} # Kembali jika timestamp tidak valid

    latest_timestamp = df['timestamp_local'].max()
    current_market_data = df[df['timestamp_local'] == latest_timestamp].copy()

    if current_market_data.empty:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Peringatan: Data terakhir kosong setelah filtering.")
        return {}

    # --- Insight 1: Gambaran Pasar Terkini ---
    total_market_cap = current_market_data['market_cap'].sum()
    total_volume_24h = current_market_data['volume_24h'].sum()

    insights['market_overview'] = {
        'latest_timestamp': latest_timestamp,
        'total_coins_tracked': len(current_market_data),
        'total_market_cap_usd': total_market_cap,
        'total_volume_24h_usd': total_volume_24h
    }

    # Top 5 Koin Berdasarkan Market Cap
    top_5_market_cap = current_market_data.sort_values(by='market_cap', ascending=False).head(5)
    insights['top_5_by_market_cap'] = top_5_market_cap[['name', 'symbol', 'market_cap', 'price', 'percent_change_24h']].to_dict(orient='records')

    # Top 5 Koin Berdasarkan Volume 24 Jam
    top_5_volume = current_market_data.sort_values(by='volume_24h', ascending=False).head(5)
    insights['top_5_by_volume_24h'] = top_5_volume[['name', 'symbol', 'volume_24h', 'price']].to_dict(orient='records')

    # Top 5 Gainers (24 jam) - pastikan percent_change_24h tidak NaN
    top_5_gainers_24h = current_market_data.dropna(subset=['percent_change_24h']).sort_values(by='percent_change_24h', ascending=False).head(5)
    insights['top_5_gainers_24h'] = top_5_gainers_24h[['name', 'symbol', 'price', 'percent_change_24h']].to_dict(orient='records')

    # Top 5 Losers (24 jam) - pastikan percent_change_24h tidak NaN
    top_5_losers_24h = current_market_data.dropna(subset=['percent_change_24h']).sort_values(by='percent_change_24h', ascending=True).head(5)
    insights['top_5_losers_24h'] = top_5_losers_24h[['name', 'symbol', 'price', 'percent_change_24h']].to_dict(orient='records')


    # --- Insight 2: Dominasi Pasar ---
    btc_data = current_market_data[current_market_data['symbol'] == 'BTC']
    eth_data = current_market_data[current_market_data['symbol'] == 'ETH']

    btc_market_cap = btc_data['market_cap'].iloc[0] if not btc_data.empty else 0
    eth_market_cap = eth_data['market_cap'].iloc[0] if not eth_data.empty else 0
    
    # Hitung Other Altcoins Market Cap hanya jika total_market_cap > 0
    other_altcoins_market_cap = max(0, total_market_cap - btc_market_cap - eth_market_cap)

    insights['market_dominance'] = {
        'btc_dominance': (btc_market_cap / total_market_cap) * 100 if total_market_cap else 0,
        'eth_dominance': (eth_market_cap / total_market_cap) * 100 if total_market_cap else 0,
        'other_altcoins_dominance': (other_altcoins_market_cap / total_market_cap) * 100 if total_market_cap else 0,
    }

    # --- Insight 3: Pergerakan Harga Koin Utama (Contoh: Bitcoin & Ethereum dalam 24 jam terakhir) ---
    # Ambil data dari 24 jam terakhir dari timestamp_local
    if latest_timestamp:
        time_window_start = latest_timestamp - timedelta(days=1)
        # Filter df yang sudah di-clean
        recent_data = df[(df['timestamp_local'] >= time_window_start) & (df['timestamp_local'] <= latest_timestamp)].copy()

        btc_recent_prices = recent_data[recent_data['symbol'] == 'BTC'][['timestamp_local', 'price']].sort_values('timestamp_local')
        eth_recent_prices = recent_data[recent_data['symbol'] == 'ETH'][['timestamp_local', 'price']].sort_values('timestamp_local')

        insights['btc_recent_prices'] = btc_recent_prices.to_dict(orient='records')
        insights['eth_recent_prices'] = eth_recent_prices.to_dict(orient='records')
    else:
        insights['btc_recent_prices'] = []
        insights['eth_recent_prices'] = []
    
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Analisis data selesai.")
    return insights

def format_insights_for_awam(insights):
    """
    Memformat insight agar mudah dipahami oleh audiens awam.
    """
    if not insights:
        return "Tidak ada wawasan yang dapat ditampilkan saat ini. Pastikan data tersedia."

    narration = []

    narration.append("--- Ringkasan Pasar Kripto Terkini ---")
    market_overview = insights.get('market_overview')
    if market_overview and market_overview['total_coins_tracked'] > 0:
        latest_ts_str = market_overview['latest_timestamp'].strftime('%d %B %Y %H:%M:%S %Z') if market_overview['latest_timestamp'] else 'Tidak diketahui'
        narration.append(f"Data pasar terakhir diperbarui pada: **{latest_ts_str}**.")
        narration.append(f"Saat ini, kami memantau {market_overview['total_coins_tracked']} aset kripto teratas.")
        narration.append(f"Total nilai pasar (kapitalisasi pasar) semua aset ini mencapai **${market_overview['total_market_cap_usd']:,.2f}**, dengan total volume perdagangan dalam 24 jam terakhir sebesar **${market_overview['total_volume_24h_usd']:,.2f}**.")
        narration.append("Ini adalah gambaran seberapa besar dan aktifnya pasar kripto secara keseluruhan.")
    else:
        narration.append("Tidak ada data pasar terkini yang ditemukan.")

    # Tambahan checks for empty lists/dicts before trying to format
    gainers = insights.get('top_5_gainers_24h')
    if gainers:
        narration.append("\n--- Top Performers (24 Jam Terakhir) ---")
        narration.append("Berikut adalah aset kripto yang menunjukkan peningkatan harga paling signifikan dalam 24 jam terakhir:")
        for i, coin in enumerate(gainers):
            narration.append(f"{i+1}. **{coin['name']} ({coin['symbol']})**: Harga sekarang ${coin['price']:,.4f}, naik **{coin['percent_change_24h']:.2f}%**.")
        narration.append("Koin-koin ini mungkin sedang menarik banyak perhatian atau memiliki berita positif.")
    
    losers = insights.get('top_5_losers_24h')
    if losers:
        narration.append("\n--- Top Laggards (24 Jam Terakhir) ---")
        narration.append("Di sisi lain, beberapa aset kripto mengalami penurunan harga yang cukup dalam dalam 24 jam terakhir:")
        for i, coin in enumerate(losers):
            narration.append(f"{i+1}. **{coin['name']} ({coin['symbol']})**: Harga sekarang ${coin['price']:,.4f}, turun **{coin['percent_change_24h']:.2f}%**.")
        narration.append("Penurunan ini bisa disebabkan oleh sentimen pasar negatif atau isu spesifik koin tersebut.")

    dominance = insights.get('market_dominance')
    if dominance:
        narration.append("\n--- Dominasi Pasar Kripto ---")
        narration.append("Pasar kripto seringkali dipimpin oleh aset-aset terbesar. Berikut adalah seberapa besar pangsa pasar aset-aset utama:")
        narration.append(f"- **Bitcoin (BTC)**: Menguasai sekitar **{dominance['btc_dominance']:.2f}%** dari total nilai pasar. Bitcoin sering menjadi 'indikator' utama sentimen pasar kripto secara keseluruhan.")
        narration.append(f"- **Ethereum (ETH)**: Menguasai sekitar **{dominance['eth_dominance']:.2f}%** dari total nilai pasar. Ethereum adalah platform penting untuk banyak aplikasi kripto (DeFi, NFT).")
        narration.append(f"- **Altcoin Lainnya**: Sisanya (sekitar **{dominance['other_altcoins_dominance']:.2f}%**) adalah gabungan dari ratusan altcoin lain yang kita pantau.")
        narration.append("Perhatikan bagaimana dominasi ini bergeser; itu bisa menunjukkan perubahan minat investor dari aset besar ke aset yang lebih kecil (dan sebaliknya).")

    narration.append("\n--- Pergerakan Harga Koin Utama (24 Jam Terakhir) ---")
    narration.append("Mari kita lihat lebih dekat bagaimana harga Bitcoin dan Ethereum telah bergerak dalam 24 jam terakhir. Ini penting karena pergerakan mereka sering mempengaruhi seluruh pasar:")
    narration.append("*(Visualisasi grafik pergerakan harga BTC dan ETH akan ditampilkan di sini di dashboard)*")

    narration.append("\n--- Kata Penutup ---")
    narration.append("Analisis ini memberikan gambaran sekilas tentang pasar kripto. Selalu ingat bahwa pasar kripto sangat volatil dan berisiko tinggi. Keputusan investasi harus didasarkan pada riset mendalam dan pemahaman risiko pribadi.")

    return "\n\n".join(narration)


if __name__ == "__main__":
    # Bagian ini akan dijalankan saat Anda menjalankan data_analyzer.py secara mandiri.
    # Ini akan melakukan transfer, cleaning, dan analisis.

    # 1. Transfer data dari RAW ke CLEANED database
    transfer_raw_to_cleaned_db()

    # 2. Ambil data dari CLEANED database
    crypto_df_cleaned = get_data_from_cleaned_db()
    
    # 3. Lakukan proses cleaning dan persiapan pada data yang diambil dari CLEANED DB
    processed_df = clean_and_prepare_data(crypto_df_cleaned)

    if not processed_df.empty:
        print("\n--- OVERVIEW DATA BERSIH AKHIR (dari crypto_analysis.db) ---")
        processed_df.info()
        print("\nHead of Cleaned Data:")
        print(processed_df.head())
        print("\nTail of Cleaned Data:")
        print(processed_df.tail())
        print("\nJumlah nilai hilang setelah cleaning:\n", processed_df.isnull().sum())
        
        # 4. Lakukan analisis dan dapatkan insight
        insights_data = analyze_and_get_insights(processed_df)

        # 5. Format insight untuk audiens awam
        formatted_narration = format_insights_for_awam(insights_data)
        print("\n--- NARASI ANALISIS UNTUK AUDIENS AWAM ---")
        print(formatted_narration)
    else:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Tidak ada data yang berhasil di-clean untuk analisis.")