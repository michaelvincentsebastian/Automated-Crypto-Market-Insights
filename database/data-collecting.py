import pandas as pd
import json
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from datetime import datetime
import time
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# --- 0. Muat Variabel Lingkungan ---
load_dotenv() # Ini akan memuat variabel dari file .env

# --- Konfigurasi ---
api_key = os.getenv('coin-marketcap-api-key') # Ambil API Key dari variabel lingkungan
database_url = os.getenv('database-url')     # Ambil URL Database dari variabel lingkungan

if not api_key:
    raise ValueError("COINMARKETCAP_API_KEY tidak ditemukan di file .env. Harap tambahkan.")
if not database_url:
    raise ValueError("DATABASE_URL tidak ditemukan di file .env. Harap tambahkan.")

API_URL = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
API_PARAMETERS = {
    'start': '1',
    'limit': '10', 
    'convert': 'USD'
}

# Inisialisasi SQLAlchemy Engine
# Ini akan membuat file database jika belum ada
try:
    engine = create_engine(database_url)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Koneksi database siap.")
except Exception as e:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Gagal membuat engine database: {e}")
    exit() # Hentikan eksekusi jika gagal terhubung ke DB

# --- Fungsi untuk Membuat Tabel (Jika Belum Ada) ---
def create_table_if_not_exists():
    with engine.connect() as connection:
        # SQL DDL (Data Definition Language) untuk membuat tabel
        # Pastikan tipe data sesuai dengan data dari API
        # id (CMC) dan symbol akan jadi identifikasi unik untuk koin
        # 'price' bisa float, 'volume_24h' bisa float
        # 'timestamp' untuk mencatat kapan data ditarik
        # Perhatikan nama kolom di df2 nanti, sesuaikan dengan kolom tabel
        
        # Contoh kolom dari CoinMarketCap API:
        # id, name, symbol, slug, num_market_pairs, date_added, tags, max_supply, circulating_supply, total_supply,
        # platform, cmc_rank, self_reported_circulating_supply, self_reported_market_cap, last_updated, quote.USD.price,
        # quote.USD.volume_24h, quote.USD.percent_change_1h, etc.
        
        # Kita hanya ambil beberapa kolom penting
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id INTEGER,                     -- ID dari CoinMarketCap
            name TEXT,                      -- Nama Koin (Bitcoin)
            symbol TEXT,                    -- Simbol Koin (BTC)
            slug TEXT,                      -- Slug (bitcoin)
            cmc_rank INTEGER,               -- Ranking CoinMarketCap
            price REAL,                     -- Harga (dari quote.USD.price)
            volume_24h REAL,                -- Volume 24 jam (dari quote.USD.volume_24h)
            market_cap REAL,                -- Kapitalisasi pasar (dari quote.USD.market_cap)
            percent_change_1h REAL,         -- Perubahan 1 jam (dari quote.USD.percent_change_1h)
            percent_change_24h REAL,        -- Perubahan 24 jam (dari quote.USD.percent_change_24h)
            percent_change_7d REAL,         -- Perubahan 7 hari (dari quote.USD.percent_change_7d)
            last_updated TEXT,              -- Waktu terakhir diupdate oleh CMC
            timestamp TEXT                  -- Waktu penarikan data oleh skrip Anda
        );
        """
        connection.execute(text(create_table_sql))
        connection.commit() # Penting untuk menyimpan perubahan DDL
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Tabel 'crypto_prices' dipastikan ada.")

# --- Fungsi Penarikan dan Penyimpanan Data ---
def fetch_and_save_data():
    """
    Mengambil data dari CoinMarketCap API dan menyimpannya ke database.
    """
    
    headers = {
        'X-CMC_PRO_API_KEY': api_key,
        'Accepts': 'application/json',
    }

    session = Session()
    session.headers.update(headers)

    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_timestamp}] Memulai penarikan data...")

    try:
        response = session.get(API_URL, params=API_PARAMETERS)
        response.raise_for_status() # Akan memunculkan HTTPError untuk status kode 4xx/5xx
        data = json.loads(response.text)
        
        # Normalisasi JSON
        df_new_data = pd.json_normalize(data['data'])

        # Ubah nama kolom yang berakar dari nested JSON (quote.USD.xxx)
        # Hapus 'quote.USD.last_updated' dari rename, karena kita akan ambil 'last_updated' yang global
        df_new_data = df_new_data.rename(columns={
            'quote.USD.price': 'price',
            'quote.USD.volume_24h': 'volume_24h',
            'quote.USD.market_cap': 'market_cap',
            'quote.USD.percent_change_1h': 'percent_change_1h',
            'quote.USD.percent_change_24h': 'percent_change_24h',
            'quote.USD.percent_change_7d': 'percent_change_7d',
            # 'quote.USD.last_updated': 'last_updated' # HAPUS BARIS INI
        })

        # Filter dan urutkan kolom sesuai tabel database
        # Pastikan 'last_updated' yang kita inginkan adalah yang di level root
        desired_columns = [
            'id', 'name', 'symbol', 'slug', 'cmc_rank',
            'price', 'volume_24h', 'market_cap',
            'percent_change_1h', 'percent_change_24h', 'percent_change_7d',
            'last_updated' # Ini seharusnya kolom 'last_updated' yang sudah ada di root DataFrame setelah normalisasi.
        ]

        # Pastikan kolom-kolom ini ada sebelum memilihnya
        available_columns = [col for col in desired_columns if col in df_new_data.columns]
        df_new_data = df_new_data[available_columns]

        # Tambahkan kolom timestamp kapan data ini ditarik oleh skrip Anda
        df_new_data['timestamp'] = pd.to_datetime(current_timestamp) 
        
        # --- Simpan Data ke Database ---
        # Gunakan to_sql() dari Pandas untuk memasukkan DataFrame ke database
        # 'crypto_prices' adalah nama tabel
        # 'if_exists='append'' berarti tambahkan baris baru jika tabel sudah ada
        # 'index=False' berarti jangan simpan indeks DataFrame sebagai kolom di DB
        df_new_data.to_sql('crypto_prices', con=engine, if_exists='append', index=False)
        
        print(f"[{current_timestamp}] {len(df_new_data)} baris data berhasil disimpan ke database.")
            
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(f"[{current_timestamp}] ERROR: Gagal mengambil data: {e}")
    except json.JSONDecodeError as e:
        print(f"[{current_timestamp}] ERROR: Gagal mem-parsing JSON: {e}. Respon: {response.text}")
    except Exception as e:
        print(f"[{current_timestamp}] ERROR: Terjadi kesalahan tak terduga: {e}")

if __name__ == "__main__":
    # Ini akan dijalankan saat skrip dipanggil
    # Pastikan folder 'database' ada
    os.makedirs('database', exist_ok=True) # Membuat folder 'database' jika belum ada

    create_table_if_not_exists() # Pastikan tabel ada sebelum mencoba menyimpan data
    fetch_and_save_data()
    # Anda tidak perlu loop di sini, karena loop akan diatur oleh Cron/Task Scheduler.