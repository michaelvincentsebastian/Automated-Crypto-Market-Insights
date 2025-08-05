import os
import sqlite3
import requests
import pandas as pd
import json
from dotenv import load_dotenv
from datetime import datetime

# --- 1. Konfigurasi ---
# Panggil load_dotenv() untuk memuat environment variables dari file .env
load_dotenv()

DB_PATH = "B:/GitHub Repository/Automated-Crypto-Market-Insights/analysis/database/crypto_data.db"
CLEANED_CSV_FILE = "B:/GitHub Repository/Automated-Crypto-Market-Insights/analysis/cleaned-data/cleaned_data.csv"
API_KEY = os.getenv("CMC_API_KEY")
API_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"

# --- 2. Fungsi untuk Menarik Data Mentah dari API ---
def fetch_raw_data(api_key):
    """
    Menarik 100 data koin terbaru dari API.
    Mengembalikan data JSON jika berhasil, None jika gagal.
    """
    print("Memulai penarikan data dari CoinMarketCap API...")
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': api_key,
    }
    parameters = {
        'start': '1',
        'limit': '100',
        'convert': 'USD'
    }
    
    if not api_key:
        print("Error: API Key tidak ditemukan. Pastikan file .env sudah diatur dengan benar.")
        return None

    try:
        response = requests.get(API_URL, headers=headers, params=parameters, timeout=10)
        response.raise_for_status()
        raw_data = response.json()
        print("Data berhasil ditarik.")
        return raw_data
    except requests.exceptions.RequestException as e:
        print(f"Error saat memanggil API: {e}")
        return None

# --- 3. Fungsi untuk Memproses dan Menambahkan Data ke Database ---
def process_and_append_to_db(raw_data, db_path):
    """
    Memproses data mentah yang berhasil ditarik dan menyimpannya ke database dengan skema baru.
    """
    if not raw_data or 'data' not in raw_data:
        print("Tidak ada data valid untuk diproses dan disimpan ke database.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Membuat tabel crypto_prices jika belum ada, sesuai dengan skema baru
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id INTEGER,
            name TEXT,
            symbol TEXT,
            slug TEXT,
            cmc_rank INTEGER,
            price REAL,
            volume_24h REAL,
            market_cap REAL,
            percent_change_1h REAL,
            percent_change_24h REAL,
            percent_change_7d REAL,
            last_updated TEXT,
            timestamp TEXT
        )
    ''')

    # Memproses setiap koin dan menyisipkan ke tabel
    for coin in raw_data['data']:
        try:
            # Mengambil data tingkat atas
            coin_id = coin.get('id')
            name = coin.get('name')
            symbol = coin.get('symbol')
            slug = coin.get('slug')
            cmc_rank = coin.get('cmc_rank')
            last_updated_cmc = coin.get('last_updated')

            # Mengambil data dari objek quote.USD
            quote_usd = coin.get('quote', {}).get('USD', {})
            price = quote_usd.get('price')
            volume_24h = quote_usd.get('volume_24h')
            market_cap = quote_usd.get('market_cap')
            percent_change_1h = quote_usd.get('percent_change_1h')
            percent_change_24h = quote_usd.get('percent_change_24h')
            percent_change_7d = quote_usd.get('percent_change_7d')

            # Timestamp saat data diambil oleh skrip ini
            timestamp_fetched = datetime.now().isoformat()
            
            # Memasukkan data yang sudah diproses ke tabel crypto_prices
            cursor.execute("""
                INSERT INTO crypto_prices (id, name, symbol, slug, cmc_rank, price, volume_24h, market_cap, 
                percent_change_1h, percent_change_24h, percent_change_7d, last_updated, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (coin_id, name, symbol, slug, cmc_rank, price, volume_24h, market_cap, 
                  percent_change_1h, percent_change_24h, percent_change_7d, last_updated_cmc, timestamp_fetched))
        except Exception as e:
            print(f"Peringatan: Gagal memproses data untuk koin {coin.get('name', 'N/A')}. Error: {e}")
            continue

    conn.commit()
    conn.close()
    print(f"100 data baru berhasil ditambahkan ke database: {db_path}")

# --- 4. Fungsi untuk Menyimpan Data dari Database ke CSV ---
def db_to_csv(db_path, csv_file):
    """
    Membaca seluruh data dari tabel crypto_prices dan menyimpannya ke file CSV.
    """
    print("Menyimpan data dari database ke CSV...")
    conn = sqlite3.connect(db_path)
    try:
        # Membaca seluruh data dari tabel ke dalam DataFrame
        df = pd.read_sql_query("SELECT * FROM crypto_prices", conn)
    except pd.io.sql.DatabaseError:
        print(f"Peringatan: Tabel 'crypto_prices' tidak ditemukan. Tidak ada data yang diproses ke CSV.")
        return
    finally:
        conn.close()

    if df.empty:
        print("Database kosong, tidak ada data untuk disimpan.")
        return

    # Menyimpan DataFrame ke CSV
    df.to_csv(csv_file, mode='w', header=True, index=False)
    print(f"Seluruh data dari database berhasil disimpan ke {csv_file}")

# --- 5. Fungsi Utama untuk Menjalankan Semua Langkah ---
def main():
    """
    Fungsi utama yang menjalankan seluruh alur kerja.
    """
    # Pengecekan jalur file
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        print(f"Peringatan: Jalur folder database tidak ditemukan: {db_dir}")
        return

    csv_dir = os.path.dirname(CLEANED_CSV_FILE)
    if csv_dir and not os.path.exists(csv_dir):
        print(f"Peringatan: Jalur folder CSV tidak ditemukan: {csv_dir}")
        return
    
    # Ambil data dari API
    raw_data = fetch_raw_data(API_KEY)
    
    # Jika pengambilan data berhasil, proses dan simpan ke database
    if raw_data:
        process_and_append_to_db(raw_data, DB_PATH)
    
    # Simpan seluruh data dari DB ke CSV
    db_to_csv(DB_PATH, CLEANED_CSV_FILE)
    
    print("Proses otomatisasi data selesai.")

if __name__ == "__main__":
    main()
