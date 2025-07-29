import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime

# --- 0. Muat Variabel Lingkungan ---
load_dotenv()

# --- Konfigurasi ---
database_url = os.getenv('database-url')

if not database_url:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: DATABASE_URL tidak ditemukan di file .env. Harap tambahkan.")
    exit()

# Inisialisasi SQLAlchemy Engine
try:
    engine = create_engine(database_url)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Koneksi database siap.")
except Exception as e:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Gagal membuat engine database: {e}")
    exit()

def check_database_contents():
    try:
        with engine.connect() as connection:
            # Query untuk mengambil semua data dari tabel crypto_prices
            # Anda bisa membatasi baris dengan LIMIT jika datanya sudah sangat banyak
            query = "SELECT * FROM crypto_prices ORDER BY timestamp DESC, cmc_rank ASC;"
            df = pd.read_sql(query, connection)

            if df.empty:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Tabel 'crypto_prices' kosong.")
            else:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Data ditemukan di tabel 'crypto_prices':")
                print(f"Total baris: {len(df)}")
                print("\n5 Baris Data Terbaru (Berdasarkan timestamp dari skrip Anda):")
                print(df.head()) # Tampilkan 5 baris pertama

                print("\nBeberapa Kolom Penting untuk Verifikasi:")
                print(df[['name', 'symbol', 'price', 'volume_24h', 'timestamp']].tail()) # Tampilkan 5 baris terakhir untuk memastikan penambahan

    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Gagal membaca data dari database: {e}")

if __name__ == "__main__":
    check_database_contents()