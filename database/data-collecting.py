# data_collector.py (VERSI FINAL UNTUK TASK SCHEDULER)
import pandas as pd
import json
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from datetime import datetime
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# --- 0. Muat Variabel Lingkungan ---
load_dotenv()

# --- Konfigurasi ---
api_key = os.getenv('coin-marketcap-api-key') # Pastikan nama variabel di .env juga COINMARKETCAP_API_KEY
database_url = os.getenv('database-url')     # Pastikan nama variabel di .env juga DATABASE_URL

if not api_key:
    # Menambahkan log ke file jika terjadi error, karena tidak ada terminal
    with open("error_log.txt", "a") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: COINMARKETCAP_API_KEY tidak ditemukan di file .env.\n")
    raise ValueError("COINMARKETCAP_API_KEY tidak ditemukan di file .env. Harap tambahkan.")
if not database_url:
    with open("error_log.txt", "a") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: DATABASE_URL tidak ditemukan di file .env.\n")
    raise ValueError("DATABASE_URL tidak ditemukan di file .env. Harap tambahkan.")

API_URL = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
API_PARAMETERS = {
    'start': '1',
    'limit': '10',
    'convert': 'USD'
}

# Inisialisasi SQLAlchemy Engine
try:
    engine = create_engine(database_url)
    # Tidak perlu print langsung ke console jika Task Scheduler menjalankannya di background
    # print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Koneksi database siap.")
except Exception as e:
    with open("error_log.txt", "a") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Gagal membuat engine database: {e}\n")
    exit()

def create_table_if_not_exists():
    with engine.connect() as connection:
        create_table_sql = """
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
        );
        """
        connection.execute(text(create_table_sql))
        connection.commit()
        # print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Tabel 'crypto_prices' dipastikan ada.")

def fetch_and_save_data():
    headers = {
        'X-CMC_PRO_API_KEY': api_key,
        'Accepts': 'application/json',
    }
    session = Session()
    session.headers.update(headers)

    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # print(f"[{current_timestamp}] Memulai penarikan data...")

    try:
        response = session.get(API_URL, params=API_PARAMETERS)
        response.raise_for_status()
        data = json.loads(response.text)

        df_new_data = pd.json_normalize(data['data'])

        df_new_data = df_new_data.rename(columns={
            'quote.USD.price': 'price',
            'quote.USD.volume_24h': 'volume_24h',
            'quote.USD.market_cap': 'market_cap',
            'quote.USD.percent_change_1h': 'percent_change_1h',
            'quote.USD.percent_change_24h': 'percent_change_24h',
            'quote.USD.percent_change_7d': 'percent_change_7d',
            # Hapus baris ini: 'quote.USD.last_updated': 'last_updated'
        })

        desired_columns = [
            'id', 'name', 'symbol', 'slug', 'cmc_rank',
            'price', 'volume_24h', 'market_cap',
            'percent_change_1h', 'percent_change_24h', 'percent_change_7d',
            'last_updated' # Ini dari level root API
        ]

        available_columns = [col for col in desired_columns if col in df_new_data.columns]
        df_new_data = df_new_data[available_columns]

        df_new_data['timestamp'] = pd.to_datetime(current_timestamp) 

        df_new_data.to_sql('crypto_prices', con=engine, if_exists='append', index=False)

        # Print atau log ke file agar bisa dimonitor
        with open("api_log.txt", "a") as f:
            f.write(f"[{current_timestamp}] {len(df_new_data)} baris data berhasil disimpan ke database.\n")

    except (ConnectionError, Timeout, TooManyRedirects) as e:
        with open("error_log.txt", "a") as f:
            f.write(f"[{current_timestamp}] ERROR: Gagal mengambil data: {e}\n")
    except json.JSONDecodeError as e:
        with open("error_log.txt", "a") as f:
            f.write(f"[{current_timestamp}] ERROR: Gagal mem-parsing JSON: {e}. Respon: {response.text}\n")
    except Exception as e:
        with open("error_log.txt", "a") as f:
            f.write(f"[{current_timestamp}] ERROR: Terjadi kesalahan tak terduga: {e}\n")

if __name__ == "__main__":
    os.makedirs('database', exist_ok=True)
    create_table_if_not_exists()
    fetch_and_save_data()