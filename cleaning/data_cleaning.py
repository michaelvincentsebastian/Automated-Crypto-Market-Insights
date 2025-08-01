import os
from dotenv import load_dotenv
from sqlalchemy import create_engine # sqlalchemy masih dibutuhkan untuk raw_engine
import pandas as pd
from datetime import datetime

# --- 0. Muat Variabel Lingkungan ---
load_dotenv()

# --- Konfigurasi ---
RAW_DATABASE_URL = os.getenv('database-url') 

# PATH untuk menyimpan data bersih dalam format CSV
CLEANED_DATA_DIR = "./cleaning"
CLEANED_CSV_PATH = os.path.join(CLEANED_DATA_DIR, "cleaned_data.csv")

TIMEZONE = os.getenv('TIMEZONE', 'Asia/Jakarta') # Digunakan untuk konversi 'timestamp' dari WIB ke UTC

if not RAW_DATABASE_URL:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: RAW_DATABASE_URL tidak ditemukan di file .env. Harap tambahkan.")
    exit()

try:
    raw_engine = create_engine(RAW_DATABASE_URL)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Koneksi RAW database siap.")
except Exception as e:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Gagal membuat engine RAW database: {e}")
    exit()

# Menghapus konfigurasi cleaned_engine karena akan menyimpan ke CSV
# try:
#     os.makedirs(os.path.dirname(CLEANED_DATABASE_PATH), exist_ok=True)
#     cleaned_engine = create_engine(CLEANED_DATABASE_URL)
#     print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Koneksi CLEANED database siap.")
# except Exception as e:
#     print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Gagal membuat engine CLEANED database: {e}")
#     exit()

def get_raw_data_from_db():
    """Mengambil semua data mentah dari tabel crypto_prices, diurutkan."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Mengambil semua data mentah dari database...")
    try:
        with raw_engine.connect() as connection:
            query = "SELECT * FROM crypto_prices ORDER BY timestamp ASC, id ASC;" 
            df = pd.read_sql(query, connection)
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Berhasil mengambil {len(df)} baris data mentah.")
            return df
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Gagal mengambil data mentah dari database: {e}")
        return pd.DataFrame() 

def clean_and_prepare_data(df):
    """
    Melakukan cleaning dan persiapan data.
    Mengonversi timestamp (WIB) ke UTC dan memastikan semua kolom waktu dalam UTC.
    Kolom lokal tidak dibuat/disimpan di sini.
    """
    if df.empty:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] DataFrame kosong, tidak ada data untuk di-clean.")
        return pd.DataFrame()

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Memulai proses data cleaning dan persiapan...")

    initial_rows_before_drop = len(df)
    if initial_rows_before_drop > 1100:
        df = df.iloc[1101:].copy()
        print(f"  - Menghapus {initial_rows_before_drop - len(df)} baris awal (index 0-1100) yang diasumsikan manual/test.")
    else:
        print(f"  - Tidak ada 1101 baris awal untuk dihapus (total baris: {initial_rows_before_drop}).")

    if 'slug' in df.columns:
        df.drop(columns=['slug'], inplace=True)
        print("  - Kolom 'slug' berhasil dihapus.")
    else:
        print("  - Kolom 'slug' tidak ditemukan.")

    # --- Logika konversi Datetime yang lebih akurat ---
    try:
        df['last_updated'] = pd.to_datetime(df['last_updated'], errors='coerce', utc=True)
        print("  - Konversi kolom 'last_updated' ke UTC berhasil.")
        
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce') 
        
        if not df['timestamp'].isnull().all():
            df['timestamp'] = df['timestamp'].dt.tz_localize(TIMEZONE, ambiguous='infer', nonexistent='shift_forward')
            df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')
            print("  - Konversi kolom 'timestamp' (dari WIB) ke UTC berhasil.")
        else:
            print("  - Kolom 'timestamp' berisi semua NaT, lewati konversi timezone.")

    except Exception as e:
        print(f"  - ERROR konversi datetime: {e}")

    # --- Ganti nama kolom timestamp dan last_updated menjadi format _utc+0 ---
    df.rename(columns={
        'timestamp': 'timestamp_utc+0',
        'last_updated': 'last_updated_utc+0'
    }, inplace=True)
    print("  - Kolom 'timestamp' dan 'last_updated' diganti nama menjadi *_utc+0.")

    numeric_cols = [
        'price', 'volume_24h', 'market_cap',
        'percent_change_1h', 'percent_change_24h', 'percent_change_7d'
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    print("  - Konversi kolom numerik berhasil.")

    initial_rows_dropna = len(df)
    df.dropna(subset=['id', 'name', 'symbol', 'timestamp_utc+0'], inplace=True)
    if len(df) < initial_rows_dropna:
        print(f"  - Menghapus {initial_rows_dropna - len(df)} baris dengan ID/Nama/Timestamp hilang.")
    print("  - Penanganan nilai hilang dasar selesai.")

    df.sort_values(by=['id', 'timestamp_utc+0'], ascending=True, inplace=True)
    df.drop_duplicates(subset=['id', 'timestamp_utc+0'], keep='last', inplace=True)
    print("  - Penanganan duplikasi (id, timestamp_utc+0) dan pengurutan akhir selesai.")
    df.reset_index(drop=True, inplace=True) # Penting setelah drop_duplicates dan sort_values

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Proses data cleaning dan persiapan selesai. Total baris setelah cleaning: {len(df)}")
    return df

def save_cleaned_data_to_csv(df):
    """Menyimpan DataFrame yang sudah bersih ke file CSV."""
    if df.empty:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] DataFrame kosong, tidak ada data untuk disimpan ke CSV.")
        return

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Menyimpan data bersih ke file CSV...")
    try:
        os.makedirs(CLEANED_DATA_DIR, exist_ok=True) # Pastikan direktori ada
        df.to_csv(CLEANED_CSV_PATH, index=False) # Simpan tanpa index DataFrame
        print(f"  - Berhasil menulis {len(df)} baris data bersih ke '{CLEANED_CSV_PATH}'.")
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Gagal menyimpan data bersih ke CSV: {e}")


if __name__ == "__main__":
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] --- Memulai Proses Data Cleaning ---")
    
    raw_df = get_raw_data_from_db()
    processed_df = clean_and_prepare_data(raw_df)
    save_cleaned_data_to_csv(processed_df) # Panggil fungsi penyimpanan CSV

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] --- Proses Data Cleaning Selesai ---")