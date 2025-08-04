import pandas as pd
import requests
import json
import os
from datetime import datetime

# Fungsi untuk mengambil data dari API CoinMarketCap
def fetch_data():
    """Mengambil data cryptocurrency dari API CoinMarketCap."""
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    parameters = {
        'start': '1',
        'limit': '500',
        'convert': 'USD'
    }
    headers = {
        'Accepts': 'application/json',
        # Menggunakan os.getenv untuk mengambil kunci API dari environment variable
        'X-CMC_PRO_API_KEY': os.getenv('CMC_PRO_API_KEY'),
    }

    try:
        response = requests.get(url, headers=headers, params=parameters)
        response.raise_for_status() # Menimbulkan error untuk kode status HTTP yang buruk
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        print(f"Response content: {response.text}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Request Error: {e}")
        return None

# Fungsi untuk membersihkan dan memformat data
def clean_and_format_data(raw_data):
    """
    Membersihkan dan memformat data JSON mentah menjadi DataFrame.
    Fungsi ini disesuaikan dengan struktur data yang tidak bertingkat.
    """
    if not raw_data or 'data' not in raw_data:
        print("Error: 'data' key not found in raw_data.")
        return pd.DataFrame()

    df = pd.json_normalize(raw_data['data'])
    pull_timestamp = raw_data['status']['timestamp']

    df_cleaned = df[[
        'id', 'name', 'symbol', 'slug', 'cmc_rank',
        'quote.USD.price', 'quote.USD.volume_24h',
        'quote.USD.market_cap', 'quote.USD.percent_change_1h',
        'quote.USD.percent_change_24h', 'quote.USD.percent_change_7d',
        'quote.USD.last_updated'
    ]].copy()

    df_cleaned.columns = [
        'id', 'name', 'symbol', 'slug', 'cmc_rank',
        'price', 'volume_24h', 'market_cap',
        'percent_change_1h', 'percent_change_24h',
        'percent_change_7d', 'last_updated'
    ]

    df_cleaned['pull_timestamp'] = pull_timestamp
    df_cleaned['last_updated'] = pd.to_datetime(df_cleaned['last_updated'])
    df_cleaned['pull_timestamp'] = pd.to_datetime(df_cleaned['pull_timestamp'])

    return df_cleaned

# Fungsi untuk menyimpan data dengan aman (mode append)
def save_data(df, file_path='cleaning/cleaned_data.csv'):
    """
    Menyimpan DataFrame ke file CSV dengan menambahkan data baru.
    Jika file sudah ada, data baru akan digabungkan, dan duplikat akan dihapus
    berdasarkan id dan timestamp terbaru.
    """
    print(f"Memeriksa apakah file {file_path} sudah ada.")
    
    if os.path.exists(file_path):
        # Jika file ada, baca data lama
        print("File sudah ada. Membaca data lama untuk digabungkan.")
        try:
            old_df = pd.read_csv(file_path)
            # Mengonversi kolom datetime yang mungkin disimpan sebagai string
            old_df['pull_timestamp'] = pd.to_datetime(old_df['pull_timestamp'])
            
            # Gabungkan DataFrame lama dan baru
            combined_df = pd.concat([old_df, df], ignore_index=True)

            # Hapus duplikat, hanya menyimpan baris dengan 'pull_timestamp' terbaru
            # Ini penting untuk memastikan tidak ada duplikat data lama
            combined_df.sort_values('pull_timestamp', ascending=False, inplace=True)
            combined_df.drop_duplicates(subset=['id', 'pull_timestamp'], keep='first', inplace=True)
            
            # Simpan kembali seluruh DataFrame ke CSV (menimpa file yang lama)
            combined_df.to_csv(file_path, index=False)
            print(f"Data baru berhasil ditambahkan dan disimpan ke {file_path}")
        except Exception as e:
            print(f"Gagal membaca atau memproses data lama: {e}. Menimpa dengan data baru.")
            df.to_csv(file_path, index=False)
    else:
        # Jika file belum ada, simpan data baru
        print("File tidak ditemukan. Membuat file baru.")
        df.to_csv(file_path, index=False)
        print(f"Data berhasil disimpan ke {file_path}")

if __name__ == "__main__":
    print("Memulai proses pengambilan dan pembersihan data...")
    raw_data = fetch_data()

    if raw_data:
        print(f"Data berhasil didapat dari API. Jumlah koin: {len(raw_data['data'])}")
        cleaned_df = clean_and_format_data(raw_data)

        if not cleaned_df.empty:
            save_data(cleaned_df)
            print("Proses selesai.")
        else:
            print("Gagal membersihkan data.")
    else:
        print("Gagal mengambil data dari API.")
