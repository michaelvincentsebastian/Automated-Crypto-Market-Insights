import os
import pandas as pd
import requests
from datetime import datetime

# URL dasar CoinMarketCap API
# Gunakan 'latest' jika Anda hanya ingin data terbaru
API_URL_LATEST = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'

# Fungsi untuk mengambil data dari CoinMarketCap API
def get_crypto_data(api_key):
    """
    Mengambil data mata uang kripto terbaru dari CoinMarketCap API.

    Args:
        api_key (str): Kunci API CoinMarketCap.

    Returns:
        dict: Respons JSON dari API.
        None: Jika terjadi kesalahan.
    """
    print("Memulai proses pengambilan data dari CoinMarketCap API...")
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': api_key,
    }
    parameters = {
        'start': '1',
        'limit': '500', # Ambil 500 koin teratas
        'convert': 'USD'
    }

    try:
        response = requests.get(API_URL_LATEST, headers=headers, params=parameters)
        response.raise_for_status() # Tangani error HTTP
        data = response.json()
        print(f"Data berhasil didapat dari API. Jumlah koin: {len(data['data'])}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error saat mengambil data dari API: {e}")
        return None

# Fungsi untuk membersihkan dan memformat data
def clean_and_format_data(raw_data):
    """
    Membersihkan data mentah dari API dan memformatnya menjadi DataFrame.

    Args:
        raw_data (dict): Data mentah dari API.

    Returns:
        DataFrame: DataFrame yang telah dibersihkan dan diformat.
        None: Jika data mentah tidak valid.
    """
    if not raw_data or 'data' not in raw_data:
        print("Data mentah tidak valid atau kosong.")
        return None

    # Normalisasi data
    df = pd.json_normalize(raw_data['data'])

    # Pastikan data 'quote' ada dan normalisasi hanya jika ada
    if 'quote' in df.columns:
        # Pengecekan keamanan: pastikan 'USD' ada di dalam 'quote'
        def extract_usd_quote(quote):
            return quote.get('USD', None)

        df['quote'] = df['quote'].apply(extract_usd_quote)
        
        # Hapus baris yang tidak memiliki data USD
        df.dropna(subset=['quote'], inplace=True)
        
        # Normalisasi data quote USD
        if not df.empty:
            quote_df = pd.json_normalize(df['quote'])
        else:
            print("Tidak ada data dengan quote USD yang valid.")
            return None
        
        # Gabungkan DataFrame utama dengan DataFrame quote
        df.drop(columns=['quote'], inplace=True)
        cleaned_df = pd.concat([df.reset_index(drop=True), quote_df.reset_index(drop=True)], axis=1)
    else:
        print("Kolom 'quote' tidak ditemukan dalam data.")
        return None

    # Ganti nama kolom agar lebih mudah dibaca sesuai permintaan
    cleaned_df.rename(columns={
        'last_updated': 'last_updated_utc+0',
    }, inplace=True)

    # Pilih dan urutkan kembali kolom yang diinginkan berdasarkan format lama
    final_columns = [
        'id', 'name', 'symbol', 'cmc_rank', 'price', 'volume_24h', 
        'market_cap', 'percent_change_1h', 'percent_change_24h', 
        'percent_change_7d', 'last_updated_utc+0'
    ]
    
    # Filter kolom yang ada di DataFrame
    final_columns = [col for col in final_columns if col in cleaned_df.columns]
    
    return cleaned_df[final_columns]

# Jalankan skrip
if __name__ == "__main__":
    print("Memulai proses pengambilan dan pembersihan data...")

    # Ambil API key dari environment variable
    CMC_PRO_API_KEY = os.getenv('CMC_PRO_API_KEY')

    if not CMC_PRO_API_KEY:
        print("Error: CMC_PRO_API_KEY tidak ditemukan di environment variables.")
    else:
        # 1. Ambil data mentah dari API
        raw_data = get_crypto_data(CMC_PRO_API_KEY)

        # 2. Bersihkan dan format data menjadi DataFrame
        if raw_data:
            cleaned_df = clean_and_format_data(raw_data)

            # 3. Simpan DataFrame ke file CSV
            if cleaned_df is not None and not cleaned_df.empty:
                output_file = os.path.join(os.path.dirname(__file__), 'cleaned_data.csv')
                cleaned_df.to_csv(output_file, index=False)
                print(f"Data yang telah dibersihkan berhasil disimpan ke '{output_file}'")
            else:
                print("Tidak ada data yang dapat disimpan.")
        else:
            print("Gagal mendapatkan data, tidak ada pemrosesan lebih lanjut.")
