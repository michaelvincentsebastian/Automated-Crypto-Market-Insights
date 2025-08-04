import pandas as pd
import requests
import json
import os
import time

# =======================================================================
# --- KONFIGURASI API KEY (PENTING!) ---
# =======================================================================
# Pastikan Anda menggunakan API Key yang valid. JANGAN LUPA untuk
# MENYIMPANNYA sebagai GitHub Secret di repository Anda
# dengan nama "CMC_PRO_API_KEY".
# Kode ini akan secara otomatis mengambilnya dari environment variable.
API_KEY = os.environ.get('CMC_PRO_API_KEY')
if not API_KEY:
    raise ValueError("CMC_PRO_API_KEY environment variable not set. Please add it to your GitHub Secrets.")

# URL API CoinMarketCap (CMC) untuk mendapatkan data top 500
URL = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'

# =======================================================================
# --- FUNGSI UNTUK MENGAMBIL DATA DARI CMC API ---
# =======================================================================
def get_crypto_data(api_key):
    """
    Mengambil data cryptocurrency terbaru dari CoinMarketCap API.
    """
    parameters = {
        'start': '1',
        'limit': '500',
        'convert': 'USD'
    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': api_key,
    }

    try:
        response = requests.get(URL, headers=headers, params=parameters)
        response.raise_for_status() # Akan memicu error untuk status kode HTTP yang buruk
        data = json.loads(response.text)
        return data['data']
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from CMC API: {e}")
        return None
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error parsing JSON response: {e}")
        print("Response text:", response.text)
        return None

# =======================================================================
# --- FUNGSI UNTUK MEMBERSIHKAN DAN MEMFORMAT DATA ---
# =======================================================================
def clean_and_format_data(raw_data):
    """
    Membersihkan data mentah yang didapat dari API dan mengonversinya ke DataFrame.
    """
    if not raw_data:
        return pd.DataFrame()

    df = pd.DataFrame(raw_data)

    # Memilih kolom yang relevan dan menyederhanakan data
    df_cleaned = pd.DataFrame()
    df_cleaned['id'] = df['id']
    df_cleaned['name'] = df['name']
    df_cleaned['symbol'] = df['symbol']
    df_cleaned['cmc_rank'] = df['cmc_rank']
    df_cleaned['last_updated_utc+0'] = df['last_updated']

    # Memperluas kolom 'quote' untuk mendapatkan metrik harga, kapitalisasi pasar, dll.
    quote_df = pd.json_normalize(df['quote'])['USD']
    for col in ['price', 'volume_24h', 'percent_change_1h', 'percent_change_24h', 'percent_change_7d', 'market_cap']:
        df_cleaned[col] = quote_df.apply(lambda x: x.get(col))

    return df_cleaned

# =======================================================================
# --- MAIN SCRIPT ---
# =======================================================================
if __name__ == "__main__":
    print("Memulai proses pengambilan dan pembersihan data...")
    
    # Dapatkan data dari API
    raw_data = get_crypto_data(API_KEY)
    
    if raw_data:
        print(f"Data berhasil didapat dari API. Jumlah koin: {len(raw_data)}")
        
        # Bersihkan dan format data
        cleaned_df = clean_and_format_data(raw_data)
        
        if not cleaned_df.empty:
            # Tentukan path untuk menyimpan file CSV
            output_dir = 'cleaning'
            output_file_path = os.path.join(output_dir, 'cleaned_data.csv')
            
            # Buat folder jika belum ada
            os.makedirs(output_dir, exist_ok=True)
            
            # Simpan DataFrame ke file CSV
            cleaned_df.to_csv(output_file_path, index=False)
            print(f"Data yang sudah dibersihkan berhasil disimpan ke '{output_file_path}'")
        else:
            print("Gagal membersihkan data atau data kosong.")
    else:
        print("Gagal mendapatkan data dari API.")
