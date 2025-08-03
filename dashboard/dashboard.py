import os
import streamlit as st
import pandas as pd
import requests
import json
import plotly.express as px
from datetime import datetime

# =======================================================================
# --- KONFIGURASI DAN SETUP API ---
# =======================================================================

# Gunakan kunci API CoinMarketCap Anda di sini
# Catatan: Jangan pernah menempatkan kunci API sensitif langsung di kode
# jika Anda akan membaginya di repositori publik.
CMC_PRO_API_KEY = os.getenv('cmc-api-2')

# URL dan header untuk API CoinMarketCap
CMC_URL = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
CMC_HEADERS = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': CMC_PRO_API_KEY,
}

# Fungsi untuk mengambil data dari API CoinMarketCap
# Menggunakan st.cache_data untuk menyimpan data selama 6 menit
# Ini mencegah panggilan API berulang-ulang dan menghemat penggunaan kuota.
@st.cache_data(ttl=60 * 6)  # Refresh data setiap 6 menit
def get_crypto_data():
    try:
        response = requests.get(CMC_URL, headers=CMC_HEADERS)
        response.raise_for_status()  # Mencegah error HTTP
        data = json.loads(response.text)
        
        # Mengubah data JSON menjadi DataFrame
        if data['status']['error_code'] == 0:
            return pd.DataFrame(data['data'])
        else:
            st.error(f"Gagal mengambil data dari API: {data['status']['error_message']}")
            return pd.DataFrame()
            
    except requests.exceptions.RequestException as e:
        st.error(f"Terjadi kesalahan koneksi: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Terjadi kesalahan saat memproses data: {e}")
        return pd.DataFrame()

# =======================================================================
# --- SETUP DASHBOARD STREAMLIT ---
# =======================================================================

st.set_page_config(
    page_title="Dashboard Analisis Kripto",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Kustomisasi CSS untuk tampilan gelap dan modern
st.markdown("""
<style>
.main {
    background-color: #1a1a2e;
    color: #fff;
}
.sidebar .sidebar-content {
    background-color: #162447;
    padding-top: 2rem;
}
.st-bb {
    background-color: #162447 !important;
}
.st-be {
    color: #fff;
}
h1, h2, h3 {
    color: #a4f5d8;
}
.metric-card {
    background-color: #1f4068;
    padding: 1.5rem;
    border-radius: 12px;
    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
}
.chart-container {
    background-color: #1f4068;
    padding: 1.5rem;
    border-radius: 12px;
    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
}
.st-bw {
    background-color: #27374D;
}
.st-bu {
    color: #fff;
}
</style>
""", unsafe_allow_html=True)

# Memuat data
df = get_crypto_data()

# =======================================================================
# --- SIDEBAR INTERAKTIF ---
# =======================================================================
st.sidebar.header("Opsi Kustomisasi")

# Pilihan koin untuk visualisasi
coin_options = df['symbol'].tolist() if not df.empty else ['BTC', 'ETH', 'ADA']
selected_coins = st.sidebar.multiselect(
    "Pilih Koin untuk Perbandingan:",
    options=coin_options,
    default=coin_options[:3]
)

# Pilihan metrik untuk dianalisis
metric_options = ['price', 'market_cap', 'volume_24h']
selected_metric = st.sidebar.selectbox(
    "Pilih Metrik untuk Analisis:",
    options=metric_options,
    index=0
)

# Filter untuk Top N koin
top_n = st.sidebar.slider(
    "Tampilkan Top N Koin:",
    min_value=5,
    max_value=20,
    value=10
)

# =======================================================================
# --- TAMPILAN UTAMA DASHBOARD ---
# =======================================================================

st.title("Dashboard Analisis Kripto Real-Time")
st.markdown(f"Terakhir diperbarui: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if not df.empty:
    # Memproses data untuk ditampilkan
    df['market_cap_formatted'] = df['quote'].apply(lambda x: x['USD']['market_cap'])
    df['volume_24h_formatted'] = df['quote'].apply(lambda x: x['USD']['volume_24h'])
    df['price_formatted'] = df['quote'].apply(lambda x: x['USD']['price'])

    # --- Bagian 1: Metrik Utama (Kartu) ---
    st.markdown("---")
    st.subheader("Metrik Koin Utama")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        btc_data = df[df['symbol'] == 'BTC'].iloc[0]['quote']['USD']
        st.markdown(f"""
        <div class="metric-card">
            <h4>Bitcoin (BTC)</h4>
            <h3 style="color: #FFC107;">${btc_data['price']:.2f}</h3>
            <p style="color: {'#28a745' if btc_data['percent_change_24h'] > 0 else '#dc3545'};">
                {btc_data['percent_change_24h']:.2f}% (24h)
            </p>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        eth_data = df[df['symbol'] == 'ETH'].iloc[0]['quote']['USD']
        st.markdown(f"""
        <div class="metric-card">
            <h4>Ethereum (ETH)</h4>
            <h3 style="color: #8A2BE2;">${eth_data['price']:.2f}</h3>
            <p style="color: {'#28a745' if eth_data['percent_change_24h'] > 0 else '#dc3545'};">
                {eth_data['percent_change_24h']:.2f}% (24h)
            </p>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        sol_data = df[df['symbol'] == 'SOL'].iloc[0]['quote']['USD']
        st.markdown(f"""
        <div class="metric-card">
            <h4>Solana (SOL)</h4>
            <h3 style="color: #9932CC;">${sol_data['price']:.2f}</h3>
            <p style="color: {'#28a745' if sol_data['percent_change_24h'] > 0 else '#dc3545'};">
                {sol_data['percent_change_24h']:.2f}% (24h)
            </p>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        doge_data = df[df['symbol'] == 'DOGE'].iloc[0]['quote']['USD']
        st.markdown(f"""
        <div class="metric-card">
            <h4>Dogecoin (DOGE)</h4>
            <h3 style="color: #FFD700;">${doge_data['price']:.2f}</h3>
            <p style="color: {'#28a745' if doge_data['percent_change_24h'] > 0 else '#dc3545'};">
                {doge_data['percent_change_24h']:.2f}% (24h)
            </p>
        </div>
        """, unsafe_allow_html=True)


    # --- Bagian 2: Visualisasi (Grafik) ---
    st.markdown("---")
    st.subheader("Visualisasi Data Kripto")
    
    col1_chart, col2_chart = st.columns(2)

    with col1_chart:
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        st.markdown("<h4>Top 10 Koin Berdasarkan Market Cap</h4>", unsafe_allow_html=True)
        top_market_cap = df.sort_values(by='market_cap_formatted', ascending=False).head(top_n)
        fig_market_cap = px.bar(
            top_market_cap,
            x='name',
            y='market_cap_formatted',
            title=f"Top {top_n} Koin Berdasarkan Market Cap",
            labels={'name': 'Nama Koin', 'market_cap_formatted': 'Kapitalisasi Pasar (USD)'}
        )
        st.plotly_chart(fig_market_cap, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with col2_chart:
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        st.markdown("<h4>Perbandingan Metrik Koin Pilihan</h4>", unsafe_allow_html=True)
        selected_data = df[df['symbol'].isin(selected_coins)]
        fig_compare = px.bar(
            selected_data,
            x='name',
            y=f'{selected_metric}_formatted',
            color='name',
            title=f"Perbandingan {selected_metric.replace('_', ' ').title()}",
            labels={'name': 'Nama Koin', f'{selected_metric}_formatted': f'{selected_metric.replace("_", " ").title()} (USD)'}
        )
        st.plotly_chart(fig_compare, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # --- Bagian 3: Tabel Data ---
    st.markdown("---")
    st.subheader("Tabel Data Kripto")
    # Menggunakan kolom yang sudah diformat untuk tampilan yang lebih baik
    st.dataframe(df[[
        'name', 'symbol', 'market_cap_formatted', 'volume_24h_formatted', 'price_formatted'
    ]].rename(columns={
        'name': 'Nama',
        'symbol': 'Simbol',
        'market_cap_formatted': 'Kapitalisasi Pasar',
        'volume_24h_formatted': 'Volume 24h',
        'price_formatted': 'Harga'
    }), use_container_width=True)

else:
    st.warning("Data tidak tersedia. Periksa kunci API Anda atau koneksi internet.")
