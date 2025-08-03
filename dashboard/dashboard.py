import streamlit as st
import pandas as pd
import plotly.express as px
import os

# =======================================================================
# --- FUNGSI UNTUK MEMUAT DATA DARI FILE LOKAL/GITHUB ---
# =======================================================================
# st.cache_data akan menyimpan data selama 6 menit untuk menghindari pemuatan berulang
@st.cache_data(ttl=360)
def load_csv_data():
    """
    Memuat data dari file cleaned_data.csv yang berada di direktori cleaning/.
    Mengambil data terakhir untuk setiap koin (symbol) berdasarkan 'last_updated_utc+0'.
    """
    try:
        df = pd.read_csv('cleaning/cleaned_data.csv')
        
        # Periksa kolom 'last_updated_utc+0' dan ubah ke format datetime
        if 'last_updated_utc+0' in df.columns:
            df['last_updated_utc+0'] = pd.to_datetime(df['last_updated_utc+0'])
            # Mengambil data terbaru untuk setiap koin
            df = df.sort_values('last_updated_utc+0').drop_duplicates(subset=['symbol'], keep='last')
            # Hapus kolom asli setelah digunakan
            df = df.drop(columns=['last_updated_utc+0'], errors='ignore')
        elif 'last_updated_utc+0' in df.columns:
            df['last_updated_utc+0'] = pd.to_datetime(df['last_updated_utc+0'])
            df = df.sort_values('last_updated_utc+0').drop_duplicates(subset=['symbol'], keep='last')
        else:
            st.error("Tidak ada kolom tanggal untuk memfilter data terbaru.")
            return pd.DataFrame()
        
        return df
    except FileNotFoundError:
        st.error("File 'cleaning/cleaned_data.csv' tidak ditemukan. Pastikan Anda telah mengunggahnya ke repositori.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Terjadi kesalahan saat memuat data: {e}")
        return pd.DataFrame()


# =======================================================================
# --- KONFIGURASI DASHBOARD STREAMLIT ---
# =======================================================================
# Favicon placeholder. Ganti URL ini dengan URL logo Anda.
st.set_page_config(
    page_title="Dashboard Kripto Real-Time",
    layout="wide",
    initial_sidebar_state="expanded",
    # Perbaikan: Menggunakan URL publik untuk favicon, bukan jalur file lokal.
    page_icon="https://cdn.jsdelivr.net/npm/emoji-datasource-apple/img/apple/64/1f4b8.png"
)

# Injeksi CSS kustom untuk tampilan
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
body {
    font-family: 'Inter', sans-serif;
}
.main {
    background-color: #1a1a2e;
    color: #fff;
}
.sidebar .sidebar-content {
    background-color: #162447;
    padding-top: 2rem;
}
h1, h2, h3 {
    color: #a4f5d8;
}
.metric-card {
    background-color: #1f4068;
    padding: 1.5rem;
    border-radius: 12px;
    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    height: 100%; /* Ensure cards have equal height */
}
.sub-metric-card {
    background-color: #2c3e50;
    padding: 1rem;
    border-radius: 8px;
    margin-top: 1rem;
    height: 100%;
}
.chart-container {
    background-color: #1f4068;
    padding: 1.5rem;
    border-radius: 12px;
    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
}
.social-icons a {
    color: #a4f5d8;
    margin-right: 15px;
    font-size: 24px;
}
</style>
""", unsafe_allow_html=True)


# =======================================================================
# --- STRUKTUR UI DASHBOARD ---
# =======================================================================

st.title("Crypto Coin Market Cap Analytics Dashboard")
st.markdown("### by Michael Vincent Sebastian Handojo")
st.markdown("""
<div class="social-icons">
    <a href="https://github.com/michaelvincentsebastian" target="_blank">
        <i class="fab fa-github"></i>
    </a>
    <a href="https://www.linkedin.com/in/michaelvincentsebastian/" target="_blank">
        <i class="fab fa-linkedin"></i>
    </a>
    <a href="https://www.instagram.com/mchlvincent_/" target="_blank">
        <i class="fab fa-instagram"></i>
    </a>
</div>
<br>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css">
""", unsafe_allow_html=True)

# Tambahkan tombol refresh data
if st.button("Refresh Data", help="Ambil data terbaru dari file CSV"):
    st.cache_data.clear()
    st.rerun()

# Muat data
df = load_csv_data()

st.sidebar.header("Opsi Kustomisasi")
if not df.empty:
    coin_options = df['symbol'].tolist()
    selected_coins = st.sidebar.multiselect(
        "Pilih Koin untuk Perbandingan:",
        options=coin_options,
        default=coin_options[:3] if len(coin_options) >= 3 else coin_options
    )

    metric_options = ['price', 'market_cap', 'volume_24h', 'percent_change_24h']
    selected_metric = st.sidebar.selectbox(
        "Pilih Metrik untuk Analisis:",
        options=metric_options,
        index=3
    )

else:
    st.sidebar.warning("Data tidak tersedia.")


if not df.empty:
    last_updated_time = df['last_updated'].max() if 'last_updated' in df.columns else "Last update not available"
    st.markdown(f"**Last_Update:** {last_updated_time}")

# --- Bagian Overview Dashboard ---
st.markdown("---")
st.subheader("Overview Dashboard")
st.info("Catatan: Data yang diambil ini ditarik dari API ke database lokal. Data hanya dapat diperbarui saat author menyalakan device dan skrip otomatis berjalan.")

# --- Bagian Agregasi Data ---
st.markdown("---")
st.subheader("Ringkasan Data Agregat")
if not df.empty:
    total_volume = df['volume_24h'].sum()
    total_market_cap = df['market_cap'].sum()
    agg_col1, agg_col2 = st.columns(2)
    with agg_col1:
        st.metric(label="Total Volume Trading (24h)", value=f"${total_volume:,.2f}")
    with agg_col2:
        st.metric(label="Total Market Cap", value=f"${total_market_cap:,.2f}")
else:
    st.warning("Data tidak tersedia untuk agregasi.")

# --- Bagian Metrik Koin Utama yang Diperbarui ---
st.markdown("---")
st.subheader("Analisis Koin Utama")
if not df.empty:
    col1, col2 = st.columns(2)
    
    with col1:
        with st.container(border=True): # Menggunakan st.container sebagai box biru
            st.markdown("<h4>Top 5 Daily Gainers 🔼</h4>", unsafe_allow_html=True)
            with st.container(): # Box abu-abu
                top_gainers = df.sort_values(by='percent_change_24h', ascending=False).head(5)
                for _, row in top_gainers.iterrows():
                    st.markdown(f"<p>{row['name']} ({row['symbol']}): <span style='color: #28a745;'>{row['percent_change_24h']:.2f}%</span></p>", unsafe_allow_html=True)
    
    with col2:
        with st.container(border=True):
            st.markdown("<h4>Top 5 Daily Losers 🔻</h4>", unsafe_allow_html=True)
            with st.container():
                top_losers = df.sort_values(by='percent_change_24h', ascending=True).head(5)
                for _, row in top_losers.iterrows():
                    st.markdown(f"<p>{row['name']} ({row['symbol']}): <span style='color: #dc3545;'>{row['percent_change_24h']:.2f}%</span></p>", unsafe_allow_html=True)

    st.markdown("---")
    col3, col4 = st.columns(2)
    with col3:
        with st.container(border=True):
            st.markdown("<h4>Top 5 Trading Volume</h4>", unsafe_allow_html=True)
            with st.container():
                top_volume = df.sort_values(by='volume_24h', ascending=False).head(5)
                for _, row in top_volume.iterrows():
                    st.markdown(f"<p>{row['name']} ({row['symbol']}): ${row['volume_24h']:.2f}</p>", unsafe_allow_html=True)
        
    with col4:
        with st.container(border=True):
            st.markdown("<h4>Top 5 Biggest Market Cap</h4>", unsafe_allow_html=True)
            with st.container():
                top_market_cap_list = df.sort_values(by='market_cap', ascending=False).head(5)
                for _, row in top_market_cap_list.iterrows():
                    st.markdown(f"<p>{row['name']} ({row['symbol']}): ${row['market_cap']:.2f}</p>", unsafe_allow_html=True)

# --- Bagian Visualisasi Data ---
st.markdown("---")
st.subheader("Visualisasi Data Kripto")
if not df.empty:
    with st.container(border=True): # Menggunakan st.container sebagai box biru
        st.markdown("<h4>Perbandingan Metrik Koin Pilihan</h4>", unsafe_allow_html=True)
        if selected_coins:
            selected_data = df[df['symbol'].isin(selected_coins)]
            fig_compare = px.bar(
                selected_data,
                x='name',
                y=selected_metric,
                color='name',
                title=f"Perbandingan {selected_metric.replace('_', ' ').title()}",
                labels={'name': 'Nama Koin', selected_metric: f'{selected_metric.replace("_", " ").title()} (USD)'}
            )
            st.plotly_chart(fig_compare, use_container_width=True)
        else:
            st.warning("Pilih setidaknya satu koin di sidebar untuk melihat perbandingan.")


# --- BAGIAN TABEL DATA ---
st.markdown("---")
st.subheader("Tabel Data Kripto")
if not df.empty:
    st.dataframe(df.sort_values(by='cmc_rank', ascending=True)[[
        'cmc_rank', 'name', 'symbol', 'price', 'market_cap', 'volume_24h', 'percent_change_24h', 'last_updated'
    ]].rename(columns={
        'cmc_rank': 'Rank',
        'name': 'Nama',
        'symbol': 'Simbol',
        'price': 'Harga (USD)',
        'market_cap': 'Kapitalisasi Pasar (USD)',
        'volume_24h': 'Volume 24h (USD)',
        'percent_change_24h': 'Perubahan 24h (%)',
        'last_updated': 'Terakhir Diperbarui'
    }), use_container_width=True)
