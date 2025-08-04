import streamlit as st
import pandas as pd
import plotly.express as px
import os

# =======================================================================
# --- FUNCTION TO LOAD DATA FROM LOCAL/GITHUB FILE ---
# =======================================================================
# st.cache_data will cache data for 6 minutes to avoid repeated loading
# This duration matches our GitHub Actions workflow schedule.
@st.cache_data(ttl=360)
def load_csv_data():
    """
    Loads data from the updated_file.csv file.
    Retrieves the latest data for each coin (symbol) based on 'last_updated_utc+0'.
    """
    # --- UPDATE: Ganti path file ke 'updated_file.csv' di root repository ---
    file_path = 'cleaning/updated_file.csv'
    
    if not os.path.exists(file_path):
        st.error(f"File '{file_path}' tidak ditemukan. Pastikan Anda telah mengunggahnya ke root repository GitHub Anda.")
        return pd.DataFrame()
        
    try:
        df = pd.read_csv(file_path)
        
        # Check the 'last_updated_utc+0' column and convert it to datetime format
        if 'last_updated_utc+0' in df.columns:
            df['last_updated'] = pd.to_datetime(df['last_updated_utc+0'])
            # Get the latest data for each coin
            df = df.sort_values('last_updated').drop_duplicates(subset=['symbol'], keep='last')
            # Remove the original column after use
            df = df.drop(columns=['last_updated_utc+0'], errors='ignore')
        elif 'last_updated' in df.columns:
            df['last_updated'] = pd.to_datetime(df['last_updated'])
            df = df.sort_values('last_updated').drop_duplicates(subset=['symbol'], keep='last')
        else:
            st.error("Tidak ada kolom tanggal untuk memfilter data terbaru. Pastikan kolom 'last_updated_utc+0' atau 'last_updated' ada.")
            return pd.DataFrame()
        
        return df
    except Exception as e:
        st.error(f"Terjadi kesalahan saat memuat atau memproses data: {e}")
        return pd.DataFrame()


# =======================================================================
# --- STREAMLIT DASHBOARD CONFIGURATION ---
# =======================================================================
# --- UPDATE: Menggunakan emoji sebagai favicon karena lebih stabil saat deployment ---
st.set_page_config(
    page_title="Crypto Coin Market Cap Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="img-resources/icon website.png"
)

# Inject custom CSS for styling
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
    color: #fff; /* Changed color to white */
    margin-right: 15px;
    font-size: 36px; /* Increased icon size */
    text-decoration: none; /* Removed the underline */
}
/* New CSS class for the Key Coin Analysis cards */
.key-coin-card {
    background-color: #162447;
    padding: 1rem;
    border-radius: 8px;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
    height: 100%;
}
</style>
""", unsafe_allow_html=True)


# =======================================================================
# --- DASHBOARD UI STRUCTURE ---
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

# Add an image between the social icons and the refresh button
st.image("https://images.unsplash.com/photo-1640161704729-cbe966a08476?q=80&w=872&auto=format&fit=crop&ixlib=rb-4.1.0&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D", use_container_width=True)

# Add a refresh data button
if st.button("Refresh Data", help="Fetch the latest data from the CSV file"):
    st.cache_data.clear()
    st.rerun()

# Load data
df = load_csv_data()

st.sidebar.header("Customization Options")
if not df.empty:
    coin_options = df['symbol'].tolist()
    selected_coins = st.sidebar.multiselect(
        "Select Coins for Comparison:",
        options=coin_options,
        default=coin_options[:3] if len(coin_options) >= 3 else coin_options
    )

    metric_options = ['price', 'market_cap', 'volume_24h', 'percent_change_24h']
    selected_metric = st.sidebar.selectbox(
        "Select Metric for Analysis:",
        options=metric_options,
        index=3
    )

else:
    st.sidebar.warning("Data not available.")


if not df.empty:
    last_updated_time = df['last_updated'].max() if 'last_updated' in df.columns else "Last update not available"
    st.markdown(f"**Last_Update:** {last_updated_time}")

# --- Dashboard Overview Section ---
st.markdown("---")
st.subheader("Overview Dashboard")
# --- UPDATE: Mengubah pesan informasi untuk mencerminkan otomatisasi GitHub Actions ---
st.info("Catatan: Dashboard ini diperbarui secara otomatis setiap 6 menit oleh GitHub Actions workflow. Data yang ditampilkan akan selalu yang terbaru!")

# --- Aggregate Data Section ---
st.markdown("---")
st.subheader("Aggregate Data Summary")
if not df.empty:
    total_volume = df['volume_24h'].sum()
    total_market_cap = df['market_cap'].sum()
    agg_col1, agg_col2 = st.columns(2)
    with agg_col1:
        st.metric(label="Total Trading Volume (24h)", value=f"${total_volume:,.2f}")
    with agg_col2:
        st.metric(label="Total Market Cap", value=f"${total_market_cap:,.2f}")
else:
    st.warning("Data not available for aggregation.")

# --- Updated Key Coin Metrics Section ---
st.markdown("---")
st.subheader("Key Coin Analysis")
if not df.empty:
    col1, col2 = st.columns(2)
    
    with col1:
        # Replaced st.container(border=True) with a new markdown div to apply the custom class
        st.markdown('<div class="key-coin-card">', unsafe_allow_html=True)
        st.markdown("<h4>Top 5 Daily Gainers <i class='fas fa-arrow-trend-up' style='color:#28a745;'></i></h4>", unsafe_allow_html=True)
        # Change st.container here for a lighter color (background-color: #2c3e50)
        with st.container():
            top_gainers = df.sort_values(by='percent_change_24h', ascending=False).head(5)
            for _, row in top_gainers.iterrows():
                st.markdown(f"<p>{row['name']} ({row['symbol']}): <span style='color: #28a745;'>{row['percent_change_24h']:.2f}%</span></p>", unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        # Replaced st.container(border=True) with a new markdown div to apply the custom class
        st.markdown('<div class="key-coin-card">', unsafe_allow_html=True)
        st.markdown("<h4>Top 5 Daily Losers <i class='fas fa-arrow-trend-down' style='color:#dc3545;'></i></h4>", unsafe_allow_html=True)
        # Change st.container here for a lighter color (background-color: #2c3e50)
        with st.container():
            top_losers = df.sort_values(by='percent_change_24h', ascending=True).head(5)
            for _, row in top_losers.iterrows():
                st.markdown(f"<p>{row['name']} ({row['symbol']}): <span style='color: #dc3545;'>{row['percent_change_24h']:.2f}%</span></p>", unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("---")
    col3, col4 = st.columns(2)
    with col3:
        st.markdown('<div class="key-coin-card">', unsafe_allow_html=True)
        st.markdown("<h4>Top 5 Trading Volume</h4>", unsafe_allow_html=True)
        with st.container():
            top_volume = df.sort_values(by='volume_24h', ascending=False).head(5)
            for _, row in top_volume.iterrows():
                st.markdown(f"<p>{row['name']} ({row['symbol']}): ${row['volume_24h']:.2f}</p>", unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
        
    with col4:
        st.markdown('<div class="key-coin-card">', unsafe_allow_html=True)
        st.markdown("<h4>Top 5 Biggest Market Cap</h4>", unsafe_allow_html=True)
        with st.container():
            top_market_cap_list = df.sort_values(by='market_cap', ascending=False).head(5)
            for _, row in top_market_cap_list.iterrows():
                st.markdown(f"<p>{row['name']} ({row['symbol']}): ${row['market_cap']:.2f}</p>", unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

# --- Data Visualization Section ---
st.markdown("---")
st.subheader("Crypto Data Visualization")
if not df.empty:
    with st.container(border=True): # Using st.container as a blue box
        st.markdown("<h4>Comparison of Selected Coin Metrics</h4>", unsafe_allow_html=True)
        if selected_coins:
            selected_data = df[df['symbol'].isin(selected_coins)]
            fig_compare = px.bar(
                selected_data,
                x='name',
                y=selected_metric,
                color='name',
                title=f"Comparison of {selected_metric.replace('_', ' ').title()}",
                labels={'name': 'Coin Name', selected_metric: f'{selected_metric.replace("_", " ").title()} (USD)'}
            )
            st.plotly_chart(fig_compare, use_container_width=True)
        else:
            st.warning("Select at least one coin in the sidebar to see the comparison.")


# --- DATA TABLE SECTION ---
st.markdown("---")
st.subheader("Crypto Data Table")
if not df.empty:
    st.dataframe(df.sort_values(by='cmc_rank', ascending=True)[[
        'cmc_rank', 'name', 'symbol', 'price', 'market_cap', 'volume_24h', 'percent_change_24h', 'last_updated'
    ]].rename(columns={
        'cmc_rank': 'Rank',
        'name': 'Name',
        'symbol': 'Symbol',
        'price': 'Price (USD)',
        'market_cap': 'Market Cap (USD)',
        'volume_24h': 'Volume 24h (USD)',
        'percent_change_24h': 'Change 24h (%)',
        'last_updated': 'Last Updated'
    }), use_container_width=True, hide_index=True)

