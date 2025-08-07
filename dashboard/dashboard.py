import streamlit as st
import pandas as pd
import plotly.express as px
import os
import streamlit.components.v1 as components
from datetime import datetime

# =======================================================================
# --- FUNCTIONS TO LOAD DATA FROM LOCAL/GITHUB FILE ---
# =======================================================================
@st.cache_data(ttl=360)
def load_latest_data():
    """
    Loads the latest snapshot of data from the updated_file.csv file.
    This function is now designed to work with a CSV that may contain historical data,
    but it only returns the most recent entry for each coin for the main dashboard metrics.
    """
    file_path = 'cleaning/updated_file.csv'
    
    if not os.path.exists(file_path):
        st.error(f"File '{file_path}' not found. Make sure you have uploaded it to the root of your GitHub repository.")
        return pd.DataFrame()
        
    try:
        df = pd.read_csv(file_path)
        
        # Check for the date column
        if 'last_updated_utc+0' in df.columns:
            df['last_updated'] = pd.to_datetime(df['last_updated_utc+0'])
        elif 'last_updated' in df.columns:
            df['last_updated'] = pd.to_datetime(df['last_updated'])
        else:
            st.error("No date column found. Please ensure 'last_updated_utc+0' or 'last_updated' exists.")
            return pd.DataFrame()
        
        # Get the latest data for each coin
        latest_df = df.sort_values('last_updated').drop_duplicates(subset=['symbol'], keep='last')
        return latest_df
    except Exception as e:
        st.error(f"An error occurred while loading or processing the data: {e}")
        return pd.DataFrame()


# =======================================================================
# --- STREAMLIT DASHBOARD CONFIGURATION ---
# =======================================================================
st.set_page_config(
    page_title="Crypto Coin Market Cap Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="img-resources/icon website.png"
)

# Inject custom CSS for styling, animations, and responsiveness
st.markdown("""
<style>
/* Smooth scrolling */
html {
    scroll-behavior: smooth;
}
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
h1, h2, h3, h5 {
    color: #a4f5d8;
}
.metric-card {
    background-color: #1f4068;
    padding: 1.5rem;
    border-radius: 12px;
    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    height: 100%;
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
    color: #fff;
    margin-right: 15px;
    font-size: 36px;
    text-decoration: none;
    /* Updated transition for the new effect */
    transition: transform 0.3s ease;
}
.social-icons a:hover {
    /* New hover effect: icon scales up */
    transform: scale(1.2);
}
.key-coin-list-item {
    padding: 8px 0;
}
.key-coin-list-item h5 {
    margin: 0;
    font-size: 1.2rem;
    color: #fff;
}
.key-coin-list-item p {
    margin: 0;
    font-size: 0.9rem;
    color: #ccc;
}
.gain {
    color: #28a745;
}
.loss {
    color: #dc3545;
}
.change-container {
    display: flex;
    align-items: center;
    gap: 5px;
}
.trend-icon {
    font-size: 1rem;
}
.flex-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
}
/* Keyframes for animations */
@keyframes fadeInUp {
    from {
        opacity: 0;
        transform: translateY(20px);
    }
    to {
        opacity: 1;
        transform: translateY(0);
    }
}
@keyframes fadeIn {
    from {
        opacity: 0;
    }
    to {
        opacity: 1;
    }
}
/* Apply animations to specific components */
.stTitle, .stSubheader, .stAlert, .stSelectbox, .stMultiselect, .stDataFrame, .stImage, .stMetric, .stPlotlyChart {
    animation: fadeInUp 0.5s ease-out forwards;
}
.stMetric {
    animation-delay: 0.2s;
}
.stPlotlyChart {
    animation-delay: 0.4s;
}
.stContainer {
    animation: fadeIn 0.5s ease-out forwards;
}
/* Style for button hover */
.stButton>button {
    background-color: #162447;
    color: white;
    border: none;
    padding: 10px 20px;
    border-radius: 8px;
    transition: background-color 0.3s, transform 0.3s, box-shadow 0.3s;
}
.stButton>button:hover {
    background-color: #1f4068;
    transform: scale(1.05) rotate(-1deg);
    box-shadow: 0 0 10px #a4f5d8;
}
/* Style for expander (for customization options) */
.st-expander {
    transition: all 0.3s ease;
}
.st-expander:hover {
    background-color: #1f4068;
    border-radius: 8px;
}
.st-expander-header {
    transition: transform 0.3s ease;
}
.st-expander-header:hover {
    transform: translateX(5px);
}
/* Mobile responsiveness */
@media (max-width: 768px) {
    .stImage img {
        height: 200px; /* Adjust image height on smaller screens */
    }
    .stTitle {
        font-size: 1.8rem;
    }
    .stSubheader {
        font-size: 1.5rem;
    }
    .stMetric {
        font-size: 0.9rem;
    }
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

st.image("https://images.unsplash.com/photo-1640161704729-cbe966a08476?q=80&w=872&auto=format&fit=crop&ixlib=rb-4.1.0&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D", use_container_width=True)

if st.button("Refresh Data", help="Fetch the latest data from the CSV file"):
    st.cache_data.clear()
    st.rerun()

# Load latest data
df_latest = load_latest_data()


with st.sidebar:
    with st.expander("Customization Options"):
        if not df_latest.empty:
            coin_options = df_latest['symbol'].tolist()
            selected_coins = st.multiselect(
                "Select Coins for Comparison:",
                options=coin_options,
                default=coin_options[:3] if len(coin_options) >= 3 else coin_options
            )

            # Map user-friendly names to actual column names
            metric_map = {
                'Price': 'price',
                'Market Cap': 'market_cap',
                'Volume 24h': 'volume_24h',
                'Percent Change 24h': 'percent_change_24h'
            }
            selected_display_metric = st.selectbox(
                "Select Metric for Analysis:",
                options=list(metric_map.keys()), # Use the user-friendly names
                index=3
            )
            # Get the internal column name from the selected display name
            selected_metric_internal = metric_map[selected_display_metric]

            selected_coin_historical = st.selectbox(
                "Select Coin for Historical Chart:",
                options=coin_options,
                index=0 # Default to the first coin
            )

        else:
            st.warning("Data not available.")


if not df_latest.empty:
    last_updated_dt = df_latest['last_updated'].max() if 'last_updated' in df_latest.columns else None

    if last_updated_dt:
        # Format the datetime object to a user-friendly string in UTC
        formatted_date = last_updated_dt.strftime("%Y-%m-%d")
        formatted_time = last_updated_dt.strftime("%H:%M")

        readable_string = f"Data last updated on **{formatted_date}** at **{formatted_time} (UTC+00:00)**."
    else:
        readable_string = "Data update status is not available."

    st.markdown(readable_string)

# --- Dashboard Overview Section ---
st.markdown("---")
st.subheader("Overview Dashboard")
st.info("Note: This dashboard is automatically updated every 6 minutes by a GitHub Actions workflow. The data shown will always be the most recent!")

# --- Aggregate Data Section ---
st.markdown("---")
st.subheader("Aggregate Data Summary")
if not df_latest.empty:
    # Combine the metrics into a single container
    with st.container(border=True):
        total_volume = df_latest['volume_24h'].sum()
        total_market_cap = df_latest['market_cap'].sum()
        
        # Use columns to place them side-by-side within the container
        agg_col1, agg_col2 = st.columns(2)
        with agg_col1:
            st.metric(label="Total Trading Volume (24h)", value=f"${total_volume:,.2f}")
        with agg_col2:
            st.metric(label="Total Market Cap", value=f"${total_market_cap:,.2f}")
else:
    st.warning("Data not available for aggregation.")

# --- Updated Key Coin Metrics Section (New Card-Style Layout) ---
st.markdown("---")
st.subheader("Key Coin Analysis")
if not df_latest.empty:
    # Top 5 Daily Gainers and Losers
    top_gainers = df_latest.sort_values(by='percent_change_24h', ascending=False).head(5)
    top_losers = df_latest.sort_values(by='percent_change_24h', ascending=True).head(5)

    col_gainer, col_loser = st.columns(2)
    
    with col_gainer:
        with st.container(border=True):
            st.markdown("<h5>Top 5 Daily Gainers</h5>", unsafe_allow_html=True)
            for _, row in top_gainers.iterrows():
                st.markdown(f"""
                <div class="key-coin-list-item flex-row">
                    <div>
                        <h5>{row['name']} ({row['symbol']})</h5>
                    </div>
                    <div class="change-container">
                        <span class="gain">{row['percent_change_24h']:.2f}%</span>
                        <i class="fas fa-caret-up trend-icon gain"></i>
                    </div>
                </div>
                """, unsafe_allow_html=True)

    with col_loser:
        with st.container(border=True):
            st.markdown("<h5>Top 5 Daily Losers</h5>", unsafe_allow_html=True)
            for _, row in top_losers.iterrows():
                st.markdown(f"""
                <div class="key-coin-list-item flex-row">
                    <div>
                        <h5>{row['name']} ({row['symbol']})</h5>
                    </div>
                    <div class="change-container">
                        <span class="loss">{row['percent_change_24h']:.2f}%</span>
                        <i class="fas fa-caret-down trend-icon loss"></i>
                    </div>
                </div>
                """, unsafe_allow_html=True)

    # Top 5 Trading Volume and Market Cap
    top_volume = df_latest.sort_values(by='volume_24h', ascending=False).head(5)
    top_market_cap_list = df_latest.sort_values(by='market_cap', ascending=False).head(5)

    col_volume, col_market_cap = st.columns(2)
    
    with col_volume:
        with st.container(border=True):
            st.markdown("<h5>Top 5 Trading Volume</h5>", unsafe_allow_html=True)
            for _, row in top_volume.iterrows():
                st.markdown(f"""
                <div class="key-coin-list-item flex-row">
                    <div>
                        <h5>{row['name']} ({row['symbol']})</h5>
                    </div>
                    <p>${row['volume_24h']:.2f}</p>
                </div>
                """, unsafe_allow_html=True)
                
    with col_market_cap:
        with st.container(border=True):
            st.markdown("<h5>Top 5 Biggest Market Cap</h5>", unsafe_allow_html=True)
            for _, row in top_market_cap_list.iterrows():
                st.markdown(f"""
                <div class="key-coin-list-item flex-row">
                    <div>
                        <h5>{row['name']} ({row['symbol']})</h5>
                    </div>
                    <p>${row['market_cap']:.2f}</p>
                </div>
                """, unsafe_allow_html=True)

# --- Data Visualization Section ---
st.markdown("---")
st.subheader("Crypto Data Visualization")
if not df_latest.empty:
    with st.container(border=True):
        st.markdown("<h4>Comparison of Selected Coin Metrics</h4>", unsafe_allow_html=True)
        if selected_coins:
            selected_data = df_latest[df_latest['symbol'].isin(selected_coins)]
            # Use the internal metric name for the plot's y-axis
            fig_compare = px.bar(
                selected_data,
                x='name',
                y=selected_metric_internal,
                color='name',
                # Set the custom color palette here
                color_discrete_sequence=["#1984c5", "#22a7f0", "#63bff0", "#a7d5ed", "#e2e2e2", "#e1a692", "#de6e56", "#e14b31", "#c23728"],
                # Use the display metric name for the title and labels
                title=f"Comparison of {selected_display_metric}",
                labels={'name': 'Coin Name', selected_metric_internal: f'{selected_display_metric} (USD)'}
            )
            # Add a transition animation to the chart
            fig_compare.update_layout(transition_duration=500)
            st.plotly_chart(fig_compare, use_container_width=True)
        else:
            st.warning("Select at least one coin in the sidebar to see the comparison.")

    # NEW SECTION: Embed TradingView Historical Chart
    st.markdown("---")
    st.subheader("Historical Price Chart (via TradingView)")
    st.info("This chart is embedded from TradingView and displays complete historical data in real-time.")
    if selected_coin_historical:
        tv_symbol = f"BINANCE:{selected_coin_historical}USDT"
        
        # HTML for the TradingView widget
        tradingview_html = f"""
        <div class="tradingview-widget-container">
          <div id="tradingview_widget_container"></div>
          <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
          <script type="text/javascript">
          new TradingView.widget(
          {{
          "width": "100%",
          "height": 500,
          "symbol": "{tv_symbol}",
          "interval": "D",
          "timezone": "Etc/UTC",
          "theme": "dark",
          "style": "1",
          "locale": "en",
          "toolbar_bg": "#f1f3f6",
          "enable_publishing": false,
          "hide_side_toolbar": false,
          "allow_symbol_change": true,
          "container_id": "tradingview_widget_container"
        }}
          );
          </script>
        </div>
        """
        
        # Use st.components.v1.html to embed the widget
        components.html(tradingview_html, height=500)


# --- DATA TABLE SECTION ---
st.markdown("---")
st.subheader("Crypto Data Table")
if not df_latest.empty:
    st.dataframe(df_latest.sort_values(by='cmc_rank', ascending=True)[[
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
