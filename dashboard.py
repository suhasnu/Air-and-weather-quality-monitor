import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import os

# --- 1. SETUP & CONNECTION ---
st.set_page_config(page_title="UrbanAir Live Monitor", layout="wide")

# Connect to database
# (We use the URI you confirmed works)
DB_URI = "postgresql://postgres.smdsanxatzeejkwtkncn:UrbanAir2026@aws-1-eu-west-1.pooler.supabase.com:5432/postgres"

@st.cache_data(ttl=60) 
def load_data():
    try:
        conn = psycopg2.connect(DB_URI)
        # Fetch all data sorted by time
        query = "SELECT * FROM air_quality_logs ORDER BY timestamp DESC"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error connecting to DB: {e}")
        return pd.DataFrame()

# --- 2. LOAD DATA ---
df = load_data()

# --- 3. SIDEBAR (The Control Panel) ---
st.sidebar.header("Filter Options")

if not df.empty:
    # Get list of unique cities from the database
    city_list = sorted(df['city'].unique().tolist())
    # Add an "All Cities" option for comparison
    city_list.insert(0, "All Cities")
    
    # THE INTERACTIVE WIDGET
    selected_city = st.sidebar.selectbox("📍 Select a City:", city_list)
    
    # Filter the data based on selection
    if selected_city != "All Cities":
        df_filtered = df[df['city'] == selected_city]
    else:
        df_filtered = df
else:
    st.sidebar.warning("No data found.")
    df_filtered = pd.DataFrame()

# --- 4. MAIN DASHBOARD UI ---
st.title("🌍 UrbanAir: Real-Time Engineering Pipeline")
st.markdown("This dashboard reads live data ingested by the **GitHub Actions ETL Pipeline**.")

if st.sidebar.button('🔄 Refresh Data'):
    st.cache_data.clear()

if not df_filtered.empty:
    # KPI Row (Shows stats for the selected city)
    # We take the first row of the FILTERED data (which is the latest entry)
    latest = df_filtered.iloc[0]
    
    col1, col2, col3, col4 = st.columns(4)
    # Logic: If 'All Cities' is selected, just show 'Multiple'
    display_city = selected_city if selected_city != "All Cities" else "Latest Update (" + latest['city'] + ")"
    
    col1.metric("Selected View", display_city)
    col2.metric("Current AQI", latest['aqi'])
    col3.metric("PM2.5 Level", f"{latest['pm2_5']} µg/m³")
    col4.metric("Last Updated", str(latest['timestamp'])[11:16])

    # Charts
    st.subheader(f"📉 Pollution Trends: {selected_city}")
    
    # Interactive Line Chart
    fig = px.line(df_filtered, x='timestamp', y='pm2_5', color='city', 
                  title="PM2.5 Concentration Over Time", markers=True)
    st.plotly_chart(fig, use_container_width=True)

    # Comparison Chart (Bar Chart) - Only show if "All Cities" is selected
    if selected_city == "All Cities":
        st.subheader("📊 City Comparison (Latest Average)")
        avg_df = df.groupby('city')['pm2_5'].mean().reset_index()
        fig_bar = px.bar(avg_df, x='city', y='pm2_5', color='city', title="Average PM2.5 by City")
        st.plotly_chart(fig_bar, use_container_width=True)

    # Raw Data
    with st.expander("View Raw Database Records"):
        st.dataframe(df_filtered)

else:
    st.warning("No data found yet! Wait for the ETL pipeline to run.")