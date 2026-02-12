import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import os

# --- 1. SETUP & CONNECTION ---
st.set_page_config(page_title="UrbanAir Live Monitor", layout="wide")

# Connection String to Supabase
# (This allows the dashboard to read the data the robot saved)
DB_URI = "postgresql://postgres.smdsanxatzeejkwtkncn:UrbanAir2026@aws-1-eu-west-1.pooler.supabase.com:5432/postgres"

# Coordinates Dictionary
# We need this because the database stores pollution levels, but not locations.
# This tells the map exactly where to put the dots.
CITY_COORDINATES = {
    "Berlin": [52.520, 13.405],
    "Fulda": [50.551, 9.675],
    "Frankfurt": [50.110, 8.682],
    "Munich": [48.135, 11.582],
    "Stuttgart": [48.775, 9.182],
    "Heidelberg": [49.398, 8.672],
    "Kassel": [51.312, 9.479],
    "Hamburg": [53.551, 9.993],
    "Hannover": [52.375, 9.732],
    "Cologne": [50.937, 6.960],
    "Bengaluru": [12.971, 77.594],
    "Pune": [18.520, 73.856]
}

@st.cache_data(ttl=60) 
def load_data():
    """Fetches the latest data from the DB. Cached for 60s to prevent spamming the database."""
    try:
        conn = psycopg2.connect(DB_URI)
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
    city_list = sorted(df['city'].unique().tolist())
    city_list.insert(0, "All Cities")
    
    # User selects a city here
    selected_city = st.sidebar.selectbox("📍 Select a City:", city_list)
    
    # Filter the dataframe based on the user's choice
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

# Button to manually refresh the data
if st.sidebar.button('🔄 Refresh Data'):
    st.cache_data.clear()

if not df_filtered.empty:
    # --- KPI Row ---
    # Grab the most recent record to show current stats
    latest = df_filtered.iloc[0]
    
    col1, col2, col3, col4 = st.columns(4)
    display_city = selected_city if selected_city != "All Cities" else "Latest Update (" + latest['city'] + ")"
    
    col1.metric("Selected View", display_city)
    col2.metric("Current AQI", latest['aqi'])
    col3.metric("PM2.5 Level", f"{latest['pm2_5']} µg/m³")
    col4.metric("Last Updated", str(latest['timestamp'])[11:16])

    # --- NEW: MAP SECTION ---
    st.subheader("🗺️ Live Pollution Map")
    
    # Logic: If viewing "All Cities", show the latest status for EVERY city.
    # If viewing one city, just show that one dot.
    if selected_city == "All Cities":
        map_df = df.sort_values('timestamp', ascending=False).groupby('city').head(1).copy()
        zoom_level = 4
    else:
        map_df = df_filtered.sort_values('timestamp', ascending=False).head(1).copy()
        zoom_level = 8

    # Map the city names to the Lat/Lon dictionary we created at the top
    map_df['lat'] = map_df['city'].map(lambda x: CITY_COORDINATES.get(x, [0,0])[0])
    map_df['lon'] = map_df['city'].map(lambda x: CITY_COORDINATES.get(x, [0,0])[1])

    # Plot the interactive map
    fig_map = px.scatter_mapbox(
        map_df, 
        lat="lat", 
        lon="lon", 
        color="aqi",           # Color changes based on AQI (Green/Yellow/Red)
        size="pm2_5",          # Size changes based on pollution amount
        size_max=20,         
        hover_name="city", 
        hover_data=["pm2_5", "temperature"],
        zoom=zoom_level, 
        mapbox_style="open-street-map",
        height=400
    )
    st.plotly_chart(fig_map, use_container_width=True)

    # --- CHARTS ---
    st.subheader(f"📉 Pollution Trends: {selected_city}")
    
    # Line chart to show history
    fig = px.line(df_filtered, x='timestamp', y='pm2_5', color='city', 
                  title="PM2.5 Concentration Over Time", markers=True)
    st.plotly_chart(fig, use_container_width=True)

    # Comparison Chart (only visible when viewing All Cities)
    if selected_city == "All Cities":
        st.subheader("📊 City Comparison (Latest Average)")
        avg_df = df.groupby('city')['pm2_5'].mean().reset_index()
        fig_bar = px.bar(avg_df, x='city', y='pm2_5', color='city', title="Average PM2.5 by City")
        st.plotly_chart(fig_bar, use_container_width=True)

    # Raw Data Table (Hidden by default)
    with st.expander("View Raw Database Records"):
        st.dataframe(df_filtered)

else:
    st.warning("No data found yet! Wait for the ETL pipeline to run.")