import requests
import psycopg2
import os
import sys
from datetime import datetime

# --- CONFIGURATION ---
# We use os.getenv so we don't expose our passwords in the code.
# These are set in the "Settings > Secrets" tab on GitHub.
API_KEY = os.getenv("API_KEY")
DB_URI = os.getenv("DB_URI")

# Safety check: Stop the script immediately if the keys are missing.
if not API_KEY or not DB_URI:
    print("Error: Missing Environment Variables. Are they set in GitHub Secrets?")
    sys.exit(1)
    
# Our list of targets: We need manual coordinates because the API requires them.
CITIES = [
    {"name": "Berlin", "lat": 52.520, "lon": 13.405},
    {"name": "Fulda", "lat": 50.551, "lon": 9.675},
    {"name": "Frankfurt", "lat": 50.110, "lon": 8.682},
    {"name": "Munich", "lat": 48.135, "lon": 11.582},
    {"name": "Stuttgart", "lat": 48.775, "lon": 9.182},
    {"name": "Heidelberg", "lat": 49.398, "lon": 8.672},
    {"name": "Kassel", "lat": 51.312, "lon": 9.479},
    {"name": "Hamburg", "lat": 53.551, "lon": 9.993},
    {"name": "Hannover", "lat": 52.375, "lon": 9.732},
    {"name": "Cologne", "lat": 50.937, "lon": 6.960},
    {"name": "Bengaluru", "lat": 12.971, "lon": 77.594},
    {"name": "Pune", "lat": 18.520, "lon": 73.856}
]

def create_table_if_not_exists():
    """Checks if the table exists in Supabase. If not, it builds it automatically."""
    commands = """
        CREATE TABLE IF NOT EXISTS air_quality_logs (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            city VARCHAR(50),
            aqi INTEGER,
            pm2_5 FLOAT,
            pm10 FLOAT,
            co FLOAT,
            temperature FLOAT
        )
    """
    try:
        conn = psycopg2.connect(DB_URI)
        cur = conn.cursor()
        cur.execute(commands)
        conn.commit()
        cur.close()
        conn.close()
        print("Table check successful.")
    except Exception as e:
        # If we can't connect to the DB, there is no point in continuing.
        print(f"Critical Database Error: {e}")
        sys.exit(1)

def fetch_data(city):
    """Hits the OpenWeatherMap API to get the latest stats for a specific city."""
    # URL 1: For Pollution (PM2.5, CO, etc.)
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={city['lat']}&lon={city['lon']}&appid={API_KEY}"
    # URL 2: For Weather (Temperature)
    weather_url = f"https://api.openweathermap.org/data/2.5/weather?lat={city['lat']}&lon={city['lon']}&appid={API_KEY}&units=metric"
    
    try:
        aq_res = requests.get(url).json()
        w_res = requests.get(weather_url).json()
        
        # Clean up the messy API response and return just what we need
        return {
            "city": city['name'],
            "aqi": aq_res['list'][0]['main']['aqi'],
            "pm2_5": aq_res['list'][0]['components']['pm2_5'],
            "pm10": aq_res['list'][0]['components']['pm10'],
            "co": aq_res['list'][0]['components']['co'],
            "temperature": w_res['main']['temp']
        }
    except Exception as e:
        print(f"API Error for {city['name']}: {e}")
        return None

def save_to_db(data):
    """Takes the cleaned data and inserts a new row into Supabase."""
    sql = """
    INSERT INTO air_quality_logs (city, aqi, pm2_5, pm10, co, temperature)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    try:
        conn = psycopg2.connect(DB_URI)
        cur = conn.cursor()
        # We use a tuple here to prevent SQL Injection attacks
        cur.execute(sql, (data['city'], data['aqi'], data['pm2_5'], data['pm10'], data['co'], data['temperature']))
        conn.commit()
        cur.close()
        conn.close()
        print(f"Saved data for {data['city']}")
    except Exception as e:
        print(f"Insert Error: {e}")

def main():
    print(f"--- ETL JOB STARTED AT {datetime.now()} ---")
    
    # Step 1: Ensure the database is ready
    create_table_if_not_exists()
    
    # Step 2: Loop through every city and fetch data
    for city in CITIES:
        data = fetch_data(city)
        if data:
            save_to_db(data)
            
    print("--- JOB FINISHED ---")

if __name__ == "__main__":
    main()