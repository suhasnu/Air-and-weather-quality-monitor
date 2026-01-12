import requests
import psycopg2
import os
import sys
from datetime import datetime

# --- CONFIGURATION ---
# "os.getenv" tells the script to look for these secret keys inside GitHub's settings
# instead of reading them from this file.
API_KEY = os.getenv("API_KEY")
DB_URI = os.getenv("DB_URI")

# Safety Check: If keys are missing, stop the script
if not API_KEY or not DB_URI:
    print("Error: Missing Environment Variables. Are they set in GitHub Secrets?")
    sys.exit(1)
    
# Cities to monitor
CITIES = [
    {"name": "Berlin", "lat": 52.520, "lon": 13.405},
    {"name": "Fulda", "lat": 50.551, "lon": 9.675},
    {"name": "Frankfurt", "lat": 50.110, "lon": 8.682},
    {"name": "Munich", "lat": 48.135, "lon": 11.582}
]

def create_table_if_not_exists():
    """Creates the table automatically if it doesn't exist."""
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
        print(f"Critical Database Error: {e}")
        sys.exit(1) # Stop script if DB fails

def fetch_data(city):
    """Hits the API for a single city."""
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={city['lat']}&lon={city['lon']}&appid={API_KEY}"
    weather_url = f"https://api.openweathermap.org/data/2.5/weather?lat={city['lat']}&lon={city['lon']}&appid={API_KEY}&units=metric"
    
    try:
        # Get Air Quality
        aq_res = requests.get(url).json()
        # Get Weather (for Temp)
        w_res = requests.get(weather_url).json()
        
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
    """Inserts one row of data into Supabase."""
    sql = """
    INSERT INTO air_quality_logs (city, aqi, pm2_5, pm10, co, temperature)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    try:
        conn = psycopg2.connect(DB_URI)
        cur = conn.cursor()
        cur.execute(sql, (data['city'], data['aqi'], data['pm2_5'], data['pm10'], data['co'], data['temperature']))
        conn.commit()
        cur.close()
        conn.close()
        print(f"Saved data for {data['city']}")
    except Exception as e:
        print(f"Insert Error: {e}")

def main():
    print(f"--- ETL JOB STARTED AT {datetime.now()} ---")
    
    # 1. Setup DB
    create_table_if_not_exists()
    
    # 2. Loop through cities
    for city in CITIES:
        data = fetch_data(city)
        if data:
            save_to_db(data)
            
    print("--- JOB FINISHED ---")

if __name__ == "__main__":
    main()