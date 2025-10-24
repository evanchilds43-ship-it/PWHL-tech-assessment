# fetch_weather.py

from meteostat import Daily
from datetime import datetime
import pandas as pd
import logging
import os

# Setup
#------------------------------------------------------------
# Define weather stations for each city and global parameters

STATIONS = {
    "Toronto": "71508",
    "Montreal": "71627",
    "New York": "72502",
    "Boston": "72509",
    "Seattle": "72793",
    "Ottawa": "71063",
    "Minnesota": "72658",
    "Vancouver": "71892"
}

START = datetime(2025, 1, 1)
END = datetime(2025, 2, 28)

DATA_DIR = "../data"
os.makedirs(DATA_DIR, exist_ok=True)

LOG_FILE = os.path.join(DATA_DIR, "fetch_weather.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

#------------------------------------------------------------

# Fetch Weather Data
# Retrieves daily weather for a single city using its station ID

def fetch_city_weather(city, station_id):
    try:
        data = Daily(station_id, START, END)
        df = data.fetch()

        if df.empty:
            logging.warning(f"No weather data available for {city} in the given date range")
            return None

        df.reset_index(inplace=True)
        df["city"] = city.strip().lower()
        logging.info(f"Fetched weather data for {city}: {len(df)} rows")
        return df

    except Exception as e:
        logging.error(f"Error fetching weather for {city}: {e}")
        return None


# Main Process
# Loops through all cities, fetches weather data, and saves the combined result

def main():
    all_data = []

    for city, station_id in STATIONS.items():
        df = fetch_city_weather(city, station_id)
        if df is not None:
            all_data.append(df)

    if not all_data:
        logging.error("No weather data retrieved for any city")
        print("No weather data available for any city.")
        return

    full_df = pd.concat(all_data, ignore_index=True)
    output_file = os.path.join(DATA_DIR, "weather_jan_feb_2025.csv")
    full_df.to_csv(output_file, index=False)

    logging.info(f"Saved combined weather data to {output_file}")
    print(f"Weather data saved to {output_file}")


# Entry Point
if __name__ == "__main__":
    main()
