# prepare_clean_data.py

import pandas as pd
import os
import logging

# Setup
#-------------------------------------------------------
# Define paths for raw data, cleaned output, and logging

DATA_DIR = "../data"
TICKET_FILE = os.path.join(DATA_DIR, "pwhl_ticket_sales.csv")
SECTION_FILE = os.path.join(DATA_DIR, "game_section_capacity.csv")
WEATHER_FILE = os.path.join(DATA_DIR, "weather_jan_feb_2025.csv")

OUTPUT_DIR = os.path.join(DATA_DIR, "cleaned")
os.makedirs(OUTPUT_DIR, exist_ok=True)

LOG_FILE = os.path.join(DATA_DIR, "prepare_clean_data.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    filemode="w"
)

#-------------------------------------------------------

logging.info("Starting data cleaning and preparation process")


# Load Raw Datasets
try:
    logging.info("Loading raw datasets")
    tickets = pd.read_csv(TICKET_FILE)
    sections = pd.read_csv(SECTION_FILE)
    weather = pd.read_csv(WEATHER_FILE)
    logging.info(f"Datasets loaded successfully: "
                 f"{len(tickets)} tickets, {len(sections)} sections, {len(weather)} weather rows")
except Exception as e:
    logging.error(f"Error loading datasets: {e}")
    raise


# Clean Ticket Sales Data
logging.info(f"Cleaning ticket sales data ({len(tickets)} rows before cleaning)")

# Convert data types and normalize string fields
tickets["event_date"] = pd.to_datetime(tickets["event_date"], format="%m/%d/%Y", errors="coerce")
tickets["ticket_price"] = pd.to_numeric(tickets["ticket_price"], errors="coerce").astype("Float64")
tickets["num_tickets"] = pd.to_numeric(tickets["num_tickets"], errors="coerce").astype("Int64")
tickets["total_spend"] = pd.to_numeric(tickets["total_spend"], errors="coerce").astype("Float64")
tickets["section"] = tickets["section"].astype(str).str.strip().str.lower()
tickets["purchase_channel"] = tickets["purchase_channel"].astype(str).str.strip().str.lower()
tickets["acct_id"] = tickets["acct_id"].astype(str).str.strip()
tickets["row"] = pd.to_numeric(tickets["row"], errors="coerce").astype("Int64")
tickets["seat"] = pd.to_numeric(tickets["seat"], errors="coerce").astype("Int64")

# Drop invalid rows and handle missing values
before_drop = len(tickets)
tickets = tickets.dropna(subset=["event_date", "num_tickets", "total_spend"])
logging.info(f"Dropped {before_drop - len(tickets)} invalid ticket rows with missing essential fields")

tickets["ticket_price"].fillna(0)
tickets["row"].fillna(0)
tickets["seat"].fillna(0)
tickets["section"].fillna("unknown")
tickets["purchase_channel"].fillna("unknown")
tickets["acct_id"].fillna("unknown")

logging.info(f"Ticket sales cleaned successfully: {len(tickets)} rows remain")


# Clean Section Capacity Data
logging.info(f"Cleaning section capacity data ({len(sections)} rows)")

sections["event_date"] = pd.to_datetime(sections["event_date"], format="%m/%d/%Y", errors="coerce")
sections["section_capacity"] = pd.to_numeric(sections["section_capacity"], errors="coerce").astype("Int64")
sections["section"] = sections["section"].astype(str).str.strip().str.lower()
sections["home_city"] = sections["home_city"].astype(str).str.strip().str.lower()

before_drop_sections = len(sections)
sections = sections.dropna(subset=["event_date", "section", "home_city", "section_capacity"])
logging.info(f"Dropped {before_drop_sections - len(sections)} invalid section rows")
logging.info(f"Section capacity cleaned successfully: {len(sections)} valid rows remain")


# Clean Weather Data
logging.info(f"Cleaning weather data ({len(weather)} rows)")

# Rename columns for consistency and clarity
weather.rename(columns={
    "time": "event_date",
    "tavg": "avg_temp_c",
    "tmin": "min_temp_c",
    "tmax": "max_temp_c",
    "prcp": "precip_mm",
    "snow": "snow_mm",
    "wdir": "wind_dir_deg",
    "wspd": "wind_speed_kmh",
    "wpgt": "wind_gust_kmh",
    "pres": "pressure_hpa",
    "tsun": "sun_hours",
    "city": "city"
}, inplace=True)

# Convert data types and fill missing numeric values
weather["event_date"] = pd.to_datetime(weather["event_date"])
numeric_cols = [
    "avg_temp_c", "min_temp_c", "max_temp_c",
    "precip_mm", "snow_mm", "wind_dir_deg",
    "wind_speed_kmh", "wind_gust_kmh", "pressure_hpa", "sun_hours"
]

for col in numeric_cols:
    weather[col] = pd.to_numeric(weather[col], errors="coerce")
    missing = weather[col].isna().sum()
    if missing > 0:
        if col in ["precip_mm", "snow_mm"]:
            logging.warning(f"Filling {missing} missing values in {col} with 0")
            weather[col] = weather[col].fillna(0)
        else:
            logging.warning(f"Keeping {missing} missing values in {col} as NaN (unknown)")

weather["city"] = weather["city"].astype(str).str.lower()
before_drop_weather = len(weather)
weather = weather.dropna(subset=["event_date", "city"])
logging.info(f"Dropped {before_drop_weather - len(weather)} rows with null city or date")
logging.info(f"Weather data cleaned successfully: {len(weather)} rows remain")


# Build Dimension Tables
logging.info("Creating dimension tables")

unique_dates = tickets["event_date"].drop_duplicates().sort_values().reset_index(drop=True)
dim_date = pd.DataFrame({"event_date": unique_dates})
dim_date["date_id"] = (
    dim_date["event_date"].dt.year * 10000 +
    dim_date["event_date"].dt.month * 100 +
    dim_date["event_date"].dt.day
)
dim_date["day"] = dim_date["event_date"].dt.day
dim_date["month"] = dim_date["event_date"].dt.month
dim_date["month_name"] = dim_date["event_date"].dt.strftime("%B").str.lower()
dim_date["year"] = dim_date["event_date"].dt.year
dim_date["weekday"] = dim_date["event_date"].dt.weekday + 1
dim_date["weekday_name"] = dim_date["event_date"].dt.strftime("%A").str.lower()
dim_date["is_weekend"] = dim_date["weekday"].isin([6, 7]).astype(int)
dim_date["week_of_year"] = dim_date["event_date"].dt.isocalendar().week

dim_venue = sections[["event_date", "section", "home_city", "section_capacity"]].drop_duplicates().reset_index(drop=True)
dim_venue["venue_id"] = dim_venue.index + 1

dim_weather = weather.drop_duplicates(subset=["event_date", "city"]).reset_index(drop=True)
dim_weather["weather_id"] = dim_weather.index + 1

dim_channel = (
    pd.DataFrame({"purchase_channel": tickets["purchase_channel"].astype(str).str.strip().str.lower()})
    .drop_duplicates()
    .reset_index(drop=True)
)
dim_channel["channel_id"] = dim_channel.index + 1

dim_customer = pd.DataFrame({"acct_id": tickets["acct_id"].unique()})
dim_customer["customer_id"] = dim_customer.index + 1

logging.info("Dimension tables created successfully")


# Build Fact Table
logging.info("Building fact table")

fact_sales = tickets.merge(dim_date, on="event_date", how="left")
fact_sales = fact_sales.merge(dim_channel, on="purchase_channel", how="left")
fact_sales = fact_sales.merge(dim_customer, on="acct_id", how="left")

# Joining fact table to venue section capcity data. Include event_date in the dim_venue table to allow for total capacity calculations

fact_sales = fact_sales.merge(
    sections[["event_date", "section", "home_city"]],
    on=["event_date", "section"],
    how="left"
)

fact_sales = fact_sales.merge(
    dim_venue[["event_date", "section", "home_city", "venue_id"]],
    on=["event_date", "section", "home_city"],
    how="left"
)

# Now that we have the city, weather_id can be pulled in
fact_sales = fact_sales.merge(
    dim_weather[["event_date", "city", "weather_id"]],
    left_on=["event_date", "home_city"],
    right_on=["event_date", "city"],
    how="left"
)

fact_sales = fact_sales[[
    "event_date", "date_id", "venue_id", "customer_id", "channel_id", "weather_id",
    "num_tickets", "ticket_price", "total_spend"
]]

logging.info(f"Fact table built successfully with {len(fact_sales)} rows")


#  Save Outputs 
logging.info("Saving cleaned dimension and fact tables")

dim_date.to_csv(os.path.join(OUTPUT_DIR, "dim_date.csv"), index=False)
dim_venue.to_csv(os.path.join(OUTPUT_DIR, "dim_venue.csv"), index=False)
dim_weather.to_csv(os.path.join(OUTPUT_DIR, "dim_weather.csv"), index=False)
dim_channel.to_csv(os.path.join(OUTPUT_DIR, "dim_channel.csv"), index=False)
dim_customer.to_csv(os.path.join(OUTPUT_DIR, "dim_customer.csv"), index=False)
fact_sales.to_csv(os.path.join(OUTPUT_DIR, "fact_ticket_sales.csv"), index=False)

logging.info("All tables saved successfully")
logging.info("Data cleaning and preparation process completed")

print(f"Cleaned fact and dimension tables saved in {OUTPUT_DIR}")
