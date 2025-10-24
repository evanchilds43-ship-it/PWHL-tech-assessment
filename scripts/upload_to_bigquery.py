# upload_to_bigquery.py

from google.cloud import bigquery
import pandas as pd
import os
import logging

# Setup
#------------------------------------------------------------
# Configure project directories, logging, and BigQuery client

DATA_DIR = "../data/cleaned"
LOG_DIR = "../data"
PROJECT_ID = "grand-icon-475820-e5"
DATASET_ID = "pwhl_analytics_assessment"

LOG_FILE = os.path.join(LOG_DIR, "upload_to_bigquery.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    filemode="w"
)

client = bigquery.Client(project=PROJECT_ID)

#------------------------------------------------------------

# Upload Function
# Uploads a CSV file from the cleaned data directory into a BigQuery table

def upload_csv_to_bq(filename, table_name, schema=None, cluster_fields=None):
    file_path = os.path.join(DATA_DIR, filename)
    logging.info(f"Starting upload for {filename} → {table_name}")

    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        print(f"File not found: {file_path}")
        return

    try:
        df = pd.read_csv(file_path)
        logging.info(f"Loaded {len(df)} rows from {filename}")

        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=(schema is None),
            schema=schema,
        )

        if cluster_fields:
            job_config.clustering_fields = cluster_fields
            logging.info(f"Applied clustering fields: {cluster_fields}")

        print(f"Uploading to table: {table_id} ...")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        table = client.get_table(table_id)
        logging.info(f"Upload complete: {table.num_rows} rows loaded into {table_id}")
        print(f"Loaded {table.num_rows} rows into {table_id}")

    except Exception as e:
        logging.error(f"Error uploading {filename} to {table_name}: {e}", exc_info=True)
        print(f"Error uploading {filename}: {e}")


# Main Upload Process
# Uploads all dimension and fact tables to BigQuery

def main():
    logging.info("Starting BigQuery upload process")

    try:
        # Dimension tables
        upload_csv_to_bq("dim_venue.csv", "dim_venue")
        upload_csv_to_bq("dim_weather.csv", "dim_weather")
        upload_csv_to_bq("dim_date.csv", "dim_date")
        upload_csv_to_bq("dim_channel.csv", "dim_channel")
        upload_csv_to_bq("dim_customer.csv", "dim_customer")

        # Fact table
        upload_csv_to_bq(
            "fact_ticket_sales.csv",
            "fact_ticket_sales",
            cluster_fields=["event_date", "venue_id", "channel_id"]
        )

        logging.info("All uploads completed successfully")
        print("All tables uploaded successfully.")

    except Exception as e:
        logging.critical(f"Fatal error during upload process: {e}", exc_info=True)
        print(f"Fatal error during upload: {e}")


# Entry Point
if __name__ == "__main__":
    main()
