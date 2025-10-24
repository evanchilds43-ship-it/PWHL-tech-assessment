# run_pipeline.py
"""
Runs the full PWHL data pipeline sequentially:
1. Fetch weather data
2. Clean and transform datasets
3. Upload results to BigQuery
"""

import subprocess
import logging
import os
from datetime import datetime

# Setup
#------------------------------------------------
# Configure logging and define the scripts to run

LOG_FILE = os.path.join("../data", "pipeline_run.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    filemode="a"
)

SCRIPTS = [
    "fetch_weather.py",
    "prepare_clean_data.py",
    "upload_to_bigquery.py"
]

#------------------------------------------------

# Utility Function
# Runs an individual script and logs the outcome

def run_script(script_name):
    logging.info(f"Starting {script_name}")
    try:
        script_path = os.path.join(os.path.dirname(__file__), script_name)
        subprocess.run(["python", script_path], check=True)
        logging.info(f"{script_name} completed successfully")
    except subprocess.CalledProcessError as e:
        logging.error(f"{script_name} failed with error: {e}")
        raise


# Main Process
# Executes each step in sequence and logs total duration

def main():
    logging.info("Pipeline run started")
    start_time = datetime.now()

    try:
        for script in SCRIPTS:
            run_script(script)
        logging.info("Pipeline completed successfully")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
    finally:
        duration = datetime.now() - start_time
        logging.info(f"Total pipeline run time: {duration}")
        logging.info("Pipeline run finished")


# Entry Point
if __name__ == "__main__":
    main()
