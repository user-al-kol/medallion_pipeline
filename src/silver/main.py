# silver/main.py

import os  # Used for filesystem operations and environment variables
import time  # Used for sleep intervals in polling loop
import logging  # Used for logging status and debugging
import subprocess  # Used to call Spark jobs as external processes
from datetime import date  # Used to get today's date for log checks
from common.utils import logger # import logger function from utils package

# Configure logger
logger(logging.INFO, "silver_container.log")

# Full path to the Silver container log file
log_file = "/app/logs/silver_container.log"

# Interval to check for bronze.db file (in seconds)
CHECK_INTERVAL = 3

# ---------------- TODAY'S DATE ----------------

# Calculate today's date as string (YYYY-MM-DD)
today_date = date.today().strftime("%Y-%m-%d")
logging.info(f"Today's date resolved as {today_date}")

# ---------------- HELPER FUNCTION ----------------

def job_already_ran():
    """
    Checks if the Spark SilverIngestion job has already run today
    by scanning the log file for a specific entry.
    """
    logging.info("Checking if SilverIngestion job already ran today")

    # If log file does not exist, the job has not run
    if not os.path.exists(log_file):
        logging.info("Log file does not exist; job has not run")
        return False

    # Read log file and check for today's run
    with open(log_file, "r") as f:
        for line in f:
            if today_date in line and "Starting Spark job SilverIngestion" in line:
                logging.info("Job already ran today according to log")
                return True

    logging.info("Job has not run today according to log")
    return False

# ---------------- ENVIRONMENT VARIABLES ----------------

# Get source path from environment variable
SOURCE = os.environ.get("SOURCE")
logging.info(f"Resolved SOURCE environment variable: {SOURCE}")

# Get destination path from environment variable
DESTINATION = os.environ.get("DESTINATION")
logging.info(f"Resolved DESTINATION environment variable: {DESTINATION}")

# ---------------- MAIN POLLING LOOP ----------------

# Infinite loop to check for bronze.db and launch Silver Spark job
while True:
    # Check if bronze.db exists in SOURCE folder and job hasn't run today
    if "bronze.db" in os.listdir(SOURCE) and not job_already_ran():
        logging.info("bronze.db found in SOURCE")
        logging.info("Starting Spark job SilverIngestion")

        try:
            # Run the Spark job as a subprocess
            subprocess.run([
                "spark-submit",
                "silver_code/business_transformations.py",
                SOURCE
            ], check=True, stdout=None, stderr=None)

            logging.info("Spark job BusinessTransformations completed successfully")

            # Only if the previous succeeded
            subprocess.run([
                "spark-submit",
                "silver_code/upsert_silver_data.py",
                SOURCE,  
                DESTINATION
            ], check=True)

            logging.info("Spark job SilverIngestion completed successfully")

        except Exception as e:
            # Log exception with stack trace
            logging.exception(f"An exception occurred while running Spark job: {e}")

    else:
        # Log that either bronze.db is missing or job already ran
        logging.info("No bronze.db found or SilverIngestion job already ran")

    # Sleep before checking again
    
    time.sleep(CHECK_INTERVAL)