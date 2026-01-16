# bronze/main.py

import subprocess  # used to execute external commands (spark-submit)
import time  # used for sleep intervals in the polling loop
import os  # used for environment variables and filesystem operations
import logging  # used for structured logging
from datetime import date  # used to get today's date for folder detection
from common.utils import logger # import logger function from utils package

# Configure logger
logger(logging.INFO, "bronze_container.log")



# Read SOURCE directory from environment variables
SOURCE = os.environ.get("SOURCE")
logging.info(f"SOURCE directory set to: {SOURCE}")

# Read DESTINATION directory from environment variables
DESTINATION = os.environ.get("DESTINATION")
logging.info(f"DESTINATION directory set to: {DESTINATION}")

# Polling interval in seconds
CHECK_INTERVAL = 3  # seconds
logging.info(f"Check interval set to {CHECK_INTERVAL} seconds")

# Get today's date in YYYY-MM-DD format
today_date = date.today().strftime("%Y-%m-%d")
logging.info(f"Today's date resolved as: {today_date}")

# Expected folder name created by Spark partitioning
todays_file = f"Processing_Date={today_date}"
logging.info(f"Expected partition folder name: {todays_file}")

# Define log_file
log_file = "/app/logs/bronze_container.log"
def job_already_ran():
    """
    Checks whether the Bronze Spark job has already run today.
    This is done by scanning the log file for today's date
    and a specific log message indicating job start.
    """

    logging.info("Checking if Bronze Spark job already ran today")

    # If log file does not exist, the job has never run
    if not os.path.exists(log_file):
        logging.info("Log file does not exist yet — job has not run")
        return False

    # Open the log file and scan line by line
    with open(log_file, "r") as f:
        for line in f:
            # Check for today's date and the specific job start message
            if today_date in line and "Starting Spark job BronzeIngestion" in line:
                logging.info("Found evidence that Bronze job already ran today")
                return True

    # If no matching log entry was found
    logging.info("No log evidence found — Bronze job has not run today")
    return False

# Infinite loop to continuously monitor the SOURCE directory
while True:
    logging.info("Scanning SOURCE directory for today's partition")

    # List all files and folders inside SOURCE directory
    current_files = os.listdir(SOURCE)
    
    # Check if today's partition exists AND the job has not run yet
    if todays_file in current_files and not job_already_ran():
        logging.info(f"Detected today's partition folder: {todays_file}")
        logging.info("Starting Spark job BronzeIngestion")

        try:
            logging.info("Starting Upsert Spark job for Bronze layer")

            # Execute Spark job responsible for Bronze upsert
            subprocess.run(
                [
                    "spark-submit",  # Spark submit command
                    "bronze_code/upsert_data.py",  # Spark job script path
                    SOURCE,  # pass SOURCE directory as argument
                    DESTINATION # pass DESTINATION directory as argument
                ],
                check=True,  # raise exception if Spark job fails
                stdout=None,  # inherit stdout
                stderr=None   # inherit stderr
            )

            logging.info("Spark job BronzeIngestion completed successfully")

        except Exception as e:
            # Log full exception traceback if Spark job fails
            logging.exception(f"An exception occured: {e}")

    else:
        # Case when today's partition does not exist or job already ran
        logging.info("No new data for today or job already executed")

    # Sleep before next polling iteration
    time.sleep(CHECK_INTERVAL)
