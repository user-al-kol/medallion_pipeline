# gold/main.py

import os                     # Provides access to operating system functionality
import time                   # Used for sleeping between checks
import logging                # Used for application-wide logging
import subprocess             # Used to execute external commands (spark-submit)
from datetime import date     # Used to get today's date
from common.utils import logger  # Import custom logger configuration function

# Initialize and configure the logger with INFO level and output file
logger(logging.INFO, "gold_container.log")

# Full absolute path to the log file
log_file = "/app/logs/gold_container.log"

# Time interval (in seconds) between filesystem checks
CHECK_INTERVAL = 3  # seconds

# Calculate today's date as a string (used for log inspection)
today_date = date.today().strftime("%Y-%m-%d")

def job_already_ran():
    """
    Checks whether the Spark Gold ingestion job
    has already run today by scanning the log file.
    """

    logging.debug("Entering job_already_ran() function")

    # If the log file does not exist, the job has not run yet
    if not os.path.exists(log_file):
        logging.info("Log file does not exist yet. Job has not run.")
        return False

    # Open the log file for reading
    with open(log_file, "r") as f:
        # Iterate through each log line
        for line in f:
            # Check for today's date and the specific job start message
            if today_date in line and "Starting Spark job GoldIngestion" in line:
                logging.info("Spark job has already run today.")
                return True

    # If no matching log entry is found
    logging.info("Spark job has NOT run today.")
    return False

# Retrieve SOURCE directory from environment variables
SOURCE = os.environ.get("SOURCE")
logging.info(f"SOURCE: {SOURCE}")

# Retrieve DESTINATION directory from environment variables
DESTINATION = os.environ.get("DESTINATION")
logging.info(f"DESTINATION: {DESTINATION}")

# Log environment variable values for debugging purposes
logging.info(f"SOURCE environment variable: {SOURCE}")
logging.info(f"DESTINATION environment variable: {DESTINATION}")


# MAIN LOOP
logging.info("Starting Gold container monitoring loop")

while True:
    logging.info("Beginning new monitoring cycle")

    try:
        # List files in the SOURCE directory
        source_files = os.listdir(SOURCE)
        logging.debug(f"Files in SOURCE directory: {source_files}")

        # Check if silver.db exists and the job has not already run today
        if "silver.db" in source_files and not job_already_ran():
            logging.info("silver.db found in SOURCE directory")
            logging.info(f"Files in SOURCE directory: {source_files}")
            logging.info("Starting Spark job GoldIngestion")

            try:
                # Execute Spark job using spark-submit
                subprocess.run(
                    [
                        "spark-submit",
                        "gold_code/gold_data_transformations.py",
                        SOURCE,
                        DESTINATION
                    ],
                    check=True,       # Raise exception if command fails
                    stdout=None,      # Inherit stdout
                    stderr=None       # Inherit stderr
                )

                logging.info("Spark job GoldIngestion completed successfully")

            except Exception as e:
                # Log full stack trace if Spark job execution fails
                logging.exception(f"An exception occurred while running Spark job: {e}")

        else:
            # Case where silver.db is missing or job already ran today
            logging.info("silver.db not found or job already ran today")

    except Exception as e:
        # Catch any unexpected errors in the monitoring loop
        logging.exception(f"Unexpected exception in monitoring loop: {e}")

    # Sleep before next iteration
    
    time.sleep(CHECK_INTERVAL)
