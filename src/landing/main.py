# landing/main.py

import subprocess  # import subprocess to run external commands (spark-submit)
import time  # import time module to sleep between polling intervals
import os  # import os module to access environment variables and list directories
import logging  # import logging module to log messages
from common.utils import logger # import logger function from utils package

# Configure logger
logger(logging.INFO,"landing_container.log")

# Get environment variables for source and destination directories
SOURCE = os.environ.get("SOURCE")  # get the source folder path from environment
logging.info(f"Source folder: {SOURCE}")  # print source folder path for debugging

DESTINATION = os.environ.get("DESTINATION")  # get the destination folder path from environment
logging.info(f"Destination folder: {DESTINATION}")  # print destination folder path for debugging

CHECK_INTERVAL = 3  # seconds between each poll of the source directory

# Initialize the set of already seen files to avoid processing them again
already_seen = set(os.listdir(SOURCE))  # read initial files in the source folder

# Main loop to continuously check for new files
while True:
    logging.info("Waiting for incoming files")  # log that the script is waiting for new files
    current_files = set(os.listdir(SOURCE))  # get the current list of files in the source folder
    new_files = current_files - already_seen  # determine which files are new by set difference

    if new_files:  # if there are new files detected
        try:
            logging.info(f"New files detected: {new_files}")  # log the list of new files
            logging.info("Starting spark job.")  # log that the Spark job will start

            # Convert the set of new files to a list for ordered processing
            files_list = list(new_files)

            # Run PySpark ingestion job using spark-submit
            subprocess.run([
                "spark-submit",  # call Spark job
                "landing_code/landing_bronze_ingestion.py",  # script to run
                SOURCE,  # pass source directory as first argument
                DESTINATION,  # pass destination directory as second argument
                *files_list  # pass all new files as additional arguments
            ], check=True, stdout=None, stderr=None)  # raise exception if Spark job fails

            # Update the set of already seen files to include the newly processed ones
            already_seen = current_files

        except Exception as e:  # catch any exception during the Spark job execution
            logging.exception(f"An exception occured: {e}")  # log exception with traceback

    time.sleep(CHECK_INTERVAL)  # wait for CHECK_INTERVAL seconds before checking again
