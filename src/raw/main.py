# raw/main.py
import os  # OS module for paths and environment variables
import time  # time module to use sleep for polling
import shutil  # shutil module to perform file copy operations
import logging  # logging module to log info and errors
from common.utils import logger # import logger function from utils package

# Configure logger
logger(logging.INFO,"raw_container.log")

# Source and destination paths obtained from environment variables
SOURCE = os.environ.get("SOURCE")  # source folder path
DESTINATION = os.environ.get("DESTINATION")  # destination folder path
CHECK_INTERVAL = 3  # polling interval in seconds

# Keep track of files already seen in the source folder
already_seen = set(os.listdir(SOURCE))

# Main loop: continuously watch for new files in the source folder
while True:
    logging.info("Waiting for incoming files")

    # List current files in source folder
    current_files = set(os.listdir(SOURCE))

    # Identify new files that haven't been processed yet
    new_files = current_files - already_seen

    if new_files:
        logging.info(f"New files detected: {new_files}")

        try:
            # Optional: create full paths for new files (currently unused)
            full_paths = [os.path.join(SOURCE, f) for f in new_files]

            # Copy each new file from source to destination
            for f in new_files:
                shutil.copy2(os.path.join(SOURCE, f),
                             os.path.join(DESTINATION, f))

            # Log successful transfer
            logging.info(f"Files {new_files} transfered successfully.")

            # Update already_seen set with the current files
            already_seen = current_files

        except Exception as e:
            # Log any exceptions that occur during file copying
            logging.exception(f"An exception occured {e}")

    # Wait for the defined interval before checking again
    time.sleep(CHECK_INTERVAL)
