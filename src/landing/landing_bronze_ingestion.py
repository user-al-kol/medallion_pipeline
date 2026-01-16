# landing_bronze_ingestion.py

import sys  # import sys module to read command-line arguments
import os  # import os module for filesystem operations
import shutil  # import shutil module to copy directories
import time  # import time module for sleep intervals
import logging  # import logging module to log messages
from datetime import date, timedelta  # import date and timedelta for processing dates
from pyspark.sql import SparkSession  # import SparkSession to create Spark context
from pyspark.sql.functions import lit  # import lit function to add literal column in DataFrame
from common.utils import logger # import logger function from utils package

# Configure logger
logger(logging.INFO,"landing_container.log")

# Performs incremental ingestion of raw files to landing zone
def ingest(spark,src_dir, dst_dir, new_files):
    """
    src_dir: source directory containing raw files
    dst_dir: destination directory for landing files
    new_files: list of new file names to process
    """
    logging.info("Starting ingestion process.")
    
    try:
        # temporary directory to write processed data
        tmp_dir = os.path.join("/tmp", os.path.basename(dst_dir))
        os.makedirs(tmp_dir, exist_ok=True)  # create tmp directory if it does not exist
        logging.info(f"Temporary directory ready at {tmp_dir}")

        # Log directories for debugging
        logging.info(f"Source folder: {src_dir}")
        logging.info(f"Destination folder: {dst_dir}")
        logging.info(f"Files to process: {new_files}")

        # Iterate over all new files
        for i, file in enumerate(new_files):
            processed_date = date.today()  # today's date for tagging records
            logging.info(f"Processing file {i+1}/{len(new_files)}: {file} with processing date {processed_date}")

            # Only process files ending with .csv
            if file.endswith('.csv'):
                src_path = os.path.join(src_dir, file)  # full path to source file
                logging.info(f"Reading CSV file from: {src_path}")

                # Read CSV file into a Spark DataFrame
                df = spark.read.csv(
                    path=src_path,
                    header=True,  # first row contains headers
                    inferSchema=True  # automatically infer column types
                )
                logging.info(f"File {file} loaded. Number of rows: {df.count()}")

                # Check if the file contains more than just the header
                if df.count() > 1:
                    logging.info("The file has data, proceeding with transformation.")

                    # Add a Processing_Date column with today's date
                    df_new = df.withColumn(
                        "Processing_Date",
                        lit(processed_date.isoformat())
                    )

                    # Write the DataFrame to tmp_dir as partitioned CSV
                    output_path = tmp_dir
                    df_new.write \
                        .format('csv') \
                        .option('header', 'true') \
                        .partitionBy('Processing_Date') \
                        .mode('append') \
                        .save(output_path)
                    logging.info(f"Data from {file} written to temporary landing directory successfully.")

                else:
                    logging.info(f"{file} contains only header row, skipping.")

            else:
                logging.warning(f"{file} is not a CSV file, skipping.")

            time.sleep(3)  # wait 3 seconds between processing files

        # Copy files from tmp_dir to final destination
        final_dir = os.path.join('/app/', dst_dir)
        logging.info(f"Copying processed files from {tmp_dir} to final destination {final_dir}")
        shutil.copytree(tmp_dir, final_dir, dirs_exist_ok=True)
        logging.info("All files successfully copied to final destination.")

    except Exception as e:
        logging.exception(f"An exception occurred during ingestion: {e}")  # log any exceptions

# Entry point of the script
if __name__ == '__main__':
    try:
        # Read command-line arguments
        SOURCE = sys.argv[1]  # source directory path
        DESTINATION = sys.argv[2]  # destination directory path
        new_files = sys.argv[3:]  # list of new files to process

        logging.info(f"Script started. Source: {SOURCE}, Destination: {DESTINATION}")
        logging.info(f"Files received for ingestion: {new_files}")
        logging.info(f"Starting Spark Session: Landing_Bronze_Ingestion")
        # Start SparkSession
        spark = SparkSession.builder.appName("Landing_Bronze_Ingestion").getOrCreate()
        logging.info("SparkSession started.")
        # Call the ingestion function with provided arguments
        ingest(spark,SOURCE, DESTINATION, new_files)

        logging.info("Ingestion process completed successfully.")

    except Exception as e:
        logging.exception(f"An exception occurred in main: {e}")  # log any exceptions in main
