# silver/business_transformation.py
import sys
import os
import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, coalesce, lit, to_date, when, round
from datetime import date
import logging

# ---------------- LOGGER CONFIGURATION ----------------

# Directory where log files will be stored
LOG_DIR = "/app/logs"

# Create log directory if it does not exist
os.makedirs(LOG_DIR, exist_ok=True)

# Full path to the bronze container log file
log_file = f"{LOG_DIR}/silver_container.log"

# Configure logging format, level and handlers
logging.basicConfig(
    level=logging.INFO,  # Log INFO and above
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",  # Standard log format
    handlers=[
        logging.StreamHandler(),  # Log to stdout (container-friendly)
        logging.FileHandler(log_file)  # Log to file
    ]
)

def business_transformations(spark, SOURCE):
    # ----------------- Date calculation -----------------
    # Calculate today's date as string in YYYY-MM-DD format
    today_date = date.today().strftime("%Y-%m-%d")
    logging.info(f"Today's date calculated: {today_date}")

    # ----------------- JDBC and table setup -----------------
    # Define JDBC URL for bronze database
    jdbc_url_bronze = f"jdbc:sqlite:{SOURCE}/bronze.db"  # SOURCE = /app/silver
    # Define the table name in bronze database
    bronze_table_name = "bronze_data"
    # Define the path to the bronze database file
    bronze_sqlite_path = f"{SOURCE}/bronze.db"

    # ----------------- Read bronze_data -----------------
    # Load bronze_data table into Spark DataFrame
    bronze_data_df = spark.read.format("jdbc") \
            .option("url", jdbc_url_bronze) \
            .option("dbtable", bronze_table_name) \
            .option("driver", "org.sqlite.JDBC") \
            .load()
    logging.info("bronze_data_df")
    bronze_data_df.select(["Student_ID","Name","Course_ID","Processing_Date"]).show(30)

    # ----------------- Filter today's records -----------------
    # Keep only records with today's Processing_Date
    today_bronze_df = bronze_data_df.filter(col("Processing_Date") == today_date)
    logging.info("today_bronze_df")
    today_bronze_df.select(["Student_ID","Name","Course_ID","Processing_Date"]).show(30)

    # ----------------- Handle duplicates -----------------
    logging.info(f"Count before removing duplicates: {today_bronze_df.count()}")
    today_bronze_nodups_df = today_bronze_df.dropDuplicates()
    logging.info(f"Count after removing duplicates: {today_bronze_nodups_df.count()}")

    # ----------------- Handle missing or null values -----------------
    logging.info(f"Count before dropping rows with missing critical data: {today_bronze_nodups_df.count()}")
    # Drop rows missing critical columns
    today_dropped_df = today_bronze_nodups_df.dropna(subset=['Student_ID','Course_ID','Enrollment_Date'])
    logging.info(f"Count after dropping rows with missing critical data: {today_dropped_df.count()}")

    # Fill missing values with defaults
    today_filled_df = today_dropped_df.fillna({
        "Age": 0,
        "Gender": "Unknown",
        "Status": "In-progress",
        "Final_Grade": "N/A",
        "Attendance_Rate": 0.0,
        "Time_Spent_on_Course_hrs": 0.0,
        "Assignments_Completed": 0,
        "Quizzes_Completed": 0,
        "Forum_Posts": 0,
        "Messages_Sent": 0,
        "Quiz_Average_Score": 0.0,
        "Assignment_Average_Score": 0.0,
        "Project_Score": 0.0,
        "Extra_Credit": 0.0,
        "Overall_Performance": 0.0,
        "Feedback_Score": 0.0,
        "Parent_Involvement": "Unknown",
        "Demographic_Group": "Unknown",
        "Internet_Access": "Unknown",
        "Learning_Disabilities": "Unknown",
        "Preferred_Learning_Style": "Unknown",
        "Language_Proficiency": "Unknown",
        "Participation_Rate": "Unknown",
        "Completion_Time_Days": 0,
        "Performance_Score": 0.0,
        "Course_Completion_Rate": 0.0,
        "Completion_Date": '12/31/9999'
    })
    logging.info('today_filled_df')
    today_filled_df.select(["Student_ID","Name","Course_ID","Processing_Date"]).show(30)

    # ----------------- Standardize date formats -----------------
    today_formated_df = today_filled_df.withColumn('Enrollment_Date', to_date(col('Enrollment_Date'),'M/d/yyyy'))\
                                       .withColumn('Completion_Date',to_date(col('Completion_Date'),'M/d/yyyy'))
    logging.info('today_formated_df')
    today_formated_df.select(["Student_ID","Name","Course_ID","Processing_Date","Enrollment_Date","Completion_Date"]).show(30)

    # ----------------- Check logical consistency -----------------
    # Filter rows where Completion_Date >= Enrollment_Date
    today_constistent_df = today_formated_df.filter(col('Completion_Date') >= col('Enrollment_Date'))
    logging.info('today_constistent_df')
    today_constistent_df.select(["Student_ID","Name","Course_ID","Processing_Date","Enrollment_Date","Completion_Date"]).show(30)

    # ----------------- Business transformations -----------------
    # Calculate Completion_Time_Days
    todays_completion_time = today_constistent_df.withColumn(
        "Completion_Time_Days",
        (col("Completion_Date") - col("Enrollment_Date")).cast("int")
    )
    logging.info("Completion_Time_Days")
    todays_completion_time.select(["Student_ID","Name","Course_ID","Processing_Date","Completion_Time_Days"]).show(30)

    # Calculate Performance_Score
    todays_score = todays_completion_time.withColumn(
        "Performance_Score",
        round(
            (col('Quiz_Average_Score') * 0.2) +
            (col('Assignment_Average_Score') * 0.2) +
            (col('Project_Score') * 0.1),
            2
        )
    )
    logging.info("Performance_Score")
    todays_score.select(["Student_ID","Name","Course_ID","Processing_Date","Performance_Score"]).show(30)

    # Calculate Course_Completion_Rate
    todays_completion_rate = todays_score.withColumn(
        "Course_Completion_Rate",
        when(col("Completion_Time_Days") <= 90, "On-Time").otherwise('Delayed')
    )
    logging.info("Course_Completion_Rate")
    todays_completion_rate.select(["Student_ID","Name","Course_ID","Processing_Date","Course_Completion_Rate"]).show(30)

    # Save transformed data back to a new table in bronze database
    data_transformed = todays_completion_rate
    data_transformed.write.format("jdbc") \
        .option("url", jdbc_url_bronze) \
        .option("dbtable", "bronze_data_transformed") \
        .option("driver", "org.sqlite.JDBC") \
        .mode("overwrite")\
        .save()
    # Check if the table was writen successfully
    bronze_data_transformed_df = spark.read.format("jdbc")\
        .option("url", jdbc_url_bronze) \
        .option("dbtable", "bronze_data_transformed") \
        .option("driver", "org.sqlite.JDBC") \
        .load()
    # Preview bronze_data_transformed_df
    logging.info("bronze_data_transformed_df preview")
    bronze_data_transformed_df.show()
    logging.info("Transformed data written to bronze_data_transformed table successfully")

# ----------------- Main script execution -----------------
if __name__ == '__main__':
    try:
        # Get source path from command line arguments
        SOURCE = sys.argv[1]
        logging.info(f"Script started. Source: {SOURCE}")

        # Create Spark session for business transformations
        logging.info("Starting Spark Session: BusinessTransformations")
        spark = SparkSession.builder.appName("BusinessTransformationsSession")\
                .config("spark.jars", "/usr/local/spark/jars/sqlite-jdbc-3.45.1.0.jar") \
                .getOrCreate()

        # Execute the business transformations function
        business_transformations(spark, SOURCE)
        logging.info("Data transformed successfully")

    except Exception as e:
        # Log exception with stack trace for debugging
        logging.exception(f"An exception occurred in main: {e}")
