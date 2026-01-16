# bronze/upsert_data.py

import sys  # Used to read command-line arguments
import sqlite3  # Used for direct SQLite operations (DDL + DELETE)
import os  # Used for filesystem operations
import logging  # Used for application logging
from pyspark.sql import SparkSession  # Spark entry point
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType  # Schema definitions
from pyspark.sql.functions import col, coalesce, lit  # Spark SQL helper functions
from datetime import date  # Used to get today's date
from common.utils import logger # import logger function from utils package
from common.spark_utils import read_jdbc_table, write_jdbc_table # import spark functions from spark_utils package

# Configure logger
logger(logging.INFO, "bronze_container.log")

# ---------------- MAIN UPSERT FUNCTION ----------------

def run_upsert(spark, SOURCE, DESTINATION):
    """
    Executes Bronze layer upsert logic using Spark and SQLite.
    """
    logging.info("Starting run_upsert function")

    # Get today's date as string (YYYY-MM-DD)
    today_date = date.today().strftime("%Y-%m-%d")
    logging.info(f"Processing date resolved as {today_date}")

    # Base source path
    base_path = SOURCE
    # Path where today's CSV files are expected
    folder_path = f"{base_path}/Processing_Date={today_date}"
    logging.info(f"Resolved input folder path: {folder_path}")

    # ---------------- CSV SCHEMA DEFINITION ----------------

    # Explicit schema for incoming CSV files
    schema = StructType([
        StructField("Student_ID", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("Gender", StringType(), True),
        StructField("Grade_Level", StringType(), True),
        StructField("Course_ID", StringType(), True),
        StructField("Course_Name", StringType(), True),
        StructField("Enrollment_Date", StringType(), True),
        StructField("Completion_Date", StringType(), True),
        StructField("Status", StringType(), True),
        StructField("Final_Grade", StringType(), True),
        StructField("Attendance_Rate", DoubleType(), True),
        StructField("Time_Spent_on_Course_hrs", DoubleType(), True),
        StructField("Assignments_Completed", IntegerType(), True),
        StructField("Quizzes_Completed", IntegerType(), True),
        StructField("Forum_Posts", IntegerType(), True),
        StructField("Messages_Sent", IntegerType(), True),
        StructField("Quiz_Average_Score", DoubleType(), True),
        StructField("Assignment_Scores", StringType(), True),
        StructField("Assignment_Average_Score", DoubleType(), True),
        StructField("Project_Score", DoubleType(), True),
        StructField("Extra_Credit", DoubleType(), True),
        StructField("Overall_Performance", DoubleType(), True),
        StructField("Feedback_Score", DoubleType(), True),
        StructField("Parent_Involvement", StringType(), True),
        StructField("Demographic_Group", StringType(), True),
        StructField("Internet_Access", StringType(), True),
        StructField("Learning_Disabilities", StringType(), True),
        StructField("Preferred_Learning_Style", StringType(), True),
        StructField("Language_Proficiency", StringType(), True),
        StructField("Participation_Rate", StringType(), True),
        StructField("Completion_Time_Days", IntegerType(), True),
        StructField("Performance_Score", DoubleType(), True),
        StructField("Course_Completion_Rate", DoubleType(), True)
    ])

    logging.info("CSV schema defined successfully")

    # ---------------- READ NEW DATA ----------------

    logging.info("Reading CSV files into Spark DataFrame")

    # Read CSV files with schema and header
    new_data_df = spark.read.option("header", "true").schema(schema).csv(folder_path)

    # Remove duplicates based on business key and add Processing_Date column
    new_data_df = new_data_df.dropDuplicates(["Student_ID", "Course_ID"]) \
                             .withColumn("Processing_Date", lit(today_date))

    logging.info("New data loaded, deduplicated and enriched with Processing_Date")

    # Debug preview of new data
    logging.info("new_data_df preview:")
    new_data_df.select(["Student_ID", "Name", "Course_ID", "Processing_Date"]).show(30)

    # ---------------- JDBC CONFIGURATION ----------------

    # JDBC connection string for SQLite
    jdbc_url = f"jdbc:sqlite:{DESTINATION}/bronze.db"
    # JDBC driver
    driver = "org.sqlite.JDBC"
    # Target table name
    table_name = "bronze_data"
    # Physical SQLite database path
    sqlite_path = f"{DESTINATION}/bronze.db"
    
    logging.info(f"JDBC URL resolved: {jdbc_url}")

    # ---------------- READ OR CREATE BRONZE TABLE ----------------

    try:
        # Attempt to read existing bronze table
        bronze_data_df = read_jdbc_table(spark,jdbc_url,table_name,driver)       

        logging.info("bronze.db was read successfully")

    except Exception as e:
        # If table does not exist, create it using SQLite
        logging.warning(f"Error reading SQLite table: {e}")
        logging.info(f"Creating table {table_name}")

        create_table = """
        CREATE TABLE IF NOT EXISTS bronze_data (
            Student_ID TEXT, 
            Name TEXT, 
            Age INTEGER, 
            Gender TEXT, 
            Grade_Level TEXT,
            Course_ID TEXT, 
            Course_Name TEXT, 
            Enrollment_Date TEXT, 
            Completion_Date TEXT,
            Status TEXT, 
            Final_Grade TEXT, 
            Attendance_Rate REAL, 
            Time_Spent_on_Course_hrs REAL,
            Assignments_Completed INTEGER, 
            Quizzes_Completed INTEGER, 
            Forum_Posts INTEGER,
            Messages_Sent INTEGER, 
            Quiz_Average_Score REAL, 
            Assignment_Scores TEXT,
            Assignment_Average_Score REAL, 
            Project_Score REAL, Extra_Credit REAL,
            Overall_Performance REAL, 
            Feedback_Score REAL, 
            Parent_Involvement TEXT,
            Demographic_Group TEXT, 
            Internet_Access TEXT, 
            Learning_Disabilities TEXT,
            Preferred_Learning_Style TEXT, 
            Language_Proficiency TEXT, 
            Participation_Rate TEXT,
            Completion_Time_Days INTEGER, 
            Performance_Score REAL, 
            Course_Completion_Rate REAL,
            Processing_Date TEXT
        )
        """

        # Execute table creation
        with sqlite3.connect(sqlite_path) as conn:
            conn.execute(create_table)
            conn.commit()

        # Read the newly created empty table
        bronze_data_df = read_jdbc_table(spark,jdbc_url,table_name,driver)
        
        logging.info("bronze_data table created and loaded as empty DataFrame")

    # Debug preview of existing bronze data
    logging.info("bronze_data_df preview:")
    bronze_data_df.select(["Student_ID", "Name", "Course_ID", "Processing_Date"]).show(30)

    # ---------------- UPSERT LOGIC ----------------

    logging.info("Starting UPSERT logic")

    # Alias existing bronze data
    b = bronze_data_df.alias("b")

    # Alias new incoming data
    n = new_data_df.alias("n")

    # Define join condition for UPSERT key
    join_condition = (
        (col("b.Student_ID") == col("n.Student_ID")) &
        (col("b.Course_ID") == col("n.Course_ID"))
    )

    logging.info("Join condition for UPSERT defined")

    # Perform full outer join to merge old and new records
    merged_df = b.join(n, join_condition, "fullouter")

    # Debug preview of merged dataset
    print("merged_df:")
    merged_df.select(
        col("b.Student_ID").alias("b_Student_ID"),
        col("b.Name").alias("b_Name"),
        col("b.Course_ID").alias("b_Course_ID"),
        col("b.Processing_Date").alias("b_Processing_Date"),
        col("n.Student_ID").alias("n_Student_ID"),
        col("n.Name").alias("n_Name"),
        col("n.Course_ID").alias("n_Course_ID"),
        col("n.Processing_Date").alias("n_Processing_Date"),
    ).show(30)

    # Resolve final column values preferring new data over old
    final_df = merged_df.select(
        coalesce(col("n.Student_ID"), col("b.Student_ID")).alias("Student_ID"),
        coalesce(col("n.Name"), col("b.Name")).alias("Name"),
        coalesce(col("n.Age"), col("b.Age")).alias("Age"),
        coalesce(col("n.Gender"), col("b.Gender")).alias("Gender"),
        coalesce(col("n.Grade_Level"), col("b.Grade_Level")).alias("Grade_Level"),
        coalesce(col("n.Course_ID"), col("b.Course_ID")).alias("Course_ID"),
        coalesce(col("n.Course_Name"), col("b.Course_Name")).alias("Course_Name"),
        coalesce(col("n.Enrollment_Date"), col("b.Enrollment_Date")).alias("Enrollment_Date"),
        coalesce(col("n.Completion_Date"), col("b.Completion_Date")).alias("Completion_Date"),
        coalesce(col("n.Status"), col("b.Status")).alias("Status"),
        coalesce(col("n.Final_Grade"), col("b.Final_Grade")).alias("Final_Grade"),
        coalesce(col("n.Attendance_Rate"), col("b.Attendance_Rate")).alias("Attendance_Rate"),
        coalesce(col("n.Time_Spent_on_Course_hrs"), col("b.Time_Spent_on_Course_hrs")).alias("Time_Spent_on_Course_hrs"),
        coalesce(col("n.Assignments_Completed"), col("b.Assignments_Completed")).alias("Assignments_Completed"),
        coalesce(col("n.Quizzes_Completed"), col("b.Quizzes_Completed")).alias("Quizzes_Completed"),
        coalesce(col("n.Forum_Posts"), col("b.Forum_Posts")).alias("Forum_Posts"),
        coalesce(col("n.Messages_Sent"), col("b.Messages_Sent")).alias("Messages_Sent"),
        coalesce(col("n.Quiz_Average_Score"), col("b.Quiz_Average_Score")).alias("Quiz_Average_Score"),
        coalesce(col("n.Assignment_Scores"), col("b.Assignment_Scores")).alias("Assignment_Scores"),
        coalesce(col("n.Assignment_Average_Score"), col("b.Assignment_Average_Score")).alias("Assignment_Average_Score"),
        coalesce(col("n.Project_Score"), col("b.Project_Score")).alias("Project_Score"),
        coalesce(col("n.Extra_Credit"), col("b.Extra_Credit")).alias("Extra_Credit"),
        coalesce(col("n.Overall_Performance"), col("b.Overall_Performance")).alias("Overall_Performance"),
        coalesce(col("n.Feedback_Score"), col("b.Feedback_Score")).alias("Feedback_Score"),
        coalesce(col("n.Parent_Involvement"), col("b.Parent_Involvement")).alias("Parent_Involvement"),
        coalesce(col("n.Demographic_Group"), col("b.Demographic_Group")).alias("Demographic_Group"),
        coalesce(col("n.Internet_Access"), col("b.Internet_Access")).alias("Internet_Access"),
        coalesce(col("n.Learning_Disabilities"), col("b.Learning_Disabilities")).alias("Learning_Disabilities"),
        coalesce(col("n.Preferred_Learning_Style"), col("b.Preferred_Learning_Style")).alias("Preferred_Learning_Style"),
        coalesce(col("n.Language_Proficiency"), col("b.Language_Proficiency")).alias("Language_Proficiency"),
        coalesce(col("n.Participation_Rate"), col("b.Participation_Rate")).alias("Participation_Rate"),
        coalesce(col("n.Completion_Time_Days"), col("b.Completion_Time_Days")).alias("Completion_Time_Days"),
        coalesce(col("n.Performance_Score"), col("b.Performance_Score")).alias("Performance_Score"),
        coalesce(col("n.Course_Completion_Rate"), col("b.Course_Completion_Rate")).alias("Course_Completion_Rate"),
        coalesce(col("b.Processing_Date"), col("n.Processing_Date")).alias("Processing_Date")
    )

    logging.info("Final upsert DataFrame constructed")

    # Debug preview of final dataset
    logging.info("final_df preview:")
    final_df.select(["Student_ID", "Name", "Course_ID", "Processing_Date"]).show(30)

    # ---------------- DELETE + APPEND STRATEGY ----------------

    logging.info("Collecting keys for DELETE step")

    # Collect distinct business keys
    keys_df = final_df.select("Student_ID", "Course_ID").distinct().collect()

    # Delete existing rows for those keys
    with sqlite3.connect(sqlite_path) as conn:
        cursor = conn.cursor()
        for row in keys_df:
            cursor.execute(
                "DELETE FROM bronze_data WHERE Student_ID = ? AND Course_ID = ?",
                (row["Student_ID"], row["Course_ID"])
            )
        conn.commit()

    logging.info("Old records deleted from SQLite")

    # Append new merged data
    write_jdbc_table(final_df,jdbc_url,table_name,driver)
    
    logging.info("Final data appended to bronze table")

    # Reload and display updated bronze table
    new_bronze_data_df = read_jdbc_table(spark,jdbc_url,table_name,driver)

    logging.info("new_bronze_data_df")
    new_bronze_data_df.select(["Student_ID", "Name", "Course_ID", "Processing_Date"]).show(30)

# ---------------- SCRIPT ENTRY POINT ----------------
if __name__ == '__main__':
    try:
        # Read command-line arguments
        SOURCE = sys.argv[1]
        DESTINATION = sys.argv[2]

        logging.info(f"Script started. Source: {SOURCE}, Destination: {DESTINATION}")

        # Create Spark session
        logging.info("Starting Spark Session: BronzeIngestion")
        spark = SparkSession.builder.appName("BronzeIngestion") \
            .config("spark.jars", "/usr/local/spark/jars/sqlite-jdbc-3.45.1.0.jar") \
            .getOrCreate()

        # Execute upsert logic
        run_upsert(spark, SOURCE, DESTINATION)

        logging.info("Data upserted successfully")

    except Exception as e:
        logging.exception(f"An exception occured in main: {e}")
