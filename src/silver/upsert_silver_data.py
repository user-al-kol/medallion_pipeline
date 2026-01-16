# silver/upsert_silver_data.py
import sys
import os
import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, coalesce, lit, to_date, when, round
from datetime import date
import logging
from common.utils import logger # import logger function from utils package
from common.utils import logger # import logger function from utils package
from common.spark_utils import read_jdbc_table, write_jdbc_table # import spark functions from spark_utils package

# Configure logger
logger(logging.INFO, "silver_container.log")

def run_upsert(spark, SOURCE, DESTINATION):
    # Function to perform upsert from bronze to silver database

    # Define JDBC URL for bronze database
    jdbc_url_bronze = f"jdbc:sqlite:{SOURCE}/bronze.db"  # SOURCE = /app/silver
    bronze_table_name = "bronze_data_transformed"  # Name of bronze table
    bronze_sqlite_path = f"{SOURCE}/bronze.db"  # Path to bronze SQLite file
    driver = "org.sqlite.JDBC"

    # Define JDBC URL for silver database
    jdbc_url_silver = f"jdbc:sqlite:{DESTINATION}/silver.db"  # DESTINATION = /app/gold
    silver_table_name = "silver_data"  # Name of silver table
    silver_sqlite_path = f"{DESTINATION}/silver.db"  # Path to silver SQLite file

    # Read bronze table into Spark DataFrame
    bronze_data_transformed = read_jdbc_table(spark,jdbc_url_bronze,bronze_table_name,driver)
    try:
        # Try reading existing silver table
        silver_data_df = read_jdbc_table(spark,jdbc_url_silver,silver_table_name,driver)
        logging.info("silver.db was read successfully")

    except Exception as e:
        # If silver table does not exist, log the exception and create it
        logging.info(f"Exception reading SQLite table: {e}")
        logging.info(f"Creating table {silver_table_name}")
        create_table = """
        CREATE TABLE silver_data (
        Student_ID TEXT,
        Name TEXT,
        Age INTEGER NOT NULL,
        Gender TEXT NOT NULL,
        Grade_Level TEXT,
        Course_ID TEXT,
        Course_Name TEXT,
        Enrollment_Date TEXT,
        Completion_Date TEXT,
        Status TEXT NOT NULL,
        Final_Grade TEXT NOT NULL,
        Attendance_Rate REAL NOT NULL,
        Time_Spent_on_Course_hrs REAL NOT NULL,
        Assignments_Completed INTEGER NOT NULL,
        Quizzes_Completed INTEGER NOT NULL,
        Forum_Posts INTEGER NOT NULL,
        Messages_Sent INTEGER NOT NULL,
        Quiz_Average_Score REAL NOT NULL,
        Assignment_Scores TEXT,
        Assignment_Average_Score REAL NOT NULL,
        Project_Score REAL NOT NULL,
        Extra_Credit REAL NOT NULL,
        Overall_Performance REAL NOT NULL,
        Feedback_Score REAL NOT NULL,
        Parent_Involvement TEXT NOT NULL,
        Demographic_Group TEXT NOT NULL,
        Internet_Access TEXT NOT NULL,
        Learning_Disabilities TEXT NOT NULL,
        Preferred_Learning_Style TEXT NOT NULL,
        Language_Proficiency TEXT NOT NULL,
        Participation_Rate TEXT NOT NULL,
        Completion_Time_Days INTEGER,
        Performance_Score REAL NOT NULL,
        Course_Completion_Rate TEXT NOT NULL,
        Processing_Date TEXT
        );
        """
        # Create silver table if it doesn't exist
        with sqlite3.connect(silver_sqlite_path) as conn:
            conn.execute(create_table)
            conn.commit()
        # Read newly created silver table into DataFrame
        silver_data_df = read_jdbc_table(spark,jdbc_url_silver,silver_table_name,driver)
        logging.info("silver_data created as empty table.")

    # Log the contents of silver_data_df for inspection
    logging.info("silver_data_df: ")
    silver_data_df.select(["Student_ID","Name","Course_ID","Processing_Date"]).show(30)

    # Alias DataFrames for merge operation
    s = silver_data_df.alias("s")
    n = bronze_data_transformed.alias("n")

    # Define join condition on Student_ID and Course_ID
    join_condition = (col("s.Student_ID") == col("n.Student_ID")) & (col("s.Course_ID") == col("n.Course_ID"))

    # Perform full outer join
    merged_df = s.join(n, join_condition, "fullouter")
    logging.info("merged_df")
    merged_df.select(
        col("s.Student_ID").alias("s_Student_ID"),
        col("s.Name").alias("s_Name"),
        col("s.Course_ID").alias("s_Course_ID"),
        col("s.Processing_Date").alias("s_Processing_Date"),
        col("n.Student_ID").alias("n_Student_ID"),
        col("n.Name").alias("n_Name"),
        col("n.Course_ID").alias("n_Course_ID"),
        col("n.Processing_Date").alias("n_Processing_Date"),
    ).show(30)

    # Coalesce columns from bronze and silver for upsert
    final_df = merged_df.select(
        coalesce(col("n.Student_ID"), col("s.Student_ID")).alias("Student_ID"),
        coalesce(col("n.Name"), col("s.Name")).alias("Name"),
        coalesce(col("n.Age"), col("s.Age")).alias("Age"),
        coalesce(col("n.Gender"), col("s.Gender")).alias("Gender"),
        coalesce(col("n.Grade_Level"), col("s.Grade_Level")).alias("Grade_Level"),
        coalesce(col("n.Course_ID"), col("s.Course_ID")).alias("Course_ID"),
        coalesce(col("n.Course_Name"), col("s.Course_Name")).alias("Course_Name"),
        coalesce(col("n.Enrollment_Date"), col("s.Enrollment_Date")).alias("Enrollment_Date"),
        coalesce(col("n.Completion_Date"), col("s.Completion_Date")).alias("Completion_Date"),
        coalesce(col("n.Status"), col("s.Status")).alias("Status"),
        coalesce(col("n.Final_Grade"), col("s.Final_Grade")).alias("Final_Grade"),
        coalesce(col("n.Attendance_Rate"), col("s.Attendance_Rate")).alias("Attendance_Rate"),
        coalesce(col("n.Time_Spent_on_Course_hrs"), col("s.Time_Spent_on_Course_hrs")).alias("Time_Spent_on_Course_hrs"),
        coalesce(col("n.Assignments_Completed"), col("s.Assignments_Completed")).alias("Assignments_Completed"),
        coalesce(col("n.Quizzes_Completed"), col("s.Quizzes_Completed")).alias("Quizzes_Completed"),
        coalesce(col("n.Forum_Posts"), col("s.Forum_Posts")).alias("Forum_Posts"),
        coalesce(col("n.Messages_Sent"), col("s.Messages_Sent")).alias("Messages_Sent"),
        coalesce(col("n.Quiz_Average_Score"), col("s.Quiz_Average_Score")).alias("Quiz_Average_Score"),
        coalesce(col("n.Assignment_Scores"), col("s.Assignment_Scores")).alias("Assignment_Scores"),
        coalesce(col("n.Assignment_Average_Score"), col("s.Assignment_Average_Score")).alias("Assignment_Average_Score"),
        coalesce(col("n.Project_Score"), col("s.Project_Score")).alias("Project_Score"),
        coalesce(col("n.Extra_Credit"), col("s.Extra_Credit")).alias("Extra_Credit"),
        coalesce(col("n.Overall_Performance"), col("s.Overall_Performance")).alias("Overall_Performance"),
        coalesce(col("n.Feedback_Score"), col("s.Feedback_Score")).alias("Feedback_Score"),
        coalesce(col("n.Parent_Involvement"), col("s.Parent_Involvement")).alias("Parent_Involvement"),
        coalesce(col("n.Demographic_Group"), col("s.Demographic_Group")).alias("Demographic_Group"),
        coalesce(col("n.Internet_Access"), col("s.Internet_Access")).alias("Internet_Access"),
        coalesce(col("n.Learning_Disabilities"), col("s.Learning_Disabilities")).alias("Learning_Disabilities"),
        coalesce(col("n.Preferred_Learning_Style"), col("s.Preferred_Learning_Style")).alias("Preferred_Learning_Style"),
        coalesce(col("n.Language_Proficiency"), col("s.Language_Proficiency")).alias("Language_Proficiency"),
        coalesce(col("n.Participation_Rate"), col("s.Participation_Rate")).alias("Participation_Rate"),
        coalesce(col("n.Completion_Time_Days"), col("s.Completion_Time_Days")).alias("Completion_Time_Days"),
        coalesce(col("n.Performance_Score"), col("s.Performance_Score")).alias("Performance_Score"),
        coalesce(col("n.Course_Completion_Rate"), col("s.Course_Completion_Rate")).alias("Course_Completion_Rate"),
        coalesce(col("s.Processing_Date"), col("n.Processing_Date")).alias("Processing_Date")
    )
    logging.info("final_df")
    final_df.select(["Student_ID","Name","Course_ID","Processing_Date"]).show(30)

    # Delete existing keys before upserting
    keys_df = final_df.select("Student_ID", "Course_ID").distinct().collect()
    with sqlite3.connect(silver_sqlite_path) as conn:
        cursor = conn.cursor()
        for row in keys_df:
            cursor.execute(
                "DELETE FROM silver_data WHERE Student_ID = ? AND Course_ID = ?",
                (row["Student_ID"], row["Course_ID"])
            )
        conn.commit()

    # Write final DataFrame to silver database
    write_jdbc_table(final_df,jdbc_url_silver,silver_table_name,driver)
    # Read new silver data to verify upsert
    new_silver_data_df = read_jdbc_table(spark,jdbc_url_silver,silver_table_name,driver)
    logging.info("new_silver_data_df")
    new_silver_data_df.select(["Student_ID","Name","Course_ID","Processing_Date","Completion_Time_Days","Performance_Score","Course_Completion_Rate"])\
        .show(30)
    
if __name__ == '__main__':
    try:
        # Read command-line arguments for source and destination paths
        SOURCE = sys.argv[1]
        DESTINATION = sys.argv[2]

        logging.info(f"Script started. Source: {SOURCE}, Destination: {DESTINATION}")

        # Create Spark session
        logging.info("Starting Spark Session: SilverIngestion")
        spark = SparkSession.builder.appName("SilverIngestion") \
            .config("spark.jars", "/usr/local/spark/jars/sqlite-jdbc-3.45.1.0.jar") \
            .getOrCreate()

        # Execute upsert logic
        run_upsert(spark, SOURCE, DESTINATION)

        logging.info("Data upserted successfully")

    except Exception as e:
        logging.exception(f"An exception occurred in main: {e}")
