import sys
import sqlite3
import pandas as pd
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, to_date, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
from common.spark_utils import read_jdbc_table, write_jdbc_table # import spark functions from spark_utils package
from datetime import date
from common.utils import logger # Import custom logger configuration function

# Configure logger
logger(logging.INFO,"gold_container.log")

def gold_data_transformations(spark,SOURCE,DESTINATION):

    # Define jdbc url.
    jdbc_silver_url = f"jdbc:sqlite:{SOURCE}/silver.db"
    jdbc_gold_url = f"jdbc:sqlite:{DESTINATION}/gold.db"
    # Define table name
    silver_table_name = "silver_data"
    student_table_name = "dim_student"
    course_table_name = "dim_course"
    fact_table_name = "fact_student_performance"
    driver = "org.sqlite.JDBC"
    # ---------------------------Reading Silver Table---------------------------
    # Calculate today's date.
    today_date = date.today().strftime("%Y-%m-%d")
    
    # Read the silver_data table
    silver_data_df = read_jdbc_table(spark,jdbc_silver_url,silver_table_name,driver)
    
    # Keep only the lines with the current date. 
    todays_silver_data_df = silver_data_df.filter(col("Processing_Date") == today_date)

    # ---------------------------Creating Dimension Tables---------------------------

    # Create dim_student if it doesn't exists
    try:
        # Try to read dim_student table.
        dim_student = read_jdbc_table(spark,jdbc_gold_url,student_table_name,driver)
        
    except Exception as e: 
        # If it doesn't exist make it.
        print(f"Exception while reading dim_student table: {e}")
        print("Creating table dim_student")
        # Define dim_student table schema.
        dim_student_schema = StructType([
            StructField("Student_ID", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("Age", IntegerType(), True),
            StructField("Gender", StringType(), True),
            StructField("Demographic_Group", StringType(), True),
            StructField("Internet_Access", StringType(), True),
            StructField("Learning_Disabilities", StringType(), True),
            StructField("Language_Proficiency", StringType(), True),
            StructField("Parent_Involvement", StringType(), True),
            ])
        # Make an empty spark dataframe.
        empty_dim_student_df = spark.createDataFrame([], dim_student_schema)
        # Make the dim_student table.
        write_jdbc_table(empty_dim_student_df,jdbc_gold_url,student_table_name,driver,"overwrite")
        
        # Read dim_student table know that it exists.
        dim_student = read_jdbc_table(spark,jdbc_gold_url,student_table_name,driver)
        
    # Print dim_student
    logging.info("dim_student")
    dim_student.show()

    # Create dim_course if it doesn't exists
    try:
        # Try to read dim_course table.
        dim_course = read_jdbc_table(spark,jdbc_gold_url,course_table_name,driver)
        
    except Exception as e: 
        # If it doesn't exist make it.
        logging.info(f"Exception while reading dim_student table: {e}")
        logging.info("Creating table dim_course")
        # Define dim_course table schema.
        dim_course_schema = StructType([
            StructField("Course_ID", StringType(), True),
            StructField("Course_Name", StringType(), True),
            StructField("Grade_Level", StringType(), True)
            ])
        # Make an empty spark dataframe.
        empty_dim_course_df = spark.createDataFrame([], dim_course_schema)
        # Make the dim_student table.
        write_jdbc_table(empty_dim_course_df,jdbc_gold_url,course_table_name,driver,"overwrite")
        
        # Read dim_course table know that it exists.
        dim_course = read_jdbc_table(spark,jdbc_gold_url,course_table_name,driver)
        
    # Print dim_student
    logging.info("dim_course")    
    dim_course.show()
    # ---------------------------Creating Fact Tables---------------------------

    # Create fact_performance_table if it doesn't exists.

    try:
        # Try reading fact_student_performance table.
        fact_student_performance = read_jdbc_table(spark,jdbc_gold_url,fact_table_name,driver)
        
    except Exception as e:
        # If it doesn't exist make it.
        logging.info(f"Exception while reading fact_student_performance table: {e}")
        logging.info("Creating table fact_student_performance.")
        # Define fact_student_performance table schema.
        fact_student_performance_schema = StructType([
            StructField("Student_ID", StringType(), True),
            StructField("Course_ID", StringType(), True),
            StructField("Enrollment_Date", DateType(), True), # it was DateType()
            StructField("Completion_Date", DateType(), True), # it was DateType()
            StructField("Status", StringType(), False),
            StructField("Final_Grade", StringType(), False),
            StructField("Attendance_Rate", DoubleType(), False),
            StructField("Time_Spent_on_Course_hrs", DoubleType(), False),
            StructField("Assignments_Completed", IntegerType(), False),
            StructField("Quizzes_Completed", IntegerType(), False),
            StructField("Forum_Posts", IntegerType(), False),
            StructField("Messages_Sent", IntegerType(), False),
            StructField("Quiz_Average_Score", DoubleType(), False),
            StructField("Assignment_Scores", StringType(), True),
            StructField("Assignment_Average_Score", DoubleType(), False),
            StructField("Project_Score", DoubleType(), False),
            StructField("Extra_Credit", DoubleType(), False),
            StructField("Overall_Performance", DoubleType(), False),
            StructField("Feedback_Score", DoubleType(), False),
            StructField("Completion_Time_Days", IntegerType(), True),
            StructField("Performance_Score", DoubleType(), False),
            StructField("Course_Completion_Rate", StringType(), False),
            StructField("Processing_Date", DateType(), True) # it was DateType()
            ])
        # Create fact_student_performance spark dataframe.
        empty_fact_student_performance = spark.createDataFrame([],fact_student_performance_schema)
        # Create table
        write_jdbc_table(empty_fact_student_performance,jdbc_gold_url,fact_table_name,driver,"overwrite")
        
        # Read it know that it exists.
        fact_student_performance = read_jdbc_table(spark,jdbc_gold_url,fact_table_name,driver)
        
    # Print fact_student_performance
    logging.info("fact_student_performance")
    fact_student_performance.show()

    # ---------------------------Upsert Logic---------------------------

    # Upsert data into dim_student
    ds = dim_student.alias("ds")
    n = todays_silver_data_df.alias("n")
    # Declaire join condition ON ds.Student_ID = n.Student_ID
    join_condition = col("ds.Student_ID") == col("n.Student_ID")
    # Merge the two tables.
    merged_ds_df = ds.join(n, join_condition, "fullouter")
    # Create final dataframe
    final_ds_df = merged_ds_df.select(
        coalesce(col("n.Student_ID"),col("ds.Student_ID")).alias("Student_ID"),
        coalesce(col("n.Name"),col("ds.Name")).alias("Name"),
        coalesce(col("n.Age"),col("ds.Age")).alias("Age"),
        coalesce(col("n.Gender"),col("ds.Gender")).alias("Gender"),
        coalesce(col("n.Demographic_Group"),col("ds.Demographic_Group")).alias("Demographic_Group"),
        coalesce(col("n.Internet_Access"),col("ds.Internet_Access")).alias("Internet_Access"),
        coalesce(col("n.Learning_Disabilities"),col("ds.Learning_Disabilities")).alias("Learning_Disabilities"),
        coalesce(col("n.Language_Proficiency"),col("ds.Language_Proficiency")).alias("Language_Proficiency"),
        coalesce(col("n.Parent_Involvement"),col("ds.Parent_Involvement")).alias("Parent_Involvement")
    )
    # Delete all the keys that need to be updated.
    keys_ds_df = final_ds_df.select("Student_ID").distinct().collect()
    with sqlite3.connect(f"{DESTINATION}/gold.db") as conn:
        cursor = conn.cursor()
        for row in keys_ds_df:
            cursor.execute(
                "DELETE FROM dim_student WHERE Student_ID = ?",
                (row["Student_ID"],)
            )
        conn.commit()
    # Append the data into gold.db.
    write_jdbc_table(final_ds_df,jdbc_gold_url,student_table_name,driver)
    
    logging.info("dim_student")
    new_dim_student = read_jdbc_table(spark,jdbc_gold_url,student_table_name,driver)
    
    new_dim_student.show()

    # Upsert data into dim_course
    dc = dim_course.alias("dc")

    # Declaire join condition ON dc.Course_ID = n.Course_ID
    join_condition = col("dc.Course_ID") == col("n.Course_ID")
    # Merge the two tables.
    merged_dc_df = dc.join(n, join_condition, "fullouter")
    # Create final df.
    final_dc_df = merged_dc_df.select(
        coalesce(col("n.Course_ID"),col("dc.Course_ID")).alias("Course_ID"),
        coalesce(col("n.Course_Name"),col("dc.Course_Name")).alias("Course_Name"),
        coalesce(col("n.Grade_Level"),col("dc.Grade_Level")).alias("Grade_Level")
    )
    final_dc_df = final_dc_df.dropDuplicates(["Course_ID"])
    # Delete all the keys that need to be updated.
    keys_dc_df = final_dc_df.select("Course_ID").distinct().collect()
    with sqlite3.connect(f"{DESTINATION}/gold.db") as conn:
        cursor = conn.cursor()
        for row in keys_dc_df:
            cursor.execute(
                "DELETE FROM dim_course WHERE Course_ID = ?",
                (row["Course_ID"],)
            )
        conn.commit()
    # Append the data into gold.db.
    write_jdbc_table(final_dc_df,jdbc_gold_url,course_table_name,driver)
    
    logging.info("dim_course")
    new_dim_course = read_jdbc_table(spark,jdbc_gold_url,course_table_name,driver)
    new_dim_course.show()

    # Upser data into fact_student_performance
    fsp = fact_student_performance.alias("fsp")

    # Declaire join condition ON dc.Course_ID = n.Course_ID
    join_condition = (col("fsp.Student_ID") == col("n.Student_ID")) & (col("fsp.Course_ID") == col("n.Course_ID"))
    # Merge the two tables.
    merged_fsp_df = fsp.join(n, join_condition, "fullouter")
    # Create final df.
    final_fsp_df = merged_fsp_df.select(
        coalesce(col("n.Student_ID"),col("fsp.Student_ID")).alias("Student_ID"),
        coalesce(col("n.Course_ID"),col("fsp.Course_ID")).alias("Course_ID"),
        coalesce(col("n.Enrollment_Date"),col("fsp.Enrollment_Date")).alias("Enrollment_Date"),
        coalesce(col("n.Completion_Date"),col("fsp.Completion_Date")).alias("Completion_Date"),
        coalesce(col("n.Status"),col("fsp.Status")).alias("Status"),
        coalesce(col("n.Final_Grade"),col("fsp.Final_Grade")).alias("Final_Grade"),
        coalesce(col("n.Attendance_Rate"),col("fsp.Attendance_Rate")).alias("Attendance_Rate"),
        coalesce(col("n.Time_Spent_on_Course_hrs"),col("fsp.Time_Spent_on_Course_hrs")).alias("Time_Spent_on_Course_hrs"),
        coalesce(col("n.Assignments_Completed"),col("fsp.Assignments_Completed")).alias("Assignments_Completed"),
        coalesce(col("n.Quizzes_Completed"),col("fsp.Quizzes_Completed")).alias("Quizzes_Completed"),
        coalesce(col("n.Forum_Posts"),col("fsp.Forum_Posts")).alias("Forum_Posts"),
        coalesce(col("n.Messages_Sent"),col("fsp.Messages_Sent")).alias("Messages_Sent"),
        coalesce(col("n.Quiz_Average_Score"),col("fsp.Quiz_Average_Score")).alias("Quiz_Average_Score"),
        coalesce(col("n.Assignment_Scores"),col("fsp.Assignment_Scores")).alias("Assignment_Scores"),
        coalesce(col("n.Assignment_Average_Score"),col("fsp.Assignment_Average_Score")).alias("Assignment_Average_Score"),
        coalesce(col("n.Project_Score"),col("fsp.Project_Score")).alias("Project_Score"),
        coalesce(col("n.Extra_Credit"),col("fsp.Extra_Credit")).alias("Extra_Credit"),
        coalesce(col("n.Overall_Performance"),col("fsp.Overall_Performance")).alias("Overall_Performance"),
        coalesce(col("n.Feedback_Score"),col("fsp.Feedback_Score")).alias("Feedback_Score"),
        coalesce(col("n.Completion_Time_Days"),col("fsp.Completion_Time_Days")).alias("Completion_Time_Days"),
        coalesce(col("n.Performance_Score"),col("fsp.Performance_Score")).alias("Performance_Score"),
        coalesce(col("n.Course_Completion_Rate"),col("fsp.Course_Completion_Rate")).alias("Course_Completion_Rate"),
        coalesce(col("fsp.Processing_Date"),col("n.Processing_Date")).alias("Processing_Date"),
    )
    # Delete all the keys that need to be updated.
    keys_fsp_df = final_fsp_df.select("Student_ID", "Course_ID").distinct().collect()
    with sqlite3.connect(f"{DESTINATION}/gold.db") as conn:
        cursor = conn.cursor()
        for row in keys_fsp_df:
            cursor.execute(
                "DELETE FROM fact_student_performance WHERE Student_ID = ? AND Course_ID = ?",
                (row["Student_ID"],row["Course_ID"])
            )
        conn.commit()
    # Append the data into gold.db.
    write_jdbc_table(final_fsp_df,jdbc_gold_url,fact_table_name,driver)
    
    logging.info("fact_student_performance")
    
    with sqlite3.connect(f"{DESTINATION}/gold.db") as conn:
        new_fact_df = pd.read_sql_query("SELECT * FROM fact_student_performance;",conn)
    print(new_fact_df.head(5))

if __name__ == '__main__':
    try:
        # Read command-line arguments
        SOURCE = sys.argv[1]
        DESTINATION = sys.argv[2]

        logging.info(f"Script started. Source: {SOURCE}, Destination: {DESTINATION}")

        # Create Spark session
        logging.info("Starting Spark Session: GoldDataTransformations")

        spark = SparkSession.builder.appName("GoldDataTransformation") \
            .config("spark.jars", "/usr/local/spark/jars/sqlite-jdbc-3.45.1.0.jar") \
            .getOrCreate()
        # Execute gold data transformaitions
        gold_data_transformations(spark,SOURCE,DESTINATION)

        logging.info("Data transformed successfully")

    except Exception as e:
        logging.exception(f"An exception occured in main: {e}")    