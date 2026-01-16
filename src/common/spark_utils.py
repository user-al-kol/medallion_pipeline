from pyspark.sql import SparkSession

def get_spark(app_name, extra_jars=None):
    builder = SparkSession.builder.appName(app_name)
    if extra_jars:
        builder = builder.config("spark.jars", extra_jars)
    return builder.getOrCreate()

def read_jdbc_table(spark, jdbc_url, table, driver):
    return spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table) \
        .option("driver", driver) \
        .load()

def write_jdbc_table(df,jdbc_url,table,driver,mode="append"):
    df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table) \
        .option("driver", driver) \
        .mode(mode) \
        .save()

def debug_show(df, cols, n=20, label=None):
    if label:
        print(f"==== {label} ====")
    df.select(cols).show(n)