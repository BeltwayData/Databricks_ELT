# Databricks notebook source
# MAGIC %md SQL & Python Create Table for Extract 
# MAGIC SQL code is commented out and is the same as the python code

# COMMAND ----------

# CREATE TABLE IF NOT EXISTS bronze_ACLED_SQL_ETL_9 (
#   event_id_cnty STRING NOT NULL,
#   event_date STRING,
#   year STRING,
#   time_precision STRING,
#   disorder_type STRING,
#   event_type STRING,
#   sub_event_type STRING,
#   actor1 STRING,
#   assoc_actor_1 STRING,
#   inter1 STRING,
#   actor2 STRING,
#   assoc_actor_2 STRING,
#   inter2 STRING,
#   interaction STRING,
#   civilian_targeting STRING,
#   iso STRING,
#   region STRING,
#   country STRING,
#   admin1 STRING,
#   admin2 STRING,
#   admin3 STRING,
#   location STRING,
#   latitude STRING,
#   longitude STRING,
#   geo_precision STRING,
#   source STRING,
#   source_scale STRING,
#   notes STRING,
#   fatalities STRING,
#   tags STRING,
#   timestamp STRING
# ) using delta tblproperties (
#   delta.autooptimize.optimizewrite = TRUE,
#   delta.autooptimize.autocompact   = TRUE );

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

spark = SparkSession.builder.getOrCreate()

table_name = "bronze_ACLED_SQL_ETL_9"
table_path = "dbfs:/user/hive/warehouse/bronze_acled_sql_etl_9"

# Define the table schema
schema = StructType([
    StructField("event_id_cnty", StringType(), nullable=False),
    StructField("event_date", StringType()),
    StructField("year", StringType()),
    StructField("time_precision", StringType()),
    StructField("disorder_type", StringType()),
    StructField("event_type", StringType()),
    StructField("sub_event_type", StringType()),
    StructField("actor1", StringType()),
    StructField("assoc_actor_1", StringType()),
    StructField("inter1", StringType()),
    StructField("actor2", StringType()),
    StructField("assoc_actor_2", StringType()),
    StructField("inter2", StringType()),
    StructField("interaction", StringType()),
    StructField("civilian_targeting", StringType()),
    StructField("iso", StringType()),
    StructField("region", StringType()),
    StructField("country", StringType()),
    StructField("admin1", StringType()),
    StructField("admin2", StringType()),
    StructField("admin3", StringType()),
    StructField("location", StringType()),
    StructField("latitude", StringType()),
    StructField("longitude", StringType()),
    StructField("geo_precision", StringType()),
    StructField("source", StringType()),
    StructField("source_scale", StringType()),
    StructField("notes", StringType()),
    StructField("fatalities", StringType()),
    StructField("tags", StringType()),
    StructField("timestamp", StringType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC Display the data schema

# COMMAND ----------

# from pyspark.sql import SparkSession

# spark = SparkSession.builder.getOrCreate()

# table_name = "bronze_ACLED_SQL_ETL_9"
# table_path = "dbfs:/user/hive/warehouse/bronze_acled_sql_etl_9"

# # Read the Delta table
# df = spark.read.format("delta").load(table_path)

# # Show the contents of the table
# #df.show(truncate=False, vertical=True) 
# display(df)
