# Databricks notebook source
# MAGIC %md
# MAGIC Extract Data from S3

# COMMAND ----------

from pyspark.sql.functions import col, to_date

def bronze_autoload_task():
    bronze_autoload = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", "s3://acledbucket/schema2/")
      .option("cloudFiles.maxFilesPerTrigger", "1")
      .load("s3://acledbucket/directory/"))

