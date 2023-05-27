# Databricks notebook source
from pyspark.sql.functions import col, to_date

def bronze_autoload_task():

    (bronze_autoload.writeStream
      .format("delta")
      .option("checkpointLocation", "s3://acledbucket/checkpointacled/")
      .option("mergeSchema", "true")
      .toTable("bronze_ACLED_SQL_ETL_9")
      .start())
