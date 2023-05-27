# Databricks notebook source
from pyspark.sql.functions import col, to_date

def bronze_autoload_task():

    (bronze_autoload.writeStream
      .format("delta")
      .option("checkpointLocation", "s3://#######")
      .option("mergeSchema", "true")
      .toTable("#######")
      .start())
