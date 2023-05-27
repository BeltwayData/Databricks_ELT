# Databricks notebook source
from pyspark.sql.functions import col, to_date

(spark.readStream
     .table("bronze_ACLED_SQL_ETL_9")
     .drop("timestamp")
     .withColumnRenamed("tags", "crowd_size")
     .writeStream
     .option("checkpointLocation", "s3://acledbucket/checkpointsilver2/")
     .table("acled_silver_final")
)

