# Databricks notebook source
from pyspark.sql.functions import col, to_date

(spark.readStream
     .table("bronze_#####")
     .drop("timestamp")
     .withColumnRenamed("tags", "crowd_size")
     .writeStream
     .option("checkpointLocation", "s3:/##############")
     .table("#####silver_final")
)

