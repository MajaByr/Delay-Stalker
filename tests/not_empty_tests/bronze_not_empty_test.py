# Databricks notebook source

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# MAGIC %md
# MAGIC
# MAGIC # Test if Tables are not Empty

# COMMAND ----------

database = "flights_bronze"
tables = spark.catalog.listTables(database)

for tbl in tables:
    full_name = f"{database}.{tbl.name}"
    df = spark.table(full_name)

    if df.limit(1).count() == 0:
        raise ValueError(f"Table '{full_name}' is empty")
