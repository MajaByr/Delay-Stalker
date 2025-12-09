# Databricks notebook source
tables = spark.catalog.listTables("flights_gold")

for table in tables:
    spark.sql(f"DROP TABLE IF EXISTS {"flights_gold"}.{table.name}")