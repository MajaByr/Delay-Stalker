# Databricks notebook source
tables = spark.catalog.listTables("flights_silver")

for table in tables:
    spark.sql(f"DROP TABLE IF EXISTS {"flights_silver"}.{table.name}")