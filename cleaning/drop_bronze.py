# Databricks notebook source
tables = spark.catalog.listTables("flights_bronze")

for table in tables:
    spark.sql(f"DROP TABLE IF EXISTS {"flights_bronze"}.{table.name}")