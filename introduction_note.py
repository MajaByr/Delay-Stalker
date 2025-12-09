# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 0. RAW DATASET
# MAGIC
# MAGIC ## airlines.csv
# MAGIC * IATA_COSE
# MAGIC * AIRLINE
# MAGIC
# MAGIC ## airports.csv
# MAGIC * IATA_COSE
# MAGIC * AIRPORT
# MAGIC
# MAGIC ## flights.csv
# MAGIC * YEAR
# MAGIC * MONTH
# MAGIC * DAY
# MAGIC * DAY_OF_WEEK
# MAGIC * AIRLINE
# MAGIC * FLIGHT_NUMBER
# MAGIC * TAIL_NUMBER
# MAGIC * ORIGIN_AIRPORT
# MAGIC * DESTINATION_AIRPORT
# MAGIC * SCHEDULED_DEPARTURE
# MAGIC * DEPARTURE_TIME
# MAGIC * DEPARTURE_DELAY
# MAGIC * TAXI_OUT
# MAGIC * WHEELS_OFF
# MAGIC * SCHEDULED_TIME
# MAGIC * ELAPSED_TIME
# MAGIC * AIR_TIME
# MAGIC * DISTANCE
# MAGIC * WHEELS_ON
# MAGIC * TAXI_IN
# MAGIC * SCHEDULED_ARRIVAL
# MAGIC * ARRIVAL_TIME
# MAGIC * ARRIVAL_DELAY
# MAGIC * DIVERTED
# MAGIC * CANCELLED
# MAGIC * CANCELLATION_REASON
# MAGIC * AIR_SYSTEM_DELAY
# MAGIC * SECURITY_DELAY
# MAGIC * AIRLINE_DELAY
# MAGIC * LATE_AIRCRAFT_DELAY
# MAGIC * WEATHER_DELAY
# MAGIC
# MAGIC ## Inspect dataset

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv("/Volumes/workspace/flightsschema/flightsvolume/flights.csv", header=True, inferSchema=True)

# COMMAND ----------

print("Wheels off unique values:")
unique_vals = df.select("wheels_off").distinct()
unique_vals.show()

print("Wheels on unique values:")
unique_vals = df.select("wheels_on").distinct()
unique_vals.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 1.1. ERD Diagram 
# MAGIC Diagram is showed in README.
# MAGIC
# MAGIC # 1.3. Snowflake Architecture
# MAGIC In this project snowflake architecture is used. This approach:
# MAGIC * Reduces data redundancy;
# MAGIC * Although there is more tables, it potentially requires less storage due to normalized dimensions;
# MAGIC * Ensures consistent data;
# MAGIC * Queries will be more complicated, but data will be wrapped in gold layer anyway;
# MAGIC
# MAGIC # 1.2. Dimensions and Facts Identification, SCD Stratedy
# MAGIC
# MAGIC These topics are described in `bronze`, `silver` and `gold` implementation notebooks.

# COMMAND ----------

