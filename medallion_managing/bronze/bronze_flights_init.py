# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# COMMAND ----------

airlines_schema = StructType([
    StructField("airline_iata_code", StringType(), True),
    StructField("airline", StringType(), True)
])

df_airlines = spark.createDataFrame([],schema=airlines_schema)

df_airlines.write.mode("overwrite").saveAsTable("flights_bronze.airlines")

# COMMAND ----------

airports_schema = StructType([
    StructField("airport_iata_code", StringType(), True),
    StructField("airport", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
])

df_airports = spark.createDataFrame([],schema=airports_schema)

df_airports.write.mode("overwrite").saveAsTable("flights_bronze.airports")

# COMMAND ----------

flights_schema = StructType([
    StructField("year", StringType(), True),
    StructField("month", StringType(), True),
    StructField("day", StringType(), True),
    StructField("day_of_week", StringType(), True),
    StructField("airline", StringType(), True),
    StructField("flight_number", StringType(), True),
    StructField("tail_number", StringType(), True),
    StructField("origin_airport", StringType(), True),
    StructField("destination_airport", StringType(), True),
    StructField("scheduled_departure", StringType(), True),
    StructField("departure_time", StringType(), True),
    StructField("departure_delay", StringType(), True),
    StructField("taxi_out", StringType(), True),
    StructField("wheels_off", StringType(), True),
    StructField("scheduled_time", StringType(), True),
    StructField("elapsed_time", StringType(), True),
    StructField("air_time", StringType(), True),
    StructField("distance", StringType(), True),
    StructField("wheels_on", StringType(), True),
    StructField("taxi_in", StringType(), True),
    StructField("scheduled_arrival", StringType(), True),
    StructField("arrival_time", StringType(), True),
    StructField("arrival_delay", StringType(), True),
    StructField("diverted", StringType(), True),
    StructField("cancelled", StringType(), True),
    StructField("cancellation_reason", StringType(), True),
    StructField("air_system_delay", StringType(), True),
    StructField("security_delay", StringType(), True),
    StructField("airline_delay", StringType(), True),
    StructField("late_aircraft_delay", StringType(), True),
    StructField("weather_delay", StringType(), True),
])

df_flights = spark.createDataFrame([],schema=flights_schema)

df_flights.write.mode("overwrite").saveAsTable("flights_bronze.flights")

# COMMAND ----------

