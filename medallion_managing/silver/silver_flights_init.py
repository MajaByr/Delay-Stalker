# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# COMMAND ----------

# SCD Type 2 - airlines can change their name for marketing purposes

airlines_schema = StructType([
    StructField("airline_iata_code", StringType(), True),
    StructField("airline", StringType(), True),
    StructField("effective_from", TimestampType(), True),
    StructField("effective_to", TimestampType(), True),
    StructField("is_current", BooleanType(), True)
])

df_airlines= spark.createDataFrame([],schema=airlines_schema)

df_airlines.write.mode("append").saveAsTable("flights_silver.airlines")

# COMMAND ----------

# SCD Type 1 - cities won't change their location (and we can assume that names too)

cities_schema = StructType([
    StructField("cities_id", IntegerType(), True),
    StructField("city_name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True)
])

df_cities = spark.createDataFrame([],schema=cities_schema)

df_cities.write.mode("overwrite").saveAsTable("flights_silver.cities")

# COMMAND ----------

# SCD Type 1

airports_schema = StructType([
    StructField("airport_iata_code", StringType(), True),
    StructField("airport", StringType(), True),
    StructField("cities_id", IntegerType(), True)
])

df_airports = spark.createDataFrame([],schema=airports_schema)

df_airports.write.mode("overwrite").saveAsTable("flights_silver.airports")

# COMMAND ----------

# SCD Type 1 - routes won't change

routes_schema = StructType([
    StructField("routes_id", IntegerType(), False),
    StructField("distance", DoubleType(), True),
    StructField("origin_airport_iata_code", StringType(), True),
    StructField("destination_airport_iata_code", StringType(), True)
])

df_routes = spark.createDataFrame([],schema=routes_schema)

df_routes.write.mode("overwrite").saveAsTable("flights_silver.routes")

# COMMAND ----------

dates_schema = StructType([
    StructField("dates_id", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("day_of_week", StringType(), True)
])

df_dates = spark.createDataFrame([],schema=dates_schema)

df_dates.write.mode("overwrite").saveAsTable("flights_silver.dates")

# COMMAND ----------

# SCD Type 2 - keep all history - time of changes and historical values

cancellations_schema = StructType([
    StructField("cancellations_id", IntegerType(), False),
    StructField("diverted", BooleanType(), True),
    StructField("cancelled", BooleanType(), True),
    StructField("cancellation_reason", StringType(), True),
    StructField("effective_from", TimestampType(), True),
    StructField("effective_to", TimestampType(), True),
    StructField("is_current", BooleanType(), True)
])

df_cancellations = spark.createDataFrame([],schema=cancellations_schema)

df_cancellations.write.mode("overwrite").saveAsTable("flights_silver.cancellations")

# COMMAND ----------

# SCD Type 1

wheels_schema = StructType([
    StructField("wheels_id", IntegerType(), False),
    StructField("wheels_on", IntegerType(), True),
    StructField("wheels_off", IntegerType(), True)
])

df_wheels = spark.createDataFrame([],schema=wheels_schema)

df_wheels.write.mode("overwrite").saveAsTable("flights_silver.wheels")

# COMMAND ----------

# SCD Type 1

taxis_schema = StructType([
    StructField("taxis_id", IntegerType(), False),
    StructField("taxi_out", IntegerType(), True),
    StructField("taxi_in", IntegerType(), True)
])

df_taxis = spark.createDataFrame([],schema=taxis_schema)

df_taxis.write.mode("overwrite").saveAsTable("flights_silver.taxis")

# COMMAND ----------

# SCD Type 3 - keep previous delay time

arrivals_schema = StructType([
    StructField("arrivals_id", IntegerType(), False),
    StructField("scheduled_arrival", IntegerType(), True),
    StructField("arrival_time", IntegerType(), True), 
    StructField("arrival_delay", IntegerType(), True),
    StructField("previous_arrival_delay", IntegerType(), True)
])

df_arrivals = spark.createDataFrame([],schema=arrivals_schema)

df_arrivals.write.mode("overwrite").saveAsTable("flights_silver.arrivals")

# COMMAND ----------

# SCD Type 3 - keep previous delay time

departures_schema = StructType([
    StructField("departures_id", IntegerType(), False),
    StructField("scheduled_departure", IntegerType(), True),
    StructField("departure_time", IntegerType(), True),
    StructField("departure_delay", IntegerType(), True),
    StructField("previous_departure_delay", IntegerType(), True)
])

df_departures = spark.createDataFrame([],schema=departures_schema)

df_departures.write.mode("overwrite").saveAsTable("flights_silver.departures")

# COMMAND ----------

# SCD Type 1 - do not track all delay causes

delays_schema = StructType([
    StructField("delays_id", IntegerType(), False),
    StructField("air_system_delay", IntegerType(),True),
    StructField("security_delay", IntegerType(),True),
    StructField("airline_delay", IntegerType(),True),
    StructField("late_aircraft_delay", IntegerType(),True),
    StructField("weather_delay", IntegerType(),True)
])

df_delays = spark.createDataFrame([],schema=delays_schema)

df_delays.write.mode("overwrite").saveAsTable("flights_silver.delays")

# COMMAND ----------

# SCD Type 1

overall_schedules_schema = StructType([
    StructField("schedules_id", IntegerType(), False),
    StructField("scheduled_time", IntegerType(), True),
    StructField("elapsed_time", IntegerType(), True), 
    StructField("air_time", IntegerType(),True)
])

df_overall_schedules = spark.createDataFrame([],schema=overall_schedules_schema)

df_overall_schedules.write.mode("overwrite").saveAsTable("flights_silver.overall_schedules")

# COMMAND ----------

# SCD Type 1

times_schema = StructType([
    StructField("times_id", IntegerType(), False),
    StructField("overall_schedules_id", IntegerType(), True),
    StructField("wheels_id", IntegerType(), True), 
    StructField("taxis_id", IntegerType(),True),
    StructField("departures_id", IntegerType(),True),
    StructField("arrivals_id", IntegerType(),True),
    StructField("delays_id", IntegerType(),True)
])

df_times = spark.createDataFrame([],schema=times_schema)

df_times.write.mode("overwrite").saveAsTable("flights_silver.times")

# COMMAND ----------

