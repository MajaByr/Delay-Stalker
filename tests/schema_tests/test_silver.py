# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Table Schema Test

# COMMAND ----------

def test_schema(df, expected_schema):
    df_schema = df.schema
    
    # Check column names
    expected_cols = [f.name for f in expected_schema.fields]
    df_cols = [f.name for f in df_schema.fields]
    if expected_cols != df_cols:
        raise ValueError(f"Names of columns differ. Expected '{expected_cols}', recieved: '{df_cols}'")
    else:
        print("Names of the columns are correct")
    
    # Check column types
    for f_exp, f_actual in zip(expected_schema.fields, df_schema.fields):
        if f_exp.dataType != f_actual.dataType:
            raise ValueError(f"Data type {f_exp.name} is not as expected. Expected: '{f_exp.dataType}', recieved: '{f_actual.dataType}'")
    
    print(f"Column have correct types")

# COMMAND ----------

airlines_schema = StructType([
    StructField("airline_iata_code", StringType(), True),
    StructField("airline", StringType(), True),
    StructField("effective_from", TimestampType(), True),
    StructField("effective_to", TimestampType(), True),
    StructField("is_current", BooleanType(), True)
])

df_airlines = spark.table("flights_silver.airlines")

test_schema(df_airlines, airlines_schema)

# COMMAND ----------

cities_schema = StructType([
    StructField("cities_id", IntegerType(), True),
    StructField("city_name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True)
])

df_cities = spark.table("flights_silver.cities")

test_schema(df_cities, cities_schema)

# COMMAND ----------

airports_schema = StructType([
    StructField("airport_iata_code", StringType(), True),
    StructField("airport", StringType(), True),
    StructField("cities_id", IntegerType(), True)
])

df_airports = spark.table("flights_silver.airports")

test_schema(df_airports, airports_schema)

# COMMAND ----------

routes_schema = StructType([
    StructField("routes_id", IntegerType(), False),
    StructField("distance", DoubleType(), True),
    StructField("origin_airport_iata_code", StringType(), True),
    StructField("destination_airport_iata_code", StringType(), True)
])

df_routes = spark.table("flights_silver.routes")

test_schema(df_routes, routes_schema)

# COMMAND ----------

dates_schema = StructType([
    StructField("dates_id", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("day_of_week", StringType(), True)
])

df_dates = spark.table("flights_silver.dates")

test_schema(df_dates, dates_schema)

# COMMAND ----------

cancellations_schema = StructType([
    StructField("cancellations_id", IntegerType(), False),
    StructField("diverted", BooleanType(), True),
    StructField("cancelled", BooleanType(), True),
    StructField("cancellation_reason", StringType(), True),
    StructField("effective_from", TimestampType(), True),
    StructField("effective_to", TimestampType(), True),
    StructField("is_current", BooleanType(), True)
])

df_cancellations = spark.table("flights_silver.cancellations")

test_schema(df_cancellations, cancellations_schema)

# COMMAND ----------

wheels_schema = StructType([
    StructField("wheels_id", IntegerType(), False),
    StructField("wheels_on", IntegerType(), True),
    StructField("wheels_off", IntegerType(), True)
])

df_wheels = spark.table("flights_silver.wheels")

test_schema(df_wheels, wheels_schema)

# COMMAND ----------

taxis_schema = StructType([
    StructField("taxis_id", IntegerType(), False),
    StructField("taxi_out", IntegerType(), True),
    StructField("taxi_in", IntegerType(), True)
])

df_taxis = spark.table("flights_silver.taxis")

test_schema(df_taxis, taxis_schema)

# COMMAND ----------

arrivals_schema = StructType([
    StructField("arrivals_id", IntegerType(), False),
    StructField("scheduled_arrival", IntegerType(), True),
    StructField("arrival_time", IntegerType(), True), 
    StructField("arrival_delay", IntegerType(),True),
    StructField("previous_arrival_delay", IntegerType(), True)
])

df_arrivals = spark.table("flights_silver.arrivals")

test_schema(df_arrivals, arrivals_schema)

# COMMAND ----------

departures_schema = StructType([
    StructField("departures_id", IntegerType(), False),
    StructField("scheduled_departure", IntegerType(), True),
    StructField("departure_time", IntegerType(), True),
    StructField("departure_delay", IntegerType(), True),
    StructField("previous_departure_delay", IntegerType(), True)
])

df_departures = spark.table("flights_silver.departures")

test_schema(df_departures, departures_schema)

# COMMAND ----------

delays_schema = StructType([
    StructField("delays_id", IntegerType(), False),
    StructField("air_system_delay", IntegerType(),True),
    StructField("security_delay", IntegerType(),True),
    StructField("airline_delay", IntegerType(),True),
    StructField("late_aircraft_delay", IntegerType(),True),
    StructField("weather_delay", IntegerType(),True)
])

df_delays = spark.table("flights_silver.delays")

test_schema(df_delays, delays_schema)

# COMMAND ----------

overall_schedules_schema = StructType([
    StructField("schedules_id", IntegerType(), False),
    StructField("scheduled_time", IntegerType(), True),
    StructField("elapsed_time", IntegerType(), True), 
    StructField("air_time", IntegerType(),True)
])

df_overall_schedules = spark.table("flights_silver.overall_schedules")

test_schema(df_overall_schedules, overall_schedules_schema)

# COMMAND ----------

times_schema = StructType([
    StructField("times_id", IntegerType(), False),
    StructField("overall_schedules_id", IntegerType(), True),
    StructField("wheels_id", IntegerType(), True), 
    StructField("taxis_id", IntegerType(),True),
    StructField("departures_id", IntegerType(),True),
    StructField("arrivals_id", IntegerType(),True),
    StructField("delays_id", IntegerType(),True)
])

df_times = spark.table("flights_silver.times")

test_schema(df_times, times_schema)