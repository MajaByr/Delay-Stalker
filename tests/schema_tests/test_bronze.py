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
    StructField("airline", StringType(), True)
])

df_airlines = spark.table("flights_bronze.airlines")

test_schema(df_airlines, airlines_schema)

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

df_airports = spark.table("flights_bronze.airports")

test_schema(df_airports, airports_schema)

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

df_flights = spark.table("flights_bronze.flights")


test_schema(df_flights, flights_schema)