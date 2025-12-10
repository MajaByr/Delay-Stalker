# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.builder.getOrCreate()

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
        raise ValueError(
            f"Names of columns differ. Expected '{expected_cols}', recieved: '{df_cols}'",
        )
    else:
        print("Names of the columns are correct")

    # Check column types
    for f_exp, f_actual in zip(expected_schema.fields, df_schema.fields):
        if f_exp.dataType != f_actual.dataType:
            raise ValueError(
                f"Data type {f_exp.name} is not as expected. Expected: '{f_exp.dataType}', recieved: '{f_actual.dataType}'",
            )

    print(f"Column have correct types")


# COMMAND ----------

flights_schema = StructType(
    [
        StructField("flight_number", IntegerType(), True),
        StructField("airline_iata_code", StringType(), True),
        StructField("tail_number", StringType(), True),
        StructField("taxis_id", IntegerType(), True),
        StructField("routes_id", IntegerType(), True),
        StructField("dates_id", IntegerType(), True),
        StructField("times_id", IntegerType(), True),
        StructField("cancellations_id", IntegerType(), True),
    ],
)

df_flights = spark.table("flights_gold.flights")

test_schema(df_flights, flights_schema)

# COMMAND ----------

origins_delays_schema = StructType(
    [
        StructField("origin_airport_city", StringType(), True),
        StructField("origin_airport_state", StringType(), True),
        StructField("origin_airport_country", StringType(), True),
        StructField("air_system_delay", LongType(), True),
        StructField("security_delay", LongType(), True),
        StructField("airline_delay", LongType(), True),
        StructField("late_aircraft_delay", LongType(), True),
        StructField("weather_delay", LongType(), True),
    ],
)

df_origins_delays = spark.table("flights_gold.origins_delays")

test_schema(df_origins_delays, origins_delays_schema)

# COMMAND ----------

destinations_cancellations_schema = StructType(
    [
        StructField("destination_airport_city", StringType(), True),
        StructField("destination_airport_state", StringType(), True),
        StructField("destination_airport_country", StringType(), True),
        StructField("cancellation_reason", StringType(), True),
        StructField("count", LongType(), True),
    ],
)

df_destinations_cancellations = spark.table("flights_gold.destinations_cancellations")

test_schema(df_destinations_cancellations, destinations_cancellations_schema)

# COMMAND ----------

daily_distances_schema = StructType(
    [
        StructField("day", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("overall_distance", DoubleType(), True),
    ],
)

df_daily_distances = spark.table("flights_gold.daily_distances")

test_schema(df_daily_distances, daily_distances_schema)
