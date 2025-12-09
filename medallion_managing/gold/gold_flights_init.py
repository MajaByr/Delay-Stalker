# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# COMMAND ----------

flights_schema = StructType([
    StructField("flight_number", IntegerType(), True),
    StructField("airline_iata_code", StringType(), True),
    StructField("tail_number", StringType(), True), 
    StructField("taxis_id", IntegerType(),True),
    StructField("routes_id", IntegerType(),True),
    StructField("dates_id", IntegerType(),True),
    StructField("times_id", IntegerType(),True),
    StructField("cancellations_id", IntegerType(),True)
])

df_flights = spark.createDataFrame([],schema=flights_schema)

df_flights.write.mode("overwrite").saveAsTable("flights_gold.flights")

# COMMAND ----------

# Origin airport delays

origins_delays_schema = StructType([
    StructField("origin_airport_city", StringType(), True),
    StructField("origin_airport_state", StringType(), True),
    StructField("origin_airport_country", StringType(), True),
    StructField("air_system_delay", LongType(), True),
    StructField("security_delay", LongType(), True),
    StructField("airline_delay", LongType(), True),
    StructField("late_aircraft_delay", LongType(), True),
    StructField("weather_delay", LongType(), True)
])

df_origins_delays= spark.createDataFrame([],schema=origins_delays_schema)

df_origins_delays.write.mode("append").saveAsTable("flights_gold.origins_delays")


# COMMAND ----------

# Destination airport cancellations

destinations_cancellations_schema = StructType([
    StructField("destination_airport_city", StringType(), True),
    StructField("destination_airport_state", StringType(), True),
    StructField("destination_airport_country", StringType(), True),
    StructField("cancellation_reason", StringType(), True),
    StructField("count", LongType(), True)
])

df_destinations_cancellations= spark.createDataFrame([],schema=destinations_cancellations_schema)

df_destinations_cancellations.write.mode("append").saveAsTable("flights_gold.destinations_cancellations")


# COMMAND ----------

# Daily distances

daily_distances_schema = StructType([
    StructField("day", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("year", IntegerType(), True),
    StructField("overall_distance", DoubleType(), True)
])

df_daily_distances= spark.createDataFrame([],schema=daily_distances_schema)

df_daily_distances.write.mode("append").saveAsTable("flights_gold.daily_distances")
