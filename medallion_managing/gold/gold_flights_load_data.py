# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import col

# COMMAND ----------

df_flights_csv = spark.table("flights_bronze.flights")

df_overall_schedules = spark.table("flights_silver.overall_schedules")
df_wheels = spark.table("flights_silver.wheels")
df_taxis = spark.table("flights_silver.taxis")
df_departures = spark.table("flights_silver.departures")
df_arrivals = spark.table("flights_silver.arrivals")
df_times = spark.table("flights_silver.times")
df_delays = spark.table("flights_silver.delays")
df_dates = spark.table("flights_silver.dates")
df_routes = spark.table("flights_silver.routes")
df_cancellations = spark.table("flights_silver.cancellations")

# COMMAND ----------

df_flights = df_flights_csv.withColumnRenamed("origin_airport", "origin_airport_iata_code").withColumnRenamed("destination_airport", "destination_airport_iata_code").withColumn("air_system_delay", col("air_system_delay").cast("int")).withColumn("security_delay", col("security_delay").cast("int")).withColumn("airline_delay", col("airline_delay").cast("int")).withColumn("late_aircraft_delay", col("late_aircraft_delay").cast("int")).withColumn("weather_delay", col("weather_delay").cast("int")) \
    .join(
    df_overall_schedules.select("scheduled_time", "elapsed_time", "air_time", "schedules_id"),
    on=["scheduled_time", "elapsed_time", "air_time"],
    how="left"
    ).withColumnRenamed("schedules_id", "overall_schedules_id") \
    .join(
    df_wheels.select("wheels_id", "wheels_on", "wheels_off"),
    on=["wheels_on", "wheels_off"],
    how="left"
    ) \
    .join(
    df_taxis.select("taxis_id", "taxi_out", "taxi_in"),
    on=["taxi_out", "taxi_in"],
    how="left"
    ) \
    .join(
    df_departures.select("departures_id", "scheduled_departure", "departure_time", "departure_delay"),
    on=["scheduled_departure", "departure_time", "departure_delay"],
    how="left"
    ) \
    .join(
    df_arrivals.select("arrivals_id", "scheduled_arrival", "arrival_time", "arrival_delay"),
    on=["scheduled_arrival", "arrival_time", "arrival_delay"],
    how="left"
    ) \
    .join(
    df_delays.select("delays_id", "air_system_delay", "security_delay", "airline_delay", "late_aircraft_delay", "weather_delay"),
    on=["air_system_delay", "security_delay", "airline_delay", "late_aircraft_delay", "weather_delay"],
    how="left"
    ) \
    .join(
    df_times.select("overall_schedules_id", "wheels_id", "taxis_id", "departures_id", "arrivals_id", "delays_id", "times_id"),
    on=["overall_schedules_id", "wheels_id", "taxis_id", "departures_id", "arrivals_id", "delays_id"],
    how="left"
    ) \
    .join(
    df_routes.select("routes_id", "distance", "origin_airport_iata_code", "destination_airport_iata_code"),
    on=["distance", "origin_airport_iata_code", "destination_airport_iata_code"],
    how="left"
    ) \
    .join(
    df_dates.select("dates_id", "year", "month", "day", "day_of_week"),
    on=["year", "month", "day", "day_of_week"],
    how="left"
    ) \
    .join(
    df_cancellations.select("cancellations_id", "diverted", "cancelled", "cancellation_reason"),
    on=["diverted", "cancelled", "cancellation_reason"],
    how="left"
    ) \
    .withColumnRenamed("airline", "airline_iata_code") \
    .select("flight_number", "airline_iata_code", "tail_number", "routes_id", "dates_id", "times_id", "cancellations_id").withColumn("flight_number", col("flight_number").cast("int"))

df_flights.write.mode("append").saveAsTable("flights_gold.flights")

# COMMAND ----------

df_airports = spark.table("flights_silver.airports")
df_cities = spark.table("flights_silver.cities")

# COMMAND ----------

df_origins_delays = df_flights \
    .join(
    df_routes.select("routes_id", "origin_airport_iata_code"),
    on=["routes_id"],
    how="left"
    ) \
    .withColumnRenamed("origin_airport_iata_code", "airport_iata_code") \
    .join(
    df_airports.select("airport_iata_code", "cities_id"),
    on=["airport_iata_code"],
    how="left"
    ) \
    .join(
    df_cities.select("city_name", "state", "country", "cities_id"),
    on=["cities_id"],
    how="left"
    ) \
    .join(
    df_times.select("times_id", "delays_id"),
    on=["times_id"],
    how="left"
    ) \
    .join(
    df_delays.select("delays_id", "air_system_delay", "security_delay", "airline_delay", "late_aircraft_delay", "weather_delay"),
    on=["delays_id"],
    how="left"
    ) \
    .withColumnRenamed("city_name", "origin_airport_city") \
    .withColumnRenamed("state", "origin_airport_state") \
    .withColumnRenamed("country", "origin_airport_country") \
    .select(
        "origin_airport_city", 
        "origin_airport_state", "origin_airport_country",
        "air_system_delay",
        "security_delay",
        "airline_delay",
        "late_aircraft_delay",
        "weather_delay"
        ) \
    .groupBy(
        "origin_airport_city", 
        "origin_airport_state", 
        "origin_airport_country"
    ) \
    .agg(
        spark_sum("air_system_delay").alias("air_system_delay"),
        spark_sum("security_delay").alias("security_delay"),
        spark_sum("airline_delay").alias("airline_delay"),
        spark_sum("late_aircraft_delay").alias("late_aircraft_delay"),
        spark_sum("weather_delay").alias("weather_delay")
    )

df_origins_delays.write.mode("append").saveAsTable("flights_gold.origins_delays")

# COMMAND ----------

df_cancellations = spark.table("flights_silver.cancellations")

# COMMAND ----------

df_destinations_cancellations = df_flights \
    .join(
    df_routes.select("routes_id", "destination_airport_iata_code"),
    on=["routes_id"],
    how="inner"
    ) \
    .withColumnRenamed("destination_airport_iata_code", "airport_iata_code") \
    .join(
    df_airports.select("airport_iata_code", "cities_id"),
    on=["airport_iata_code"],
    how="inner"
    ) \
    .join(
    df_cities.select("city_name", "state", "country", "cities_id"),
    on=["cities_id"],
    how="inner"
    ) \
    .join(
    df_cancellations.select("cancellations_id", "cancellation_reason"),
    on=["cancellations_id"],
    how="inner"
    ) \
    .withColumnRenamed("city_name", "destination_airport_city") \
    .withColumnRenamed("state", "destination_airport_state") \
    .withColumnRenamed("country", "destination_airport_country") \
    .select(
        "destination_airport_city", 
        "destination_airport_state", "destination_airport_country",
        "cancellation_reason"
        ) \
    .groupBy(
        "destination_airport_city", 
        "destination_airport_state", 
        "destination_airport_country",
        "cancellation_reason"
    ).count()

df_destinations_cancellations.write.mode("append").saveAsTable("flights_gold.destinations_cancellations")

# COMMAND ----------

df_dates = spark.table("flights_silver.dates")

# COMMAND ----------

df_daily_distances = df_flights \
    .join(
    df_routes.select("routes_id", "distance"),
    on=["routes_id"],
    how="inner"
    ) \
    .join(
    df_dates.select("dates_id", "month", "day", "year"),
    on=["dates_id"],
    how="inner"
    ) \
    .select(
        "day", "month", "year", "distance"
        ) \
    .groupBy(
        "day", "month", "year"
    ) \
    .agg(
        spark_sum("distance").alias("overall_distance")
    )

df_daily_distances.write.mode("append").saveAsTable("flights_gold.daily_distances")

# COMMAND ----------

