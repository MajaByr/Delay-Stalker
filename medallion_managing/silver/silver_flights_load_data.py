# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# COMMAND ----------

df_airlines=spark.table("flights_bronze.airlines")

df_airlines.write.mode("append").saveAsTable("flights_silver.airlines")

# COMMAND ----------

df_airports_csv= spark.table("flights_bronze.airports")

df_airports_csv = df_airports_csv.toDF(*[c.lower() for c in df_airports_csv.columns])

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# COMMAND ----------

df_cities = df_airports_csv.select(
    col("city").alias("city_name"),
    col("state"),
    col("country"),
    col("latitude"),
    col("longitude")
)

df_cities = df_cities.dropDuplicates(["country", "latitude", "longitude"])

w = Window.orderBy("city_name", "state", "country")
df_cities = df_cities.withColumn("cities_id", row_number().over(w))

df_cities.write.mode("append").saveAsTable("flights_silver.cities")

# COMMAND ----------

df_airports = df_airports_csv.withColumnRenamed("city", "city_name").join(
    df_cities.select("cities_id", "city_name", "latitude", "longitude"),
    on=["city_name", "latitude", "longitude"],
    how="left"
).select("cities_id", "airport_iata_code", "airport")

df_airports = df_airports.dropDuplicates(["airport_iata_code"])

df_airports.write.mode("append").saveAsTable("flights_silver.airports")

# COMMAND ----------

df_flights_csv = spark.table("flights_bronze.flights")

df_flights_csv = df_flights_csv.toDF(*[c.lower() for c in df_flights_csv.columns])

# COMMAND ----------

df_routes= df_flights_csv.select(
    col("origin_airport").alias("origin_airport_iata_code"),
    col("destination_airport").alias("destination_airport_iata_code"),
    col("distance").cast("double")
)

df_routes = df_routes.dropDuplicates(["origin_airport_iata_code", "destination_airport_iata_code", "distance"])

w = Window.orderBy("origin_airport_iata_code", "destination_airport_iata_code")
df_routes = df_routes.withColumn("routes_id", row_number().over(w))

df_routes.write.mode("append").saveAsTable("flights_silver.routes")


# COMMAND ----------

df_dates = df_flights_csv.select(
    col("year").cast("integer"),
    col("month").cast("integer"),
    col("day").cast("integer"),
    col("day_of_week"),
)

df_dates = df_dates.dropDuplicates(["year", "month", "day"])

w = Window.orderBy("year", "month", "day")
df_dates = df_dates.withColumn("dates_id", row_number().over(w))

df_dates.write.mode("append").saveAsTable("flights_silver.dates")

# COMMAND ----------

df_cancellations = df_flights_csv.select(
    col("diverted").cast("boolean"),
    col("cancelled").cast("boolean"),
    col("cancellation_reason")
)

df_cancellations = df_cancellations.dropDuplicates(["diverted", "cancelled", "cancellation_reason"])

w = Window.orderBy("diverted", "cancelled", "cancellation_reason")
df_cancellations = df_cancellations.withColumn("cancellations_id", row_number().over(w))

df_cancellations.write.mode("append").saveAsTable("flights_silver.cancellations")

# COMMAND ----------

df_wheels = df_flights_csv.select(
    col("wheels_on").cast("integer"),
    col("wheels_off").cast("integer")
)

df_wheels = df_wheels.dropDuplicates(["wheels_on", "wheels_off"])

w = Window.orderBy("wheels_on", "wheels_off")
df_wheels = df_wheels.withColumn("wheels_id", row_number().over(w))

df_wheels.write.mode("append").saveAsTable("flights_silver.wheels")

# COMMAND ----------

df_taxis = df_flights_csv.select(
    col("taxi_out").cast("integer"),
    col("taxi_in").cast("integer")
)

df_taxis = df_taxis.dropDuplicates(["taxi_out", "taxi_in"])

w = Window.orderBy("taxi_out", "taxi_in")
df_taxis = df_taxis.withColumn("taxis_id", row_number().over(w))

df_taxis.write.mode("append").saveAsTable("flights_silver.taxis")

# COMMAND ----------

df_departures = df_flights_csv.select(
    col("scheduled_departure").cast("integer"),
    col("departure_time").cast("integer"),
    col("departure_delay").cast("integer")
)

df_departures = df_departures.dropDuplicates(["scheduled_departure", "departure_time", "departure_delay"])

w = Window.orderBy("scheduled_departure", "departure_time", "departure_delay")
df_departures = df_departures.withColumn("departures_id", row_number().over(w))

df_departures.write.mode("append").saveAsTable("flights_silver.departures")

# COMMAND ----------

df_arrivals = df_flights_csv.select(
    col("scheduled_arrival").cast("integer"),
    col("arrival_time").cast("integer"),
    col("arrival_delay").cast("integer")
)

df_arrivals = df_arrivals.dropDuplicates(["scheduled_arrival", "arrival_time", "arrival_delay"])

w = Window.orderBy("scheduled_arrival", "arrival_time", "arrival_delay")
df_arrivals = df_arrivals.withColumn("arrivals_id", row_number().over(w))

df_arrivals.write.mode("append").saveAsTable("flights_silver.arrivals")

# COMMAND ----------

df_overall_schedules = df_flights_csv.select(
    col("scheduled_time").cast("integer"),
    col("elapsed_time").cast("integer"),
    col("air_time").cast("integer")
)

df_overall_schedules = df_overall_schedules.dropDuplicates(["scheduled_time", "elapsed_time", "air_time"])

w = Window.orderBy("scheduled_time", "elapsed_time", "air_time")
df_overall_schedules = df_overall_schedules.withColumn("schedules_id", row_number().over(w))

df_overall_schedules.write.mode("append").saveAsTable("flights_silver.overall_schedules")

# COMMAND ----------

df_delays = df_flights_csv.select(
    col("air_system_delay").cast("integer"),
    col("security_delay").cast("integer"),
    col("airline_delay").cast("integer"),
    col("late_aircraft_delay").cast("integer"),
    col("weather_delay").cast("integer")
)

df_delays = df_delays.dropDuplicates(["air_system_delay", "security_delay", "airline_delay", "late_aircraft_delay", "weather_delay"])

w = Window.orderBy("air_system_delay", "security_delay", "airline_delay", "late_aircraft_delay", "weather_delay")
df_delays = df_delays.withColumn("delays_id", row_number().over(w))

df_delays.write.mode("append").saveAsTable("flights_silver.delays")

# COMMAND ----------

df_times = df_flights_csv \
    .join(
    df_overall_schedules.select("scheduled_time", "elapsed_time", "air_time", "schedules_id"),
    on=["scheduled_time", "elapsed_time", "air_time"],
    how="left"
    ) \
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
    .select("schedules_id", "wheels_id", "taxis_id", "departures_id", "arrivals_id", "delays_id")

df_times = df_times.withColumnRenamed("schedules_id", "overall_schedules_id")

df_times = df_times.dropDuplicates(["overall_schedules_id", "wheels_id", "taxis_id", "departures_id", "arrivals_id", "delays_id"])

w = Window.orderBy("overall_schedules_id", "wheels_id", "taxis_id", "departures_id", "arrivals_id", "delays_id")
df_times = df_times.withColumn("times_id", row_number().over(w))

df_times.write.mode("append").saveAsTable("flights_silver.times")

# COMMAND ----------

