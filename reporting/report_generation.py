# Databricks notebook source

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

df_origins_delays = spark.table("flights_gold.origins_delays")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Distances analysis
# MAGIC
# MAGIC ## Daily distances

# COMMAND ----------

df_daily_distances = spark.table("flights_gold.daily_distances")

# COMMAND ----------

import plotly.express as px
from pyspark.sql.functions import concat_ws, to_date

df_date = df_daily_distances.withColumn(
    "date",
    to_date(
        concat_ws(
            "-",
            df_daily_distances.year,
            df_daily_distances.month,
            df_daily_distances.day,
        ),
    ),
)

df_sorted = df_date.select("date", "overall_distance").orderBy("date")

df_pd = df_sorted.toPandas()

fig = px.line(df_pd, x="date", y="overall_distance", title="Daily distance (miles)")
fig.show()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Distances frequency

# COMMAND ----------

df_routes = spark.table("flights_silver.routes")

df_pd = df_routes.select("distance").toPandas()

fig = px.histogram(
    df_pd,
    x="distance",
    nbins=80,
    title="Histogram of Route Distances",
)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Most frequent causes of delays
# MAGIC
# MAGIC ## Most common causes of delays in states with greatest total delay

# COMMAND ----------

df_origins_delays = spark.table("flights_gold.origins_delays")

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import sum as spark_sum

delay_cols = [
    "air_system_delay",
    "security_delay",
    "airline_delay",
    "late_aircraft_delay",
    "weather_delay",
]

df_grouped = df_origins_delays.groupBy("origin_airport_state").agg(
    *[spark_sum(c).alias(c) for c in delay_cols],
)

df_total = df_grouped.withColumn(
    "total_delay",
    sum([col(c) for c in delay_cols]),
)

# 6 states with greatest total delay
top6_states = df_total.orderBy(col("total_delay").desc()).limit(6)

pdf = top6_states.toPandas()

for c in delay_cols:
    pdf[c] = pdf[c] / pdf["total_delay"]

# COMMAND ----------

pdf_melt = pdf.melt(
    id_vars="origin_airport_state",
    value_vars=delay_cols,
    var_name="delay_type",
    value_name="proportion",
)

fig = px.bar(
    pdf_melt,
    x="origin_airport_state",
    y="proportion",
    color="delay_type",
    title="Delay causes in top 6 states with greatest total delay",
    barmode="stack",
)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Average delay of each cause

# COMMAND ----------

from pyspark.sql.functions import avg

df_delays = spark.table("flights_silver.delays")

df_avg = df_delays.agg(
    *[avg(c).alias(c) for c in delay_cols],
)

pdf = df_avg.toPandas().T.reset_index()
pdf.columns = ["delay_type", "avg_delay"]

fig = px.bar(
    pdf,
    x="delay_type",
    y="avg_delay",
    title="Mean delays by delay type (minutes)",
)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Most frequent destinations cancellations

# COMMAND ----------

df_destinations_cancellations = spark.table("flights_gold.destinations_cancellations")

df_reasons = df_destinations_cancellations.groupBy("cancellation_reason").agg(
    spark_sum("count").alias("total_cancellations"),
)

df_pd = df_reasons.toPandas()

fig = px.pie(
    df_pd,
    names="cancellation_reason",
    values="total_cancellations",
    title="Proportion of Cancellation Reasons",
)

fig.show()


# COMMAND ----------

df_destinations_cancellations = spark.table("flights_gold.destinations_cancellations")

df_reasons = (
    df_destinations_cancellations.where(
        col("destination_airport_city") == "Los Angeles",
    )
    .groupBy("cancellation_reason")
    .agg(spark_sum("count").alias("total_cancellations"))
)

df_pd = df_reasons.toPandas()

fig = px.pie(
    df_pd,
    names="cancellation_reason",
    values="total_cancellations",
    title="Proportion of Cancellation Reasons in Los Angeles",
)

fig.show()


# COMMAND ----------
