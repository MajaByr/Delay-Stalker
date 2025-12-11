# Databricks notebook source

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

airports_schema = StructType(
    [
        StructField("airport_iata_code", StringType(), True),
        StructField("airport", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
    ],
)

# COMMAND ----------

df_airports = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(airports_schema)
    .load("/Volumes/workspace/flightsschema/flightsvolume/airports.csv")
)

last_two_rows = df_airports.tail(2)
df_last_two = spark.createDataFrame(last_two_rows, airports_schema)

delta_tbl = DeltaTable.forName(spark, "flights_bronze.airports")

old_n_rows = delta_tbl.toDF().count()

_ = (
    delta_tbl.alias("dt")
    .merge(
        df_last_two.alias("lt"),
        "dt.airport_iata_code = lt.airport_iata_code",  # do not insert row with existing `airport_iata_code`
    )
    .whenNotMatchedInsertAll()  # insert not matched rows
    .execute()
)

new_n_rows = delta_tbl.toDF().count()

expected_added = df_last_two.join(
    delta_tbl.toDF(),
    on=["airport_iata_code"],
    how="left_anti",
).count()

if new_n_rows != old_n_rows + expected_added:
    raise ValueError(
        f"Incorrect number of rows. Rows before={old_n_rows}, Rows after={new_n_rows}, "
        f"Expected={old_n_rows + expected_added}",
    )

else:
    print(
        f"Rows before={old_n_rows}, Rows after={new_n_rows}, "
        f"Expected={old_n_rows + expected_added}",
    )

# COMMAND ----------

new_data = [
    (
        "NOT-EXISTING-AIRPORT",
        "Not Existing Airport",
        "NE City",
        "NE Country",
        "NE State",
        "4188.418",
        "5555.000",
    ),
]

df_airports = spark.createDataFrame(new_data, schema=airports_schema)

last_two_rows = df_airports.tail(2)
df_last_two = spark.createDataFrame(last_two_rows, airports_schema)

delta_tbl = DeltaTable.forName(spark, "flights_bronze.airports")

old_n_rows = delta_tbl.toDF().count()

_ = (
    delta_tbl.alias("dt")
    .merge(
        df_last_two.alias("lt"),
        "dt.airport_iata_code = lt.airport_iata_code",  # do not insert row with existing `airport_iata_code`
    )
    .whenNotMatchedInsertAll()  # insert not matched rows
    .execute()
)

new_n_rows = delta_tbl.toDF().count()

if new_n_rows != old_n_rows + 1:
    raise ValueError(
        f"Incorrect number of rows. Rows before={old_n_rows}, Rows after={new_n_rows}, "
        f"Expected={old_n_rows + 1}",
    )

else:
    print(
        f"Rows before={old_n_rows}, Rows added={new_n_rows}, "
        f"Expected={old_n_rows + 1}",
    )

# COMMAND ----------
