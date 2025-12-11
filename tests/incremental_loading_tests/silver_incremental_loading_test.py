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

airlines_schema = StructType(
    [
        StructField("airline_iata_code", StringType(), True),
        StructField("airline", StringType(), True),
    ],
)

df_airlines = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(airlines_schema)
    .load("/Volumes/workspace/flightsschema/flightsvolume/airlines.csv")
)

last_two_rows = df_airlines.tail(2)
df_last_two = spark.createDataFrame(last_two_rows, airlines_schema)

delta_tbl = DeltaTable.forName(spark, "flights_bronze.airlines")

old_n_rows = delta_tbl.toDF().count()

_ = (
    delta_tbl.alias("dt")
    .merge(
        df_last_two.alias("lt"),
        "dt.airline_iata_code = lt.airline_iata_code",  # do not insert row with existing `airline_iata_code`
    )
    .whenNotMatchedInsertAll()  # insert not matched rows
    .execute()
)

new_n_rows = delta_tbl.toDF().count()

expected_added = df_last_two.join(
    delta_tbl.toDF(),
    on=["airline_iata_code"],
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

new_data = [("NOT-EXISTING", "Not Existing Airline")]

df_airlines = spark.createDataFrame(new_data, schema=airlines_schema)

last_two_rows = df_airlines.tail(2)
df_last_two = spark.createDataFrame(last_two_rows, airlines_schema)

delta_tbl = DeltaTable.forName(spark, "flights_bronze.airlines")

old_n_rows = delta_tbl.toDF().count()

_ = (
    delta_tbl.alias("dt")
    .merge(
        df_last_two.alias("lt"),
        "dt.airline_iata_code = lt.airline_iata_code",  # do not insert row with existing `airline_iata_code`
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
