import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    unix_timestamp,
    sum as _sum,
    date_format,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)
import great_expectations as gx
from great_expectations.expectations import ExpectColumnValuesToBeBetween


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def test_bikeride_duration(df, max_hours=12):
    max_seconds = max_hours * 3600

    context = gx.get_context()
    data_source = context.data_sources.add_spark(name="duration_source")
    data_asset = data_source.add_dataframe_asset(name="daily_durations_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "daily_durations_batch"
    )

    batch_parameters = {"dataframe": df}
    expectation = ExpectColumnValuesToBeBetween(
        column="total_duration_seconds", max_value=max_seconds, min_value=0
    )

    batch = batch_definition.get_batch(batch_parameters=batch_parameters)
    validation_results = batch.validate(expectation)

    logging.info(f"Validation passed: {validation_results.success}")
    if not validation_results.success:
        logging.error(f"Found records where total duration exceeds {max_hours} hours!")

    return validation_results.success


# Create a SparkSession
spark = SparkSession.builder.appName("BikeRideDuration").getOrCreate()

# Define the schema based on the provided CSV structure
schema = StructType(
    [
        StructField("ride_id", StringType(), True),
        StructField("rideable_type", StringType(), True),
        StructField("started_at", StringType(), True),
        StructField("ended_at", StringType(), True),
        StructField("start_station_name", StringType(), True),
        StructField("start_station_id", StringType(), True),
        StructField("end_station_name", StringType(), True),
        StructField("end_station_id", StringType(), True),
        StructField("start_lat", DoubleType(), True),
        StructField("start_lng", DoubleType(), True),
        StructField("end_lat", DoubleType(), True),
        StructField("end_lng", DoubleType(), True),
        StructField("member_casual", StringType(), True),
    ]
)

input_csv_path = "data/202306-divvy-tripdata.csv"

df = spark.read.csv(input_csv_path, header=True, schema=schema, mode="DROPMALFORMED")

df = df.withColumn(
    "started_at", to_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ss")
).withColumn("ended_at", to_timestamp(col("ended_at"), "yyyy-MM-dd HH:mm:ss"))


df = df.withColumn(
    "duration_seconds",
    unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at")),
)

df = df.withColumn("date", date_format(col("started_at"), "yyyy-MM-dd"))

daily_durations = df.groupBy("date").agg(
    _sum("duration_seconds").alias("total_duration_seconds")
)


res = test_bikeride_duration(daily_durations, max_hours=6)

output_parquet_path = "results/output_file.parquet"
daily_durations.write.mode("overwrite").parquet(output_parquet_path)
