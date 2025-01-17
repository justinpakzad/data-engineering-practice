import csv
import re
import logging
import os
from io import StringIO
from zipfile import ZipFile
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as f
from pathlib import Path
from pyspark import Row

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class AnalyticsReports:
    def __init__(self, df, folder_path):
        self.df = df
        self.folder_path = folder_path
        folder_path.mkdir(exist_ok=True)

    def avg_trip_duration(self, write_to_file=False):
        """1.What is the average trip duration per day?"""
        df_avg_trip_duration = (
            self.df.groupBy(
                f.date_format("start_time", "yyyy-MM-dd").alias("trip_date")
            )
            .agg(f.round(f.avg("tripduration"), 2).alias("avg_trip_duration"))
            .orderBy("trip_date")
        )
        if write_to_file:
            df_avg_trip_duration.coalesce(1).write.option("header", True).csv(
                f"{self.folder_path}/avg_trip_duration"
            )
        return df_avg_trip_duration

    def n_trips_daily(self, write_to_file=False):
        """How many trips were taken each day?"""
        df_n_trips = (
            self.df.groupBy(
                f.date_format("start_time", "yyyy-MM-dd").alias("trip_date")
            )
            .count()
            .orderBy("trip_date")
        )
        if write_to_file:
            df_n_trips.coalesce(1).write.option("header", True).csv(
                f"{self.folder_path}/n_trip_daily"
            )
        return df_n_trips

    def most_popular_starting_station_monthly(self, write_to_file=False):
        """What was the most popular starting trip station for each month?"""
        window_spec = Window.partitionBy("month").orderBy(f.desc("cnt"))
        df_stations_by_month = (
            self.df.groupBy(
                f.date_format("start_time", "MMMM").alias("month"),
                f.col("from_station_name"),
            )
            .agg(f.count("*").alias("cnt"))
            .withColumn("rn", f.row_number().over(window_spec))
            .filter(f.col("rn") == 1)
            .drop("rn")
        )
        if write_to_file:
            df_stations_by_month.coalesce(1).write.option("header", True).csv(
                f"{self.folder_path}/popular_stations_by_month"
            )
        return df_stations_by_month

    def most_popular_stations_last_two_weeks(self, write_to_file=False):
        """What were the top 3 trip stations each day for the last two weeks?"""
        window_spec = Window.partitionBy("day").orderBy(f.desc("cnt"))
        two_weeks_prev = self.df.select(f.date_add(f.max("start_time"), -14)).collect()[
            0
        ][0]
        df_most_pop_last_two_weeks = (
            self.df.filter(f.col("start_time") >= two_weeks_prev)
            .groupBy(
                f.date_format("start_time", "yyyy MM dd").alias("day"),
                f.trim(f.col("from_station_name")),
            )
            .agg(f.count("*").alias("cnt"))
            .withColumn("rn", f.row_number().over(window_spec))
            .filter(f.col("rn") <= 3)
            .drop("rn")
        )
        if write_to_file:
            df_most_pop_last_two_weeks.coalesce(1).write.option("header", True).csv(
                f"{self.folder_path}/most_popular_station_last_two_weeks"
            )
        return df_most_pop_last_two_weeks

    def gender_trip_longest_avg(self, write_to_file=False):
        """Do Males or Females take longer trips on average?"""
        window_spec = Window.orderBy(f.desc("avg_tripduration"))

        df_gender_trip_longest_avg = (
            self.df.groupBy("gender")
            .agg(f.round(f.avg("tripduration"), 2).alias("avg_tripduration"))
            .filter(f.trim("gender") != "")
            .withColumn("rnk", f.row_number().over(window_spec))
            .filter(f.col("rnk") == 1)
            .drop("rnk")
        )
        if write_to_file:
            df_gender_trip_longest_avg.coalesce(1).write.option("header", True).csv(
                f"{self.folder_path}/gender_trip_longest_avg"
            )
        return df_gender_trip_longest_avg

    def top_10_ages_longest_shortest_trips(self, write_to_file=False):
        """
        What is the top 10 ages of those that take the
        longest trips, and shortest?
        """
        df_with_ages = self.df.withColumn(
            "age", f.year(f.current_date()) - f.col("birthyear")
        )
        df_longest_trips_by_age = (
            df_with_ages.filter(f.col("age").isNotNull())
            .orderBy(f.desc("tripduration"))
            .limit(10)
            .select(f.col("age").alias("ages_longes_trip"))
        ).withColumn("id", f.monotonically_increasing_id())

        df_shortest_trips_by_age = (
            df_with_ages.filter(f.col("age").isNotNull())
            .orderBy("tripduration")
            .limit(10)
            .select(f.col("age").alias("ages_shortes_trip"))
        ).withColumn("id", f.monotonically_increasing_id())

        df_full = df_longest_trips_by_age.join(df_shortest_trips_by_age, "id").drop(
            "id"
        )

        if write_to_file:
            df_full.coalesce(1).write.option("header", True).csv(
                f"{self.folder_path}/top10_ages_trip_duration"
            )

    def full_report(self):
        logging.info("Starting full report generation...")

        self.avg_trip_duration(write_to_file=True)
        logging.info("Average Trip Duration Report generated successfully.")

        self.n_trips_daily(write_to_file=True)
        logging.info("Number of Trips Daily Report generated successfully.")

        self.most_popular_starting_station_monthly(write_to_file=True)
        logging.info(
            "Most Popular Starting Station (Monthly) Report generated successfully."
        )

        self.most_popular_stations_last_two_weeks(write_to_file=True)
        logging.info(
            "Most Popular Station (Daily) Last Two Weeks Report generated successfully."
        )

        self.gender_trip_longest_avg(write_to_file=True)
        logging.info("Longest Trip By Gender Averages Report generated successfully.")

        self.top_10_ages_longest_shortest_trips(write_to_file=True)
        logging.info(
            "Top 10 Ages (Longest & Shortest Trips) Report generated successfully."
        )

        logging.info("Full report generation completed successfully.")


def convert_dtypes(df):
    df = df.withColumns({c: f.trim(c) for c in df.columns})
    cols_to_convert_to_int = {
        c: f.col(c).cast("integer") for c in df.columns if "id" in c or c == "birthyear"
    }
    cols_to_convert_to_dt = {
        c: f.col(c).cast("timestamp") for c in df.columns if "time" in c
    }

    df = df.withColumns({**cols_to_convert_to_dt, **cols_to_convert_to_int})
    return df


def align_columns(df):
    return (
        df.withColumnsRenamed(
            {
                "ride_id": "trip_id",
                "start_station_id": "from_station_id",
                "start_station_name": "from_station_name",
                "started_at": "start_time",
                "ended_at": "end_time",
                "end_station_name": "to_station_name",
                "end_station_id": "to_station_id",
            }
        )
        .select(
            "trip_id",
            "from_station_id",
            "from_station_name",
            "start_time",
            "end_time",
            "to_station_name",
            "to_station_id",
        )
        .withColumn(
            "tripduration",
            f.to_timestamp("end_time").cast("long")
            - f.to_timestamp("start_time").cast("long"),
        )
        .withColumn("tripduration", f.col("tripduration").cast("long"))
    )


def extract_rows_from_zip(zip_file_path):
    with ZipFile(zip_file_path, "r") as zf:
        csv_files = [
            file_name
            for file_name in zf.namelist()
            if re.match(r"^[^_].*\.csv$", file_name)
        ]
        for csv_file in csv_files:
            with zf.open(csv_file) as csvfile:
                contents = StringIO(csvfile.read().decode("utf-8"))
                reader = csv.DictReader(contents)
                for row in reader:
                    yield dict(row)


def create_data_generators(data_path):
    row_generators = {}
    for file_path in data_path.iterdir():
        file_name = str(file_path).split("/", -1)[-1].split(".")[0].lower().strip()
        row_generators[file_name] = (
            Row(**row) for row in extract_rows_from_zip(file_path)
        )
    return row_generators


def clean_crc_files(crc_path):
    for crc_file in crc_path.rglob("*.crc"):
        crc_file.unlink()


def rename_csv_files(csv_path):
    for csv_file in csv_path.rglob("*.csv"):
        folder = "/".join(str(csv_file).split("/")[:-1])
        proper_file_name = f'{"".join(str(csv_file).split("/")[-2])}.csv'
        full_path = f"{folder}/{proper_file_name}"
        csv_file.rename(full_path)


def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    spark.conf.set("parquet.fs.output.write-checksum", "false")
    data_path = Path(os.getcwd()) / "data"
    csv_folder_path = Path(os.getcwd()) / "csvs"
    data_dict = create_data_generators(data_path)
    df_2019 = (
        spark.createDataFrame(data_dict.get("divvy_trips_2019_q4"))
        .drop(
            "usertype",
            "bikeid",
        )
        .withColumn(
            "tripduration",
            f.regexp_replace(f.col("tripduration"), "[,.]", "").cast("long"),
        )
    )
    df_2020 = (
        align_columns(spark.createDataFrame(data_dict.get("divvy_trips_2020_q1")))
        .withColumn("gender", f.lit(None).cast("string"))
        .withColumn("birthyear", f.lit(None).cast("string"))
    )
    df = convert_dtypes(df_2019.unionByName(df_2020))
    analytics_report = AnalyticsReports(df=df, folder_path=csv_folder_path)
    analytics_report.full_report()
    clean_crc_files(csv_folder_path)
    rename_csv_files(csv_folder_path)


if __name__ == "__main__":
    main()
