from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import Window
import pyspark.sql.functions as f
from zipfile import ZipFile
from pathlib import Path
import os
import re
import csv
import logging
from io import StringIO

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def extract_rows_from_zip(zip_file_path, csv_file_name):
    with ZipFile(zip_file_path, "r") as zf:
        with zf.open(csv_file_name) as csvfile:
            contents = StringIO(csvfile.read().decode("utf-8"))
            reader = csv.DictReader(contents)
            for row in reader:
                yield dict(row)


def get_csv_file_names(zip_file_path):
    with ZipFile(zip_file_path, "r") as zf:
        file_names = [
            file_name
            for file_name in zf.namelist()
            if re.match(r"^[^_].*\.csv$", file_name)
        ]
        return file_names


def create_file_meta_columns(df, csv_file_name):
    return df.withColumn("source_file", f.lit(csv_file_name)).withColumn(
        "file_date",
        f.concat_ws("-", f.slice(f.split("source_file", "-"), 3, 3)).cast("date"),
    )


def rank_capacity(df):
    window = Window.orderBy(f.desc("capacity_bytes"))
    df = (
        df.select("capacity_bytes", "model", "serial_number")
        .withColumn("capacity_bytes", f.col("capacity_bytes").cast("long"))
        .withColumn("storage_rank", f.dense_rank().over(window))
        .withColumnRenamed("model", "model_right")
        .withColumnRenamed("serial_number", "serial_number_right")
        .drop("capacity_bytes")
    )
    return df


def extract_brand(df):
    return df.withColumn(
        "brand",
        f.when(
            f.size(f.split("model", " ")) > 1,
            f.element_at(f.split("model", " "), 1),
        )
        .otherwise("unknown")
        .alias("brand"),
    )


def read_df_from_zip(spark_session, zip_file_path, csv_file_name):
    data_generator_obj = extract_rows_from_zip(zip_file_path, csv_file_name)
    row_generator = (Row(**row) for row in data_generator_obj)
    df = spark_session.createDataFrame(row_generator)
    return df


def main():
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()
    zip_file_path = (
        Path(os.getcwd()) / "data" / "hard-drive-2022-01-01-failures.csv.zip"
    )
    csv_file_name = get_csv_file_names(zip_file_path)[0]
    df = read_df_from_zip(spark, zip_file_path, csv_file_name)
    df = create_file_meta_columns(df, csv_file_name)
    df = extract_brand(df)
    df_ranked_capacity = rank_capacity(df)
    df = (
        df.join(
            df_ranked_capacity,
            (df.model == df_ranked_capacity.model_right)
            & (df.serial_number == df_ranked_capacity.serial_number_right),
        )
        .drop("model_right", "serial_number_right")
        .withColumn(
            "primary_key", f.md5(f.concat(f.col("serial_number"), f.col("model")))
        )
    )
    print(df.show())


if __name__ == "__main__":
    main()
