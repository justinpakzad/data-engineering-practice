import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from main import extract_brand, rank_capacity


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    return spark


def test_extract_brand(spark):
    df = spark.createDataFrame(
        [("TOSHIBA MG07ACA14TA",), ("ST8000NM0055",)], schema=["model"]
    )
    expected_df = spark.createDataFrame(
        [("TOSHIBA MG07ACA14TA", "TOSHIBA"), ("ST8000NM0055", "unknown")],
        schema=["model", "brand"],
    )
    df_brand_extracted = extract_brand(df)

    assert df_brand_extracted.collect() == expected_df.collect()


def test_rank_capacity(spark):
    df = spark.createDataFrame(
        [
            ("Model_A", 2500, "JP101"),
            ("Model_B", 150000, "ZA202"),
            ("Model_C", 1000, "YT303"),
        ],
        ["model", "capacity_bytes", "serial_number"],
    )
    expected_df = spark.createDataFrame(
        [("Model_A", "JP101", 2), ("Model_B", "ZA202", 1), ("Model_C", "YT303", 3)],
        ["model_right", "serial_number_right", "storage_rank"],
    ).orderBy(f.desc("storage_rank"))
    df_ranked = rank_capacity(df).orderBy(f.desc("storage_rank"))

    assert df_ranked.collect() == expected_df.collect()
