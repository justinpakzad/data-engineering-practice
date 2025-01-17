import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from main import AnalyticsReports


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    return spark


@pytest.fixture(scope="function")
def mock_dataframe(spark):
    spark = SparkSession.builder.master("local[1]").appName("TestSession").getOrCreate()
    data = [
        (
            25223640,
            "2019-10-01 00:01:39",
            "2019-10-01 00:17:20",
            9400,
            20,
            "Sheffield Ave & Kingsbury St",
            309,
            "Leavitt St & Armitage Ave",
            "Male",
            1987,
        ),
        (
            25223641,
            "2019-10-01 00:02:16",
            "2019-10-01 00:06:34",
            2580,
            19,
            "Throop (Loomis) St & Taylor St",
            241,
            "Morgan St & Polk St",
            "Male",
            1998,
        ),
        (
            25223642,
            "2019-10-01 00:04:32",
            "2019-10-01 00:18:43",
            8500,
            84,
            "Milwaukee Ave & Grand Ave",
            199,
            "Wabash Ave & Grand Ave",
            "Female",
            1991,
        ),
        (
            25223643,
            "2019-10-01 00:04:32",
            "2019-10-01 00:43:43",
            23500,
            313,
            "Lakeview Ave & Fullerton Pkwy",
            290,
            "Kedzie Ave & Palmer Ct",
            "Male",
            1990,
        ),
        (
            25223644,
            "2019-10-01 00:04:34",
            "2019-10-01 00:35:42",
            18670,
            210,
            "Ashland Ave & Division St",
            382,
            "Western Ave & Congress Pkwy",
            "Male",
            1987,
        ),
        (
            25223645,
            "2019-10-01 00:04:38",
            "2019-10-01 00:10:51",
            3730,
            156,
            "Clark St & Wellington Ave",
            226,
            "Racine Ave & Belmont Ave",
            "Female",
            1994,
        ),
        (
            25223646,
            "2019-10-02 00:04:52",
            "2019-10-02 00:22:45",
            10720,
            84,
            "Milwaukee Ave & Grand Ave",
            142,
            "McClurg Ct & Erie St",
            "Female",
            1991,
        ),
        (
            25223647,
            "2019-10-02 00:04:57",
            "2019-10-02 00:29:16",
            14580,
            156,
            "Clark St & Wellington Ave",
            463,
            "Clark St & Berwyn Ave",
            "Male",
            1995,
        ),
        (
            25223648,
            "2019-10-02 00:05:20",
            "2019-10-02 00:29:18",
            14370,
            156,
            "Clark St & Wellington Ave",
            463,
            "Clark St & Berwyn Ave",
            "Female",
            1993,
        ),
    ]

    schema = [
        "trip_id",
        "start_time",
        "end_time",
        "tripduration",
        "from_station_id",
        "from_station_name",
        "to_station_id",
        "to_station_name",
        "gender",
        "birthyear",
    ]

    df = spark.createDataFrame(data, schema=schema)

    df = df.withColumn("start_time", f.col("start_time").cast("timestamp")).withColumn(
        "end_time", f.col("end_time").cast("timestamp")
    )

    return df


def test_avg_trip_duration_daily(spark, mock_dataframe, tmp_path):
    analytics_report = AnalyticsReports(df=mock_dataframe, folder_path=tmp_path)
    df_avg_trip_duration = analytics_report.avg_trip_duration(write_to_file=False)
    expected_data = [("2019-10-01", 11063.33), ("2019-10-02", 13223.33)]
    expected_df = spark.createDataFrame(
        expected_data, ["trip_date", "avg_tripduration"]
    )
    assert df_avg_trip_duration.collect() == expected_df.collect()


def test_n_trips_daily(spark, mock_dataframe, tmp_path):
    analytics_report = AnalyticsReports(df=mock_dataframe, folder_path=tmp_path)
    df_n_trips_daily = analytics_report.n_trips_daily(write_to_file=False)
    expected_data = [("2019-10-01", 6), ("2019-10-02", 3)]
    expected_df = spark.createDataFrame(expected_data, ["trip_date", "count"])
    assert df_n_trips_daily.collect() == expected_df.collect()


def test_gender_longest_trip_avg(spark, mock_dataframe, tmp_path):
    analytics_report = AnalyticsReports(df=mock_dataframe, folder_path=tmp_path)
    df_test_gender_longest_trip_avg = analytics_report.gender_trip_longest_avg(
        write_to_file=False
    )
    expected_data = [("Male", 13746.0)]
    expected_df = spark.createDataFrame(expected_data, ["gender", "avg_tripduration"])
    assert df_test_gender_longest_trip_avg.collect() == expected_df.collect()
