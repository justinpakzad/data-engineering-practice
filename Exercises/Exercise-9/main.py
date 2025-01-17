import polars as pl


def bike_rides_per_day(lazy_frame):
    bike_rides_per_day = (
        lazy_frame.group_by(pl.col("started_at").dt.truncate("1d"))
        .agg(pl.col("ride_id").unique().count().alias("ride_count"))
        .sort(by="started_at")
    )
    return bike_rides_per_day


def weekly_stats(lazy_frame):
    weekly_stats = (
        lazy_frame.group_by(pl.col("started_at").dt.truncate("1w"))
        .agg(pl.col("ride_id").count().alias("n_trips"))
        .select(
            pl.col("n_trips").min().alias("min_rides_per_week"),
            pl.col("n_trips").max().alias("max_rides_per_week"),
            pl.col("n_trips").mean().alias("avg_rides_per_week"),
        )
    )
    return weekly_stats


def weekly_ride_diffs(bike_rides_lazy_frame):
    weekly_ride_diffs = (
        bike_rides_lazy_frame.with_columns(
            pl.col("ride_count").shift(7).alias("ride_count_lag"),
            pl.col("ride_count").cast(pl.Int32),
        )
        .with_columns(
            (pl.col("ride_count") - pl.col("ride_count_lag").cast(pl.Int32)).alias(
                "weekly_ride_count_diff"
            )
        )
        .select("started_at", "weekly_ride_count_diff")
    )
    return weekly_ride_diffs


def main():
    lazy_frame = pl.scan_csv("data/202306-divvy-tripdata.csv", infer_schema_length=1000)
    lazy_frame = lazy_frame.with_columns(
        pl.col(c).str.to_datetime("%Y-%m-%d %H:%M:%S")
        for c in ["started_at", "ended_at"]
    )

    df_biker_rides_per_day = bike_rides_per_day(lazy_frame)
    df_weekly_stats = weekly_stats(lazy_frame)
    df_weekly_ride_diffs = weekly_ride_diffs(df_biker_rides_per_day)
    for df in [
        df_biker_rides_per_day.collect(),
        df_weekly_stats.collect(),
        df_weekly_ride_diffs.collect(),
    ]:
        print(df.head())


if __name__ == "__main__":
    main()
