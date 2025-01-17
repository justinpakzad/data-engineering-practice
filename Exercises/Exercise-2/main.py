import pandas as pd


def main():
    url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"

    tables = pd.read_html(url)

    df = pd.concat(tables)

    file_name = df[df["Last modified"] == "2024-01-19 10:27"]["Name"].iloc[0]

    file_contents_df = pd.read_csv(f"{url}/{file_name}")

    highest_dry_bulb_temp = file_contents_df[
        file_contents_df["HourlyDryBulbTemperature"]
        == max(file_contents_df["HourlyDryBulbTemperature"])
    ]
    print(highest_dry_bulb_temp.head())


if __name__ == "__main__":
    main()
