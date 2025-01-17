import requests
import os
import zipfile
import time

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def download_zip_files(download_uris, folder):
    for uri in download_uris:
        try:
            response = requests.get(uri)
            if response.status_code == 200:
                file_name = uri.split("/", -1)[-1]
                with open(f"{folder}/{file_name}", "wb") as f:
                    f.write(response.content)
        except Exception as e:
            print(f"Error: {e}")


def extract_zip_files(folder):
    for item in os.listdir(folder):
        try:
            with zipfile.ZipFile(f"{folder}/{item}") as zipf:
                for zip_contents in zipf.namelist():
                    if not zip_contents.startswith("_"):
                        zipf.extract(zip_contents, folder)
                        os.remove(f"{folder}/{item}")
        except Exception as e:
            print(f"Error: {e} for file {item}")


def main():
    start = time.perf_counter()
    if not os.path.isdir("downloads"):
        os.mkdir("downloads")

    download_zip_files(download_uris=download_uris, folder="downloads")
    extract_zip_files(folder="downloads")
    end = time.perf_counter()
    print(f"Time taken: {end - start}")


if __name__ == "__main__":
    main()
