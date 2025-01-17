import boto3
from dotenv import load_dotenv
import os
from io import StringIO, BytesIO
import gzip


def get_object(s3, bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return response
    except Exception as e:
        print(f"Error: {e}")
        return None


def get_uri(response):
    with gzip.GzipFile(fileobj=response.get("Body")) as f:
        return f.readline().decode("utf-8").strip()


def main():
    load_dotenv()
    s3 = boto3.client(
        # Used personal creds,
        # perm issues with accesing the public bucket
        "s3",
        aws_access_key_id=os.getenv("AKEYID"),
        aws_secret_access_key=os.getenv("SECKEY"),
    )
    bucket = "commoncrawl"
    response_wet_path = get_object(
        s3, bucket=bucket, key="crawl-data/CC-MAIN-2024-51/wet.paths.gz"
    )
    uri = get_uri(response_wet_path)
    response = get_object(s3, bucket=bucket, key=uri)
    with gzip.GzipFile(fileobj=response.get("Body")) as f:
        for line in f.readlines():
            print(line.decode("utf-8"))


if __name__ == "__main__":
    main()
