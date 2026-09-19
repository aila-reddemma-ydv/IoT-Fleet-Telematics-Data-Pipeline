import os
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv


# Load environment variables from .env
load_dotenv()


AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_PROCESSED_PREFIX = os.getenv("S3_PROCESSED_PREFIX", "processed/")

LOCAL_FILE = Path("data/processed/clean_sensor_data.csv")
S3_FILE_NAME = f"{S3_PROCESSED_PREFIX}clean_sensor_data.csv"


def upload_file_to_s3():
    """Upload the cleaned CSV file to Amazon S3."""

    if not S3_BUCKET_NAME:
        raise ValueError("S3_BUCKET_NAME is missing in the .env file.")

    if not LOCAL_FILE.exists():
        raise FileNotFoundError(
            f"Local file not found: {LOCAL_FILE}. "
            "Run the cleaning script first."
        )

    s3_client = boto3.client("s3", region_name=AWS_REGION)

    try:
        s3_client.upload_file(
            str(LOCAL_FILE),
            S3_BUCKET_NAME,
            S3_FILE_NAME
        )

        print("File uploaded successfully.")
        print(f"Local file: {LOCAL_FILE}")
        print(f"S3 location: s3://{S3_BUCKET_NAME}/{S3_FILE_NAME}")

    except (BotoCoreError, ClientError) as error:
        print(f"S3 upload failed: {error}")


if __name__ == "__main__":
    upload_file_to_s3()

