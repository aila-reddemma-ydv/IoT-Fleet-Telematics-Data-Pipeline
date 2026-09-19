import os
import boto3
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_CURATED_PREFIX = os.getenv("S3_CURATED_PREFIX", "curated/")

LOCAL_ANALYTICS_PATH = (
    "data/processed/vehicle_analytics/part-00000-e464ffc0-ab86-4d5f-b5ab-b79af4b4dc25-c000.csv"
)

S3_OBJECT_KEY = f"{S3_CURATED_PREFIX}vehicle_analytics.csv"


def upload_analytics_to_s3():
    if not S3_BUCKET_NAME:
        raise ValueError("S3_BUCKET_NAME is missing in the .env file")

    if not os.path.exists(LOCAL_ANALYTICS_PATH):
        raise FileNotFoundError(
            f"Analytics file not found: {LOCAL_ANALYTICS_PATH}"
        )

    s3_client = boto3.client("s3", region_name=AWS_REGION)

    s3_client.upload_file(
        LOCAL_ANALYTICS_PATH,
        S3_BUCKET_NAME,
        S3_OBJECT_KEY
    )

    print("Analytics file uploaded successfully")
    print(f"S3 location: s3://{S3_BUCKET_NAME}/{S3_OBJECT_KEY}")


if __name__ == "__main__":
    upload_analytics_to_s3()
