import os
import boto3
from dotenv import load_dotenv

load_dotenv()

BUCKET = os.getenv("S3_BUCKET_NAME")
REPORTS_PREFIX = os.getenv("S3_REPORTS_PREFIX", "reports/")

s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION"))


def upload_report(local_path, s3_key):
    s3.upload_file(local_path, BUCKET, s3_key)
    print(f"Uploaded: s3://{BUCKET}/{s3_key}")


def main():
    driver_file = "data/processed/driver_behavior/part-00000*.csv"
    fuel_file = "data/processed/fuel_efficiency/part-00000*.csv"

    import glob

    driver_files = glob.glob(driver_file)
    fuel_files = glob.glob(fuel_file)

    if driver_files:
        upload_report(
            driver_files[0],
            f"{REPORTS_PREFIX}driver_behavior.csv"
        )

    if fuel_files:
        upload_report(
            fuel_files[0],
            f"{REPORTS_PREFIX}fuel_efficiency.csv"
        )

    print("Reports uploaded successfully.")


if __name__ == "__main__":
    main()
