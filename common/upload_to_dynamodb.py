import os
import csv
from decimal import Decimal

import boto3
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
DYNAMODB_TABLE_NAME = os.getenv(
    "DYNAMODB_TABLE_NAME",
    "fleet-vehicle-status"
)

ANALYTICS_DIRECTORY = "data/processed/vehicle_analytics"


def find_analytics_file():
    """
    Find the Spark-generated CSV file.
    Ignore Spark's _SUCCESS file.
    """
    for filename in os.listdir(ANALYTICS_DIRECTORY):
        if filename.endswith(".csv"):
            return os.path.join(ANALYTICS_DIRECTORY, filename)

    raise FileNotFoundError(
        "No analytics CSV file found in data/processed/vehicle_analytics"
    )


def convert_value(value):
    """
    Convert numeric CSV values to Decimal for DynamoDB.
    Keep text values as strings.
    """
    if value is None or value == "":
        return None

    value = value.strip()

    try:
        return Decimal(value)
    except Exception:
        return value


def upload_analytics_to_dynamodb():
    analytics_file = find_analytics_file()

    dynamodb = boto3.resource(
        "dynamodb",
        region_name=AWS_REGION
    )

    table = dynamodb.Table(DYNAMODB_TABLE_NAME)

    print(f"Reading file: {analytics_file}")
    print(f"Writing to table: {DYNAMODB_TABLE_NAME}")

    uploaded_count = 0

    with open(analytics_file, "r", newline="") as file:
        reader = csv.DictReader(file)

        print("CSV columns:", reader.fieldnames)

        if "device_id" not in reader.fieldnames:
            raise ValueError(
                "CSV does not contain the required device_id column"
            )

        with table.batch_writer() as batch:
            for row in reader:
                device_id = row.get("device_id")

                if device_id is None or device_id.strip() == "":
                    print("Skipping record without device_id")
                    continue

                # Build the item and explicitly set the required key.
                item = {
                    "device_id": str(device_id).strip()
                }

                for key, value in row.items():
                    if key == "device_id":
                        continue

                    converted_value = convert_value(value)

                    if converted_value is not None:
                        item[key] = converted_value

                batch.put_item(Item=item)

                uploaded_count += 1
                print(f"Prepared device: {item['device_id']}")

    print(
        f"Analytics data uploaded to DynamoDB successfully. "
        f"Records uploaded: {uploaded_count}"
    )


if __name__ == "__main__":
    upload_analytics_to_dynamodb()
