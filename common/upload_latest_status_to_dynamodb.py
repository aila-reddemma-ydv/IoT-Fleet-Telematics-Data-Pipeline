import os
import csv
from decimal import Decimal
from datetime import datetime, timezone

import boto3
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
TABLE_NAME = os.getenv(
    "DYNAMODB_STATUS_TABLE_NAME",
    "fleet-latest-sensor-status"
)

ANALYTICS_DIRECTORY = "data/processed/vehicle_analytics"


def find_analytics_file():
    for filename in os.listdir(ANALYTICS_DIRECTORY):
        if filename.endswith(".csv"):
            return os.path.join(ANALYTICS_DIRECTORY, filename)

    raise FileNotFoundError(
        "No analytics CSV file found."
    )


def to_decimal(value):
    if value is None or value == "":
        return None

    return Decimal(str(value))


def upload_latest_status():
    analytics_file = find_analytics_file()

    dynamodb = boto3.resource(
        "dynamodb",
        region_name=AWS_REGION
    )

    table = dynamodb.Table(TABLE_NAME)

    with open(analytics_file, "r", newline="") as file:
        reader = csv.DictReader(file)

        uploaded_count = 0

        for row in reader:
            device_id = str(row["device_id"]).strip()

            item = {
                "device_id": device_id,
                "timestamp": datetime.now(
                    timezone.utc
                ).isoformat(),
                "total_records": to_decimal(row["total_records"]),
                "average_speed": to_decimal(row["average_speed"]),
                "average_gps_speed": to_decimal(
                    row["average_gps_speed"]
                ),
                "average_fuel_efficiency": to_decimal(
                    row["average_fuel_efficiency"]
                ),
                "average_battery": to_decimal(
                    row["average_battery"]
                ),
                "average_engine_temperature": to_decimal(
                    row["average_engine_temperature"]
                ),
                "average_rpm": to_decimal(row["average_rpm"]),
                "vehicle_health_status": row[
                    "vehicle_health_status"
                ]
            }

            # Remove attributes whose values are missing.
            item = {
                key: value
                for key, value in item.items()
                if value is not None
            }

            table.put_item(Item=item)

            uploaded_count += 1
            print(f"Latest status saved for device: {device_id}")

    print(
        f"Latest sensor status upload completed. "
        f"Records uploaded: {uploaded_count}"
    )


if __name__ == "__main__":
    upload_latest_status()
