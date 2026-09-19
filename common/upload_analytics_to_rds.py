import os
import csv
from decimal import Decimal

import pymysql
from dotenv import load_dotenv

load_dotenv()

RDS_HOST = os.getenv("RDS_HOST")
RDS_PORT = int(os.getenv("RDS_PORT", "3306"))
RDS_DATABASE = os.getenv("RDS_DATABASE")
RDS_USER = os.getenv("RDS_USER")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")

ANALYTICS_DIRECTORY = "data/processed/vehicle_analytics"


def find_analytics_file():
    for filename in os.listdir(ANALYTICS_DIRECTORY):
        if filename.endswith(".csv"):
            return os.path.join(ANALYTICS_DIRECTORY, filename)

    raise FileNotFoundError(
        "No analytics CSV file found."
    )


def to_float(value):
    if value is None or value == "":
        return None

    return float(Decimal(str(value)))


def upload_analytics():

    analytics_file = find_analytics_file()

    print(f"Analytics file: {analytics_file}")
    print(f"Connecting to RDS: {RDS_HOST}")

    connection = pymysql.connect(
        host=RDS_HOST,
        port=RDS_PORT,
        user=RDS_USER,
        password=RDS_PASSWORD,
        database=RDS_DATABASE,
        connect_timeout=15,
        cursorclass=pymysql.cursors.DictCursor
    )

    try:

        insert_query = """
        INSERT INTO vehicle_analytics (
            device_id,
            total_records,
            average_speed,
            average_gps_speed,
            average_fuel_efficiency,
            average_battery,
            average_engine_temperature,
            average_rpm,
            vehicle_health_status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            total_records = VALUES(total_records),
            average_speed = VALUES(average_speed),
            average_gps_speed = VALUES(average_gps_speed),
            average_fuel_efficiency = VALUES(average_fuel_efficiency),
            average_battery = VALUES(average_battery),
            average_engine_temperature = VALUES(average_engine_temperature),
            average_rpm = VALUES(average_rpm),
            vehicle_health_status = VALUES(vehicle_health_status)
        """

        uploaded_count = 0

        with open(analytics_file, "r", newline="") as file:

            reader = csv.DictReader(file)

            with connection.cursor() as cursor:

                for row in reader:

                    values = (
                        str(row["device_id"]).strip(),
                        int(row["total_records"]),
                        to_float(row["average_speed"]),
                        to_float(row["average_gps_speed"]),
                        to_float(row["average_fuel_efficiency"]),
                        to_float(row["average_battery"]),
                        to_float(row["average_engine_temperature"]),
                        to_float(row["average_rpm"]),
                        row["vehicle_health_status"]
                    )

                    cursor.execute(insert_query, values)

                    uploaded_count += 1

                    print(
                        f"Uploaded device: {row['device_id']}"
                    )

        connection.commit()

        print()
        print("RDS upload completed successfully.")
        print(f"Records uploaded: {uploaded_count}")

    finally:

        connection.close()

        print("RDS connection closed.")


if __name__ == "__main__":
    upload_analytics()
