import csv
import json
import os

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "fleet-telematics"
)

DATASET_PATH = os.getenv(
    "DATASET_PATH",
    "datasets/v2.csv"
)

BATCH_SIZE = int(
    os.getenv("PRODUCER_BATCH_SIZE", "1000")
)


def convert_value(value):
    """
    Convert CSV values into numbers when possible.
    Keep text values as strings.
    """
    if value is None:
        return None

    value = value.strip()

    if value == "":
        return None

    try:
        number = float(value)

        if number.is_integer():
            return int(number)

        return number

    except ValueError:
        return value


def normalize_record(row):
    """
    Convert the CSV column names into
    standard names used by the pipeline.
    """
    return {
        "trip_id": convert_value(row.get("tripID")),
        "device_id": convert_value(row.get("deviceID")),
        "timestamp": row.get("timeStamp"),
        "acc_data": row.get("accData"),
        "gps_speed": convert_value(row.get("gps_speed")),
        "battery": convert_value(row.get("battery")),
        "c_temp": convert_value(row.get("cTemp")),
        "dtc": convert_value(row.get("dtc")),
        "e_load": convert_value(row.get("eLoad")),
        "iat": convert_value(row.get("iat")),
        "imap": convert_value(row.get("imap")),
        "kpl": convert_value(row.get("kpl")),
        "maf": convert_value(row.get("maf")),
        "rpm": convert_value(row.get("rpm")),
        "speed": convert_value(row.get("speed")),
        "t_adv": convert_value(row.get("tAdv")),
        "t_pos": convert_value(row.get("tPos"))
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks="all",
        retries=3,
        value_serializer=lambda value: json.dumps(
            value
        ).encode("utf-8"),
        key_serializer=lambda key: str(key).encode("utf-8")
    )

    sent_count = 0

    print(f"Reading dataset: {DATASET_PATH}")
    print(f"Sending records to topic: {TOPIC}")

    try:
        with open(
            DATASET_PATH,
            "r",
            encoding="utf-8",
            errors="replace",
            newline=""
        ) as file:

            reader = csv.DictReader(file)

            for row in reader:
                record = normalize_record(row)

                producer.send(
                    TOPIC,
                    key=record["device_id"],
                    value=record
                )

                sent_count += 1

                if sent_count % BATCH_SIZE == 0:
                    producer.flush()
                    print(f"Sent {sent_count} records")

                # First test: send only 5,000 records
                if sent_count >= 5000:
                    break

    except KeyboardInterrupt:
        print("\nProducer stopped by user.")

    finally:
        producer.flush()
        producer.close()
        print(f"Total records sent: {sent_count}")


if __name__ == "__main__":
    main()
