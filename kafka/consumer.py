import json
import os
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "fleet-telematics"
)

VALID_OUTPUT_PATH = os.getenv(
    "RAW_OUTPUT_PATH",
    "data/raw/validated_sensor_data.jsonl"
)

INVALID_OUTPUT_PATH = os.getenv(
    "INVALID_OUTPUT_PATH",
    "data/raw/invalid_sensor_data.jsonl"
)

Path(VALID_OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
Path(INVALID_OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)


def validate_record(record):
    required_fields = ["trip_id", "device_id", "timestamp"]

    for field in required_fields:
        if field not in record or record[field] in [None, ""]:
            return False, f"Missing field: {field}"

    numeric_fields = [
    "gps_speed",
    "battery",
    "c_temp",
    "dtc",
    "e_load",
    "iat",
    "imap",
    "kpl",
    "maf",
    "rpm",
    "speed",
    "t_adv",
    "t_pos"
]

    for field in numeric_fields:
        if field in record and record[field] not in [None, ""]:
            try:
                float(record[field])
            except ValueError:
                return False, f"Invalid numeric value in: {field}"

    return True, "Valid"


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="fleet-telematics-validation-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
        consumer_timeout_ms=10000
    )

    valid_count = 0
    invalid_count = 0

    print(f"Consumer started for topic: {TOPIC}")
    print("Waiting for messages...")

    try:
        with open(VALID_OUTPUT_PATH, "a", encoding="utf-8") as valid_file, \
             open(INVALID_OUTPUT_PATH, "a", encoding="utf-8") as invalid_file:

            for message in consumer:
                record = message.value
                is_valid, reason = validate_record(record)

                if is_valid:
                    valid_file.write(json.dumps(record) + "\n")
                    valid_count += 1
                else:
                    invalid_record = {
                        "record": record,
                        "reason": reason
                    }
                    invalid_file.write(
                        json.dumps(invalid_record) + "\n"
                    )
                    invalid_count += 1

                if (valid_count + invalid_count) % 1000 == 0:
                    valid_file.flush()
                    invalid_file.flush()

                    print(
                        f"Processed: {valid_count + invalid_count}, "
                        f"Valid: {valid_count}, "
                        f"Invalid: {invalid_count}"
                    )

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")

    finally:
        consumer.close()
        print(f"Valid records: {valid_count}")
        print(f"Invalid records: {invalid_count}")


if __name__ == "__main__":
    main()
