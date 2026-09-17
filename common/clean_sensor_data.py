import json
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

INPUT_PATH = os.getenv(
    "RAW_OUTPUT_PATH",
    "data/raw/validated_sensor_data.jsonl"
)

OUTPUT_PATH = "data/processed/clean_sensor_data.csv"


def load_jsonl(file_path):
    records = []

    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()

            if line:
                records.append(json.loads(line))

    return records


def clean_sensor_data(records):
    df = pd.DataFrame(records)

    numeric_columns = [
        "trip_id",
        "device_id",
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

    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(
                df[column],
                errors="coerce"
            )

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            errors="coerce"
        )

    df = df.dropna(
        subset=["trip_id", "device_id", "timestamp"]
    )

    df = df.drop_duplicates()

    df = df.sort_values(
        by=["device_id", "timestamp"]
    )

    return df


def main():
    output_file = Path(OUTPUT_PATH)
    output_file.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    print(f"Reading: {INPUT_PATH}")

    records = load_jsonl(INPUT_PATH)

    if not records:
        print("No records found.")
        return

    df = clean_sensor_data(records)

    df.to_csv(
        OUTPUT_PATH,
        index=False
    )

    print(f"Records read: {len(records)}")
    print(f"Records after cleaning: {len(df)}")
    print(f"Output created: {OUTPUT_PATH}")
    print(f"Columns: {list(df.columns)}")


if __name__ == "__main__":
    main()
