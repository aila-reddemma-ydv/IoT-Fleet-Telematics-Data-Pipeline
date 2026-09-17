import pandas as pd

file_path = "datasets/v2.csv"
chunk_size = 100000

total_rows = 0
unique_devices = set()
unique_trips = set()

numeric_columns = [
    "gps_speed",
    "battery",
    "cTemp",
    "dtc",
    "eLoad",
    "iat",
    "imap",
    "kpl",
    "maf",
    "rpm",
    "speed",
    "tAdv",
    "tPos"
]

missing_counts = {column: 0 for column in numeric_columns}
zero_counts = {column: 0 for column in numeric_columns}
invalid_counts = {column: 0 for column in numeric_columns}

min_values = {column: float("inf") for column in numeric_columns}
max_values = {column: float("-inf") for column in numeric_columns}

for chunk in pd.read_csv(
    file_path,
    chunksize=chunk_size,
    low_memory=False
):

    total_rows += len(chunk)

    unique_devices.update(chunk["deviceID"].dropna().unique())
    unique_trips.update(chunk["tripID"].dropna().unique())

    for column in numeric_columns:

        # Convert values to numeric.
        # Invalid values become NaN instead of causing an error.
        numeric_values = pd.to_numeric(
            chunk[column],
            errors="coerce"
        )

        # Count values that could not be converted
        invalid_counts[column] += (
            numeric_values.isna() & chunk[column].notna()
        ).sum()

        # Count missing values after conversion
        missing_counts[column] += numeric_values.isna().sum()

        # Count zero values
        zero_counts[column] += (numeric_values == 0).sum()

        # Calculate min and max safely
        if numeric_values.notna().any():

            min_values[column] = min(
                min_values[column],
                numeric_values.min()
            )

            max_values[column] = max(
                max_values[column],
                numeric_values.max()
            )

print("\n" + "=" * 60)
print("FULL DATASET INSPECTION")
print("=" * 60)

print("\nTotal rows:", total_rows)
print("Unique devices:", len(unique_devices))
print("Device IDs:", sorted(map(str, unique_devices))[:20])

print("\nUnique trips:", len(unique_trips))
print("Trip IDs:", sorted(map(str, unique_trips))[:20])

print("\n" + "-" * 60)
print("COLUMN STATISTICS")
print("-" * 60)

for column in numeric_columns:

    print(f"\nColumn: {column}")
    print("  Missing values:", missing_counts[column])
    print("  Invalid values:", invalid_counts[column])
    print("  Zero values:", zero_counts[column])
    print("  Minimum:", min_values[column])
    print("  Maximum:", max_values[column])
