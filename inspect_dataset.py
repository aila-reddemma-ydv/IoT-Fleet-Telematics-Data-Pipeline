import pandas as pd

file_path = "datasets/v2.csv"

# Read only a small sample first
sample_df = pd.read_csv(file_path, nrows=10000)

print("\nColumn names:")
print(sample_df.columns.tolist())

print("\nData types:")
print(sample_df.dtypes)

print("\nFirst 5 rows:")
print(sample_df.head())

print("\nMissing values in sample:")
print(sample_df.isnull().sum())

print("\nUnique devices in sample:")
print(sample_df["deviceID"].nunique())

print("\nUnique trips in sample:")
print(sample_df["tripID"].nunique())

# Numeric columns to inspect
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

print("\nSample statistics:")
print(sample_df[numeric_columns].describe().T)

