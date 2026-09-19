from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    round,
    when
)


INPUT_PATH = "file:///home/rgukt/Bridgelabz/projects/fleet-telematics-pipeline/data/processed/clean_sensor_data.csv"
OUTPUT_PATH = "file:///home/rgukt/Bridgelabz/projects/fleet-telematics-pipeline/data/processed/vehicle_analytics"


def create_spark_session():
    """Create and return a Spark session."""

    return (
        SparkSession.builder
        .appName("FleetTelematicsAnalytics")
        .master("local[*]")
        .getOrCreate()
    )


def read_sensor_data(spark):
    """Read cleaned sensor data from CSV."""

    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_PATH)
    )


def create_vehicle_analytics(df):
    """Create vehicle-level analytical metrics."""

    analytics_df = (
        df.groupBy("device_id")
        .agg(
            count("*").alias("total_records"),
            round(avg("speed"), 2).alias("average_speed"),
            round(avg("gps_speed"), 2).alias("average_gps_speed"),
            round(avg("kpl"), 2).alias("average_fuel_efficiency"),
            round(avg("battery"), 2).alias("average_battery"),
            round(avg("c_temp"), 2).alias("average_engine_temperature"),
            round(avg("rpm"), 2).alias("average_rpm")
        )
    )

    analytics_df = analytics_df.withColumn(
        "vehicle_health_status",
        when(
            (col("average_battery") < 12) |
            (col("average_engine_temperature") > 100),
            "Needs Attention"
        ).otherwise("Healthy")
    )

    return analytics_df


def save_analytics(df):
    """Save analytics output as CSV."""

    (
        df.write
        .mode("overwrite")
        .option("header", True)
        .csv(OUTPUT_PATH)
    )

    print(f"Analytics output saved to: {OUTPUT_PATH}")


def main():
    spark = create_spark_session()

    try:
        sensor_df = read_sensor_data(spark)

        print("Input schema:")
        sensor_df.printSchema()

        print("Input record count:", sensor_df.count())

        analytics_df = create_vehicle_analytics(sensor_df)

        print("Vehicle analytics:")
        analytics_df.show(truncate=False)

        save_analytics(analytics_df)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
