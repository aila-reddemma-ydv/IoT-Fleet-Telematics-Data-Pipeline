from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, max, min, round, when

PROJECT_PATH = "/home/rgukt/Bridgelabz/projects/fleet-telematics-pipeline"

INPUT_PATH = f"file://{PROJECT_PATH}/data/processed/clean_sensor_data.csv"
DRIVER_OUTPUT = f"file://{PROJECT_PATH}/data/processed/driver_behavior"
FUEL_OUTPUT = f"file://{PROJECT_PATH}/data/processed/fuel_efficiency"


def main():

    spark = (
        SparkSession.builder
        .appName("FleetDriverFuelAnalytics")
        .master("local[*]")
        .getOrCreate()
    )

    print("Reading cleaned sensor data...")

    df = spark.read.csv(
        INPUT_PATH,
        header=True,
        inferSchema=True
    )

    # Driver behavior analytics
    driver_behavior = (
        df.groupBy("device_id")
        .agg(
            count("*").alias("total_records"),
            round(avg("speed"), 2).alias("average_speed"),
            round(max("speed"), 2).alias("maximum_speed"),
            round(min("speed"), 2).alias("minimum_speed"),
            round(avg("rpm"), 2).alias("average_rpm"),
            round(avg("gps_speed"), 2).alias("average_gps_speed")
        )
    )

    driver_behavior = driver_behavior.withColumn(
        "speed_behavior",
        when(driver_behavior["maximum_speed"] > 100, "High Speed")
        .when(driver_behavior["average_speed"] > 80, "Aggressive")
        .otherwise("Normal")
    )

    # Fuel efficiency analytics
    fuel_efficiency = (
        df.groupBy("device_id")
        .agg(
            count("*").alias("total_records"),
            round(avg("kpl"), 2).alias("average_kpl"),
            round(max("kpl"), 2).alias("maximum_kpl"),
            round(min("kpl"), 2).alias("minimum_kpl"),
            round(avg("speed"), 2).alias("average_speed"),
            round(avg("rpm"), 2).alias("average_rpm")
        )
    )

    fuel_efficiency = fuel_efficiency.withColumn(
        "fuel_efficiency_status",
        when(fuel_efficiency["average_kpl"] >= 15, "Good")
        .when(fuel_efficiency["average_kpl"] >= 10, "Moderate")
        .otherwise("Low")
    )

    print("Writing driver behavior report...")

    driver_behavior.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(DRIVER_OUTPUT)

    print("Writing fuel efficiency report...")

    fuel_efficiency.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(FUEL_OUTPUT)

    print("Driver behavior analytics completed.")
    print("Fuel efficiency analytics completed.")

    spark.stop()


if __name__ == "__main__":
    main()