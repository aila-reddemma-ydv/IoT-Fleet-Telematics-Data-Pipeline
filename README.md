# IoT Fleet Telematics Data Pipeline

## Overview

This project implements an end-to-end IoT sensor data analytics pipeline for fleet telematics data.

The pipeline collects vehicle sensor data through Apache Kafka, validates and cleans the data using Python, performs analytics using PySpark, and stores results using AWS services including Amazon S3, DynamoDB, Amazon RDS, AWS Lambda, and AWS Step Functions.

## Architecture

Kafka Producer → Kafka Topic → Kafka Consumer → Validation → Cleaning → S3 → PySpark Analytics → DynamoDB → RDS → Lambda → Step Functions → Reports

## Technologies Used

- Python
- Apache Kafka
- PySpark
- Pandas
- MySQL
- Amazon S3
- AWS Lambda
- Amazon DynamoDB
- Amazon RDS
- AWS Step Functions
- Boto3
- Git and GitHub

## Dataset

The telematics dataset contains vehicle sensor information such as:

- Trip ID
- Device ID
- Timestamp
- GPS speed
- Vehicle speed
- Battery
- Coolant temperature
- Engine load
- RPM
- Fuel efficiency
- Mass airflow
- Intake air temperature
- Diagnostic trouble codes

## Pipeline

### 1. Kafka Data Ingestion

The Kafka producer reads vehicle telematics data and publishes sensor records to the `fleet-telematics` topic.

### 2. Data Validation

The Kafka consumer validates incoming records and separates valid and invalid sensor records.

### 3. Data Cleaning

The cleaning process converts numeric fields, parses timestamps, removes missing identifiers, removes duplicates, and sorts the data.

### 4. Amazon S3 Data Lake

Data is organized into:

text
raw/
processed/
curated/
reports/
### 5. PySpark Analytics

PySpark calculates:

- Total records
- Average speed
- Average GPS speed
- Average fuel efficiency
- Average battery
- Average engine temperature
- Average RPM
- Vehicle health status
- Driver behavior
- Fuel efficiency status

### 6. DynamoDB

The project uses:

fleet-vehicle-status
fleet-latest-sensor-status

for vehicle analytics and latest vehicle status.

### 7. Amazon RDS

Amazon RDS MySQL stores vehicle analytics in the `fleet_analytics` database and `vehicle_analytics` table.

### 8. AWS Lambda

The `fleet-telematics-s3-validator` Lambda function processes S3 events for validated sensor data.

### 9. AWS Step Functions

The `fleet-telematics-workflow` state machine orchestrates the serverless workflow.

## Project Structure

fleet-telematics-pipeline/
├── common/
│   ├── clean_sensor_data.py
│   ├── driver_fuel_analytics.py
│   ├── pyspark_analytics.py
│   ├── upload_analytics_to_rds.py
│   ├── upload_analytics_to_s3.py
│   ├── upload_latest_status_to_dynamodb.py
│   ├── upload_raw_to_s3.py
│   ├── upload_reports_to_s3.py
│   ├── upload_to_dynamodb.py
│   └── upload_to_s3.py
├── kafka/
│   ├── producer.py
│   └── consumer.py
├── datasets/
├── data/
├── architecture/
├── logs/
├── requirements.txt
├── .gitignore
└── README.md

## Setup

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

Create a `.env` file with the required Kafka, AWS, S3, DynamoDB, and RDS configuration.

Do not commit `.env`, AWS credentials, database passwords, SSL certificates, or other secrets.

## Running the Pipeline

python3 kafka/producer.py
python3 kafka/consumer.py
python3 common/clean_sensor_data.py
python3 common/pyspark_analytics.py
python3 common/driver_fuel_analytics.py
python3 common/upload_to_s3.py
python3 common/upload_raw_to_s3.py
python3 common/upload_analytics_to_s3.py
python3 common/upload_to_dynamodb.py
python3 common/upload_latest_status_to_dynamodb.py
python3 common/upload_analytics_to_rds.py
python3 common/upload_reports_to_s3.py

## AWS Resources

- Amazon S3 for data lake storage
- Amazon DynamoDB for operational vehicle status
- Amazon RDS MySQL for relational analytics
- AWS Lambda for S3 event processing
- AWS Step Functions for workflow orchestration

## Security

Sensitive configuration is excluded using `.gitignore`.

The repository does not contain:

- `.env`
- AWS credentials
- Database passwords
- SSL certificates
- Generated sensor data
- Large datasets

## Results

The project demonstrates an end-to-end IoT fleet telematics data engineering workflow covering streaming ingestion, validation, cleaning, cloud storage, distributed analytics, NoSQL storage, relational storage, serverless processing, workflow orchestration, driver behavior analytics, and fuel efficiency analytics.

## Author
Aila Reddemma
