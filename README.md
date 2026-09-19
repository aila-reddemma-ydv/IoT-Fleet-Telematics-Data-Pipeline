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

```text
raw/
processed/
curated/
reports/
