# ----------------------------------
# 1. INCREMENTAL DATA - LOAN APPLICATION
# ----------------------------------
import dlt
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, TimestampType, DoubleType

# Schema for Loan Application
loan_schema = StructType() \
    .add("application_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("loan_amount", DoubleType()) \
    .add("loan_type", StringType()) \
    .add("application_status", StringType()) \
    .add("last_updated_at", TimestampType()) \
    .add("created_at", TimestampType())

# ----------------------------------
# BRONZE: Load raw incremental data
# ----------------------------------

@dlt.table(
    name="bronze_loan_applications",
    comment="Raw loan application data (incremental loads)"
)
def bronze_loan_applications():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")  # change to csv/parquet if needed
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "/mnt/files/customer360-metastore-root/chekpoint/loan_schema")
        .load("/mnt/files/customer360-metastore-root/loan_applications/")
    )

# ----------------------------------
# SILVER: Clean and deduplicate
# ----------------------------------

@dlt.table(
    name="silver_loan_applications",
    comment="Deduplicated and validated loan applications"
)
def silver_loan_applications():
    df = dlt.read_stream("bronze_loan_applications")
    return (
        df
        .filter("application_id IS NOT NULL")
        .withWatermark("last_updated_at", "1 day")
        .dropDuplicates(["application_id", "last_updated_at"])
    )


# ----------------------------------
# 2. INCREMENTAL DATA - CUSTOMER PROFILE
# ----------------------------------
import dlt
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, TimestampType, DateType

# Define schema for Customer Profile
customer_schema = StructType() \
    .add("customer_id", StringType()) \
    .add("first_name", StringType()) \
    .add("last_name", StringType()) \
    .add("email", StringType()) \
    .add("phone_number", StringType()) \
    .add("address", StringType()) \
    .add("date_of_birth", DateType()) \
    .add("created_at", TimestampType()) \
    .add("last_updated_at", TimestampType())

# -------------------------------
# BRONZE: Raw Customer Ingestion
# -------------------------------

@dlt.table(
    name="bronze_customer_profile",
    comment="Raw customer profile data (incremental via Autoloader)"
)
def bronze_customer_profile():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")  # or csv/parquet
        .option("cloudFiles.schemaLocation", "/mnt/files/customer360-metastore-root/chekpoint/customer_profile/")
        .load("/mnt/files/customer360-metastore-root/customer_profile/")
    )

# -------------------------------
# SILVER: Deduplicated Customer
# -------------------------------

@dlt.table(
    name="silver_customer_profile",
    comment="Clean and deduplicated customer profiles"
)
def silver_customer_profile():
    df = dlt.read_stream("bronze_customer_profile")
    return (
        df
        .filter("customer_id IS NOT NULL")
        .withWatermark("last_updated_at", "1 day")
        .dropDuplicates(["customer_id", "last_updated_at"])
    )



# ----------------------------------
# 3. INCREMENTAL DATA - TRANSACTION DATA
# ----------------------------------
import dlt
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Transaction schema
transaction_schema = StructType() \
    .add("transaction_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("currency", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("status", StringType()) \
    .add("last_updated_at", TimestampType())

# ----------------------------------
# BRONZE: Raw Incremental Load
# ----------------------------------

@dlt.table(
    name="bronze_transaction_data",
    comment="Raw transaction data ingested incrementally"
)
def bronze_transaction_data():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")  # Or csv/parquet
        .option("cloudFiles.schemaLocation", "/mnt/files/customer360-metastore-root/chekpoint/transactions/")
        .load("/mnt/files/customer360-metastore-root/transactions/")
    )

# ----------------------------------
# SILVER: Clean & Deduplicated
# ----------------------------------

@dlt.table(
    name="silver_transaction_data",
    comment="Cleaned and deduplicated transaction records"
)
def silver_transaction_data():
    df = dlt.read_stream("bronze_transaction_data")
    return (
        df
        .filter("transaction_id IS NOT NULL AND amount > 0")
        .withWatermark("last_updated_at", "1 day")
        .dropDuplicates(["transaction_id", "last_updated_at"])
    )


# ----------------------------------
# 3. INCREMENTAL DATA - CREDIT RISK API
# ----------------------------------

# ----------------------------------
# API CALL - INCREMENTAL
# ----------------------------------

import requests
import json
from datetime import datetime
from pyspark.sql import SparkSession

# Example API call
url = "https://api.partnerbank.com/customer-risk"
headers = {"Authorization": f"Bearer YOUR_TOKEN"}
params = {"updated_since": "2024-01-01T00:00:00Z"}  # or dynamic based on last sync

response = requests.get(url, headers=headers, params=params)
data = response.json()

# Save to DBFS/ADLS
now = datetime.now().strftime("%Y%m%d_%H%M%S")
output_path = f"/dbfs/mnt/raw/customer_risk/risk_data_{now}.json"

with open(output_path, "w") as f:
    json.dump(data, f)


# -----------------------------------
# BRONZE: Raw incremental risk data
# -----------------------------------

@dlt.table(
    name="bronze_customer_risk",
    comment="Raw risk scores from API (incremental)"
)
def bronze_customer_risk():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/files/customer360-metastore-root/chekpoint/customer_risk/")
        .load("/mnt/files/customer360-metastore-root/customer_risk/")
    )

# -----------------------------------
# SILVER: Cleaned + deduplicated
# -----------------------------------

@dlt.table(
    name="silver_customer_risk",
    comment="Clean and deduplicated customer risk scores"
)
def silver_customer_risk():
    df = dlt.read_stream("bronze_customer_risk")
    return (
        df
        .filter("customer_id IS NOT NULL")
        .withWatermark("risk_score_timestamp", "1 day")
        .dropDuplicates(["customer_id", "risk_score_timestamp"])
    )