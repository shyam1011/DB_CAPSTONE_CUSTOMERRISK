# ----------------------
# 1. LOAN APPLICATION DATA
# ----------------------

import dlt
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, DoubleType, DateType

# Define schema (if known) for better performance
loan_schema = StructType() \
    .add("application_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("loan_amount", DoubleType()) \
    .add("status", StringType()) \
    .add("application_date", DateType()) \
    .add("channel", StringType())

# ----------------------
# BRONZE LAYER -  READ DATA using Autoloader
# ----------------------

@dlt.table(
    name="bronze_loan_applications",
    comment="Raw historic loan applications from storage"
)
def bronze_loan_applications():
    return (
        spark.read.format("cloudFiles")
        .option("cloudFiles.format", "json")  # or "csv"
        .option("cloudFiles.inferColumnTypes", "true")
        .schema(loan_schema)
        .load("abfss://customer360-metastore-root@customer360shyam.dfs.core.windows.net/loan_applications/")
    )

# ----------------------
# SILVER LAYER
# ----------------------

@dlt.table(
    name="silver_loan_applications",
    comment="Cleaned and validated loan applications"
)
def silver_loan_applications():
    df = dlt.read("bronze_loan_applications")
    return (
        df
        .dropDuplicates(["application_id"])
        .filter(col("loan_amount") > 0)
        .filter(col("status").isin("approved", "pending", "rejected"))
    )



# ----------------------
# 2. CUSTOMER PROFILE DATA
# ----------------------

import dlt
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, DateType, TimestampType

# Schema definition (helps with performance and evolution)
customer_schema = StructType() \
    .add("customer_id", StringType()) \
    .add("name", StringType()) \
    .add("email", StringType()) \
    .add("phone", StringType()) \
    .add("dob", DateType()) \
    .add("created_at", TimestampType()) \
    .add("is_deleted", StringType())

# ----------------------
# BRONZE: Raw Data Load using Autoloader
# ----------------------

@dlt.table(
    name="bronze_customer_profiles",
    comment="Raw historic customer profiles from data lake"
)
def bronze_customer_profiles():
    return (
        spark.read.format("cloudFiles")
        .option("cloudFiles.format", "json")  # adjust to "csv" or "parquet" if needed
        .schema(customer_schema)
        .load("abfss://customer360-metastore-root@customer360shyam.dfs.core.windows.net/customers/")
    )

# ----------------------
# SILVER LAYER
# ----------------------

@dlt.table(
    name="silver_customer_profiles",
    comment="Cleaned and validated loan applications"
)
def silver_loan_applications():
    df = dlt.read("bronze_customer_profiles")
    return (
        df
        .dropDuplicates(["customer_id"])
        .filter(col("customer_id") IS NULL)
    )


# ----------------------
# 3. TRANSACTION DATA LOAD
# ----------------------
import dlt
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Define schema for transaction message
transaction_schema = StructType() \
    .add("transaction_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("currency", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("status", StringType())

# ----------------------
# BRONZE: Kafka ingestion
# ----------------------

@dlt.table(
    name="bronze_transactions",
    comment="Raw transaction messages from Kafka"
)
def bronze_transactions():
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "sap_transaction.kafka.server:9092")
        .option("subscribe", "transactions_historic")
        .option("startingOffsets", "earliest")  # Load historic data
        .load("abfss://customer360-metastore-root@customer360shyam.dfs.core.windows.net/transactions/")
        .selectExpr("CAST(value AS STRING) as json_str")
        .withColumn("data", from_json(col("json_str"), transaction_schema))
        .select("data.*")
    )

# ----------------------
# SILVER: Cleaned transactions
# ----------------------

@dlt.table(
    name="silver_transactions",
    comment="Validated and structured transactions"
)
def silver_transactions():
    df = dlt.read_stream("bronze_transactions")
    return (
        df
        .filter(col("amount") > 0)
        .filter(col("status").isin("completed", "pending", "failed"))
        .dropDuplicates(["transaction_id"])
    )


# ----------------------
# 3. CREDIT SCORE DATA LOAD
# ----------------------
import requests
import pandas as pd
from pyspark.sql import SparkSession

# Setup
spark = SparkSession.builder.getOrCreate()
headers = {"Authorization": "Bearer <YOUR_API_KEY>"}
api_url = "https://api.creditscore.com/v1/historic-scores"

# Placeholder: Pagination logic
def fetch_all_scores():
    all_data = []
    page = 1
    while True:
        response = requests.get(f"{api_url}?page={page}", headers=headers)
        if response.status_code != 200:
            break
        data = response.json()
        if not data["scores"]:
            break
        all_data.extend(data["scores"])
        page += 1
    return all_data

# Call API
credit_scores = fetch_all_scores()
df = pd.DataFrame(credit_scores)

# Save to bronze location
bronze_path = "abfss://customer360-metastore-root@customer360shyam.dfs.core.windows.net/credit_scores/"
spark_df = spark.createDataFrame(df)
spark_df.write.format("json").mode("overwrite").save(bronze_path)
