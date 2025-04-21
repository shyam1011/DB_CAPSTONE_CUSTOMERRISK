# -----------------------------------
# CUSTOMER RISK - TRAINING DATASET
# -----------------------------------

import dlt
from pyspark.sql.functions import col, avg, count, max

@dlt.table(
    name="silver_ml_dataset",
    comment="Training dataset with features from customer, transactions, and risk data"
)
def gold_customer_risk_training_dataset():
    # Read from Silver layer
    customers = dlt.read("silver_customer_profile")
    transactions = dlt.read("silver_transaction_data")
    risks = dlt.read("silver_customer_risk")

    # Aggregate transaction-level features
    txn_features = (
        transactions
        .groupBy("customer_id")
        .agg(
            count("*").alias("txn_count"),
            avg("amount").alias("avg_txn_amount"),
            max("amount").alias("max_txn_amount")
        )
    )

    # Join features
    training_data = (
        customers.alias("c")
        .join(txn_features.alias("t"), "customer_id", "left")
        .join(risks.alias("r"), "customer_id", "left")
    )

    # Define binary target variable
    training_data = training_data.withColumn(
        "is_high_risk", when(col("r.risk_score") > 0.7, 1).otherwise(0)
    )

    # Select final training columns
    return training_data.select(
        col("c.customer_id"),
        col("c.gender"),
        col("c.age"),
        col("t.txn_count"),
        col("t.avg_txn_amount"),
        col("t.max_txn_amount"),
        col("r.risk_score"),
        col("r.risk_category"),
        col("r.risk_score_timestamp"),
        col("is_high_risk")
    ).filter("risk_score IS NOT NULL")
