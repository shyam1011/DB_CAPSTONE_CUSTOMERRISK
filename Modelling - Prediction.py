# -----------------------------------
# PREDICT - CUSTOMERS
# -----------------------------------

#Load Model
import mlflow
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd

# Load latest production model
model_uri = "models:/customer_risk_classifier/Production"
model = mlflow.pyfunc.load_model(model_uri)

# Load scoring dataset from Gold layer
df = spark.table("customer360.gold.customer_risk_training_dataset")
features = ["txn_count", "avg_txn_amount", "max_txn_amount"]

# Define Pandas UDF for scoring
@pandas_udf(DoubleType())
def predict_risk_udf(*cols):
    X = pd.concat(cols, axis=1)
    preds = model.predict_proba(X)[:, 1]  # probability of high risk
    return pd.Series(preds)

# Apply prediction UDF
scored_df = df.withColumn(
    "predicted_risk_score", 
    predict_risk_udf(*[df[col] for col in features])
).withColumn(
    "predicted_risk_label",
    (df["predicted_risk_score"] > 0.7).cast("int")  # threshold for high risk
)

# Save to Gold table
scored_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("customer360.gold.customer_risk_predictions")

# -----------------------------------
# OPTIMIZE - RISK PREDICTIONS
# -----------------------------------

OPTIMIZE customer360.gold.customer_risk_predictions
ZORDER BY (predicted_risk_label, customer_id);