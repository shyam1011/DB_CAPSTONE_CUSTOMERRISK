# -----------------------------------
# ML MODEL - Using MLFlow
# -----------------------------------

import pandas as pd
import mlflow
import mlflow.sklearn
import mlflow.lightgbm

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import SVC
import lightgbm as lgb

# Load data from Silver table
df = spark.table("customer360.silver.silver_ml_dataset").toPandas()

# Feature selection
features = ["txn_count", "avg_txn_amount", "max_txn_amount","Gender","Age"]
target = "is_high_risk"

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(df[features], df[target], test_size=0.2, random_state=42)

# Define models to train
models = {
    "LogisticRegression": LogisticRegression(max_iter=1000),
    "RandomForest": RandomForestClassifier(n_estimators=100),
    "LightGBM": lgb.LGBMClassifier(),
    "SVM": SVC(probability=True),
    "NaiveBayes": GaussianNB()
}

# -----------------------------------
# MLFlow - Experiment
# -----------------------------------
mlflow.set_experiment("/CustomerRiskModeling")

for name, model in models.items():
    with mlflow.start_run(run_name=name):
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        # Metrics
        acc = accuracy_score(y_test, y_pred)
        prec = precision_score(y_test, y_pred)
        rec = recall_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred)

        # Logging
        mlflow.log_param("model", name)
        mlflow.log_metrics({
            "accuracy": acc,
            "precision": prec,
            "recall": rec,
            "f1_score": f1
        })

        # Log model
        if name == "LightGBM":
            mlflow.lightgbm.log_model(model, "model")
        else:
            mlflow.sklearn.log_model(model, "model")

        print(f"{name}: accuracy={acc:.4f}, precision={prec:.4f}, recall={rec:.4f}, f1={f1:.4f}")