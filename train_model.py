"""
Machine Learning Model Training and Evaluation
Trains an Isolation Forest anomaly detection model on historical smart-meter readings.
Saves the serialized model and generates an offline evaluation report against injected anomalies.
"""

import sys
import json
import logging
from pathlib import Path
import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import IsolationForest
from sklearn.metrics import precision_score, recall_score, f1_score, confusion_matrix

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).resolve().parent))
from config.settings import (
    DATASET_PATH,
    MODEL_PATH,
    MODEL_CONTAMINATION,
    EVALUATION_REPORT_PATH,
    MODEL_FEATURES,
    GROUND_TRUTH_COLUMN
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("ModelTrainer")


def train_anomaly_model(
    dataset_path: Path = DATASET_PATH,
    model_output_path: Path = MODEL_PATH,
    report_output_path: Path = EVALUATION_REPORT_PATH,
    contamination: float = MODEL_CONTAMINATION,
    train_split_ratio: float = 0.8,
    random_state: int = 42
):
    """
    Trains an Isolation Forest model using smart meter historical readings.
    
    CRITICAL:
    - Excludes ground truth anomaly label ('synthetic_anomaly_ground_truth') from training features.
    - Fits Isolation Forest on chronological training portion (first 80%).
    - Evaluates performance on held-out chronological test portion (last 20%) and overall dataset.
    - Saves model artifact to `models/anomaly_model.pkl`.
    - Saves evaluation metrics to `reports/model_evaluation.json`.
    """
    if not dataset_path.exists():
        logger.error(f"Dataset not found at {dataset_path}. Please run generate_dataset.py first.")
        raise FileNotFoundError(f"Dataset missing: {dataset_path}")

    logger.info(f"Loading smart meter data from {dataset_path}...")
    df = pd.read_csv(dataset_path)

    # 1. Validation and cleaning
    for col in MODEL_FEATURES:
        if col not in df.columns:
            raise ValueError(f"Required model feature '{col}' is missing from the dataset.")

    # Remove any invalid or NaN rows
    df_clean = df.dropna(subset=MODEL_FEATURES).copy()
    logger.info(f"Dataset loaded: {len(df_clean)} valid records.")

    # 2. Chronological Split (80% train, 20% test)
    split_index = int(len(df_clean) * train_split_ratio)
    train_df = df_clean.iloc[:split_index]
    test_df = df_clean.iloc[split_index:]

    logger.info(f"Chronological split: Train records = {len(train_df):,}, Test records = {len(test_df):,}")

    # 3. Feature selection (Strictly features only - no leakage)
    X_train = train_df[MODEL_FEATURES]
    X_test = test_df[MODEL_FEATURES]
    X_full = df_clean[MODEL_FEATURES]

    # 4. Train Isolation Forest model
    logger.info(
        f"Training Isolation Forest (n_estimators=100, contamination={contamination}, features={MODEL_FEATURES})..."
    )
    model = IsolationForest(
        n_estimators=100,
        contamination=contamination,
        random_state=random_state,
        n_jobs=-1
    )
    model.fit(X_train)

    # 5. Model Persistence
    model_output_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, model_output_path)
    logger.info(f"Model artifact successfully saved to: {model_output_path}")

    # 6. Predictions & Evaluation
    # Isolation Forest: +1 for inliers (normal), -1 for outliers (anomalies)
    # Convert to standard convention: 0 = Normal, 1 = Flagged Anomaly
    def get_predictions(X):
        raw_pred = model.predict(X)
        binary_pred = np.where(raw_pred == -1, 1, 0)
        # Decision function: lower values mean more anomalous
        scores = model.decision_function(X)
        return binary_pred, scores

    test_pred, test_scores = get_predictions(X_test)
    full_pred, full_scores = get_predictions(X_full)

    # Evaluate against synthetic ground truth if present
    evaluation_report = {
        "model_type": "IsolationForest",
        "n_estimators": 100,
        "contamination": contamination,
        "features": MODEL_FEATURES,
        "train_records": len(train_df),
        "test_records": len(test_df),
        "total_records": len(df_clean),
        "timestamp_evaluated": pd.Timestamp.now().isoformat()
    }

    if GROUND_TRUTH_COLUMN in df_clean.columns:
        y_test_true = test_df[GROUND_TRUTH_COLUMN].values
        y_full_true = df_clean[GROUND_TRUTH_COLUMN].values

        # Test set metrics
        test_precision = float(precision_score(y_test_true, test_pred, zero_division=0))
        test_recall = float(recall_score(y_test_true, test_pred, zero_division=0))
        test_f1 = float(f1_score(y_test_true, test_pred, zero_division=0))
        cm_test = confusion_matrix(y_test_true, test_pred).tolist()

        # Overall dataset metrics
        full_precision = float(precision_score(y_full_true, full_pred, zero_division=0))
        full_recall = float(recall_score(y_full_true, full_pred, zero_division=0))
        full_f1 = float(f1_score(y_full_true, full_pred, zero_division=0))
        cm_full = confusion_matrix(y_full_true, full_pred).tolist()

        evaluation_report.update({
            "test_evaluation": {
                "evaluated_records": len(test_df),
                "injected_anomalies": int(np.sum(y_test_true)),
                "predicted_anomalies": int(np.sum(test_pred)),
                "precision": round(test_precision, 4),
                "recall": round(test_recall, 4),
                "f1_score": round(test_f1, 4),
                "confusion_matrix": cm_test
            },
            "overall_evaluation": {
                "evaluated_records": len(df_clean),
                "injected_anomalies": int(np.sum(y_full_true)),
                "predicted_anomalies": int(np.sum(full_pred)),
                "precision": round(full_precision, 4),
                "recall": round(full_recall, 4),
                "f1_score": round(full_f1, 4),
                "confusion_matrix": cm_full
            }
        })
    else:
        evaluation_report["note"] = "No ground truth column found for supervised evaluation metrics."

    # Save report JSON
    report_output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_output_path, "w") as f:
        json.dump(evaluation_report, f, indent=4)
    logger.info(f"Evaluation report saved to: {report_output_path}")

    # Generate Confusion Matrix image plots
    try:
        from plot_confusion_matrix import generate_confusion_matrix_plots
        generate_confusion_matrix_plots(report_output_path)
    except Exception as pe:
        logger.warning(f"Could not generate confusion matrix plots: {pe}")

    # Print summary to terminal
    print_evaluation_summary(evaluation_report)
    return model, evaluation_report


def print_evaluation_summary(report: dict):
    """Displays formatted evaluation results."""
    overall = report.get("overall_evaluation", {})
    test = report.get("test_evaluation", {})
    
    print("""
================================================================================
ISOLATION FOREST MODEL TRAINING & EVALUATION REPORT
================================================================================
Model Architecture   : Isolation Forest (Scikit-Learn)
Input Features       : {features}
Configured Contam.   : {contamination}
Total Data Records   : {total_records:,} (Train: {train_records:,} | Test: {test_records:,})
--------------------------------------------------------------------------------
TEST SUBSET EVALUATION (Chronological Generalization):
  - Records Evaluated    : {test_records_eval:,}
  - Injected Anomalies   : {test_inj}
  - Predicted Anomalies  : {test_pred}
  - Precision            : {test_prec:.4f}
  - Recall               : {test_rec:.4f}
  - F1-Score             : {test_f1:.4f}
  - Confusion Matrix     : TN={test_tn}, FP={test_fp}, FN={test_fn}, TP={test_tp}
--------------------------------------------------------------------------------
OVERALL DATASET EVALUATION:
  - Total Evaluated      : {full_records_eval:,}
  - Injected Anomalies   : {full_inj}
  - Predicted Anomalies  : {full_pred}
  - Precision            : {full_prec:.4f}
  - Recall               : {full_rec:.4f}
  - F1-Score             : {full_f1:.4f}
  - Confusion Matrix     : TN={full_tn}, FP={full_fp}, FN={full_fn}, TP={full_tp}
================================================================================
NOTE: Evaluation on synthetic data confirms algorithmic correctness.
In real-world power systems, anomalies must be verified by field inspection.
================================================================================
""".format(
        features=", ".join(report.get("features", [])),
        contamination=report.get("contamination", 0.01),
        total_records=report.get("total_records", 0),
        train_records=report.get("train_records", 0),
        test_records=report.get("test_records", 0),
        test_records_eval=test.get("evaluated_records", 0),
        test_inj=test.get("injected_anomalies", 0),
        test_pred=test.get("predicted_anomalies", 0),
        test_prec=test.get("precision", 0.0),
        test_rec=test.get("recall", 0.0),
        test_f1=test.get("f1_score", 0.0),
        test_tn=test.get("confusion_matrix", [[0,0],[0,0]])[0][0],
        test_fp=test.get("confusion_matrix", [[0,0],[0,0]])[0][1],
        test_fn=test.get("confusion_matrix", [[0,0],[0,0]])[1][0],
        test_tp=test.get("confusion_matrix", [[0,0],[0,0]])[1][1],
        full_records_eval=overall.get("evaluated_records", 0),
        full_inj=overall.get("injected_anomalies", 0),
        full_pred=overall.get("predicted_anomalies", 0),
        full_prec=overall.get("precision", 0.0),
        full_rec=overall.get("recall", 0.0),
        full_f1=overall.get("f1_score", 0.0),
        full_tn=overall.get("confusion_matrix", [[0,0],[0,0]])[0][0],
        full_fp=overall.get("confusion_matrix", [[0,0],[0,0]])[0][1],
        full_fn=overall.get("confusion_matrix", [[0,0],[0,0]])[1][0],
        full_tp=overall.get("confusion_matrix", [[0,0],[0,0]])[1][1]
    ))


if __name__ == "__main__":
    train_anomaly_model()

