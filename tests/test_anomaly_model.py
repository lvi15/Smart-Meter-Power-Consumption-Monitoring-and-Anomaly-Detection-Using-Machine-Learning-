"""
Unit Tests for Machine Learning Anomaly Detection (Layer 4)
Validates Isolation Forest training, persistence, feature consistency,
exclusion of target leakage, prediction mapping, and evaluation reporting.
"""

import sys
import json
from pathlib import Path
import pandas as pd
import numpy as np
import joblib
import pytest
from sklearn.ensemble import IsolationForest

# Add parent directory to path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from train_model import train_anomaly_model
from config.settings import MODEL_FEATURES, GROUND_TRUTH_COLUMN
from generate_dataset import generate_synthetic_dataset


@pytest.fixture(scope="module")
def trained_artifacts(tmp_path_factory):
    """Trains a model on a small dataset and returns model and report paths."""
    temp_dir = tmp_path_factory.mktemp("ml_test")
    data_csv = temp_dir / "smart_meter_test.csv"
    model_pkl = temp_dir / "anomaly_model.pkl"
    report_json = temp_dir / "model_evaluation.json"

    # Generate test dataset (2 days)
    generate_synthetic_dataset(output_path=data_csv, num_days=2, random_seed=42)

    # Train model
    model, report = train_anomaly_model(
        dataset_path=data_csv,
        model_output_path=model_pkl,
        report_output_path=report_json,
        contamination=0.01,
        train_split_ratio=0.8,
        random_state=42
    )
    return model, report, model_pkl, report_json, data_csv


def test_model_artifact_created(trained_artifacts):
    """Verifies that the serialized model artifact exists and loads with Joblib."""
    _, _, model_pkl, _, _ = trained_artifacts
    assert model_pkl.exists(), "Model pickle file was not created!"
    loaded_model = joblib.load(model_pkl)
    assert isinstance(loaded_model, IsolationForest), "Loaded model is not an IsolationForest instance!"


def test_no_data_leakage_in_features(trained_artifacts):
    """Ensures ground truth column is NOT in model training features."""
    _, report, _, _, _ = trained_artifacts
    features_used = report["features"]
    assert GROUND_TRUTH_COLUMN not in features_used, "Target leakage detected! Ground truth found in features!"
    assert features_used == MODEL_FEATURES, f"Feature mismatch: {features_used} != {MODEL_FEATURES}"


def test_model_prediction_output(trained_artifacts):
    """Tests model inference on synthetic normal vs extreme outlier data points."""
    model, _, _, _, _ = trained_artifacts

    # Normal typical daytime sample
    normal_sample = pd.DataFrame([[1.2, 0.15, 5.5]], columns=MODEL_FEATURES)
    raw_pred_norm = model.predict(normal_sample)[0]
    score_norm = model.decision_function(normal_sample)[0]

    # Extreme anomalous surge sample (12 kW active power)
    anomaly_sample = pd.DataFrame([[12.5, 2.5, 55.0]], columns=MODEL_FEATURES)
    raw_pred_anom = model.predict(anomaly_sample)[0]
    score_anom = model.decision_function(anomaly_sample)[0]

    # Inlier should yield +1, outlier should yield -1
    assert raw_pred_norm == 1, "Normal sample was incorrectly predicted as outlier!"
    assert raw_pred_anom == -1, "Extreme surge was not detected as an outlier!"
    # Anomaly score for outlier should be significantly lower (more negative)
    assert score_anom < score_norm, "Outlier score should be lower than inlier score!"


def test_evaluation_report_format(trained_artifacts):
    """Validates that evaluation report contains required metrics."""
    _, _, _, report_json, _ = trained_artifacts
    assert report_json.exists(), "Evaluation report JSON missing!"

    with open(report_json, "r") as f:
        report = json.load(f)

    assert "model_type" in report
    assert "test_evaluation" in report
    assert "overall_evaluation" in report

    test_metrics = report["test_evaluation"]
    assert "precision" in test_metrics
    assert "recall" in test_metrics
    assert "f1_score" in test_metrics
    assert "confusion_matrix" in test_metrics
    assert test_metrics["precision"] >= 0.0
    assert test_metrics["recall"] >= 0.0

