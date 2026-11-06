"""
Unit Tests for Smart Meter Dataset Generation (Layer 1)
Validates dataset dimensions, column schemas, timestamp formatting,
non-negative power values, and reproducible anomaly injection.
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import pytest

# Add parent directory to path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from generate_dataset import generate_synthetic_dataset
from config.settings import GROUND_TRUTH_COLUMN


@pytest.fixture(scope="module")
def sample_dataset(tmp_path_factory):
    """Generates a small test dataset (2 days) for fast testing."""
    test_csv = tmp_path_factory.mktemp("data") / "test_readings.csv"
    df = generate_synthetic_dataset(output_path=test_csv, num_days=2, sampling_minutes=1, random_seed=42)
    return df, test_csv


def test_dataset_record_count(sample_dataset):
    """Verifies that generated records match expectations."""
    df, _ = sample_dataset
    expected_rows = 2 * 24 * 60  # 2,880 rows
    assert len(df) == expected_rows, f"Expected {expected_rows} rows, got {len(df)}"


def test_required_columns_present(sample_dataset):
    """Verifies all required column names are present in output dataset."""
    df, _ = sample_dataset
    required_cols = [
        "Date_Time",
        "Global_active_power",
        "Global_reactive_power",
        "Global_intensity",
        "Sub_metering_1",
        "Sub_metering_2",
        "Sub_metering_3",
        GROUND_TRUTH_COLUMN
    ]
    for col in required_cols:
        assert col in df.columns, f"Required column '{col}' is missing from dataset!"


def test_timestamps_chronological_and_valid(sample_dataset):
    """Checks that timestamps are strictly chronological and parseable."""
    df, _ = sample_dataset
    ts_series = pd.to_datetime(df["Date_Time"], format="%Y-%m-%d %H:%M:%S")
    assert ts_series.is_monotonic_increasing, "Timestamps are not strictly chronological!"
    assert ts_series.isnull().sum() == 0, "Found null timestamps in dataset!"


def test_power_measurements_non_negative(sample_dataset):
    """Ensures physical power measurements are non-negative."""
    df, _ = sample_dataset
    assert (df["Global_active_power"] >= 0).all(), "Negative active power detected!"
    assert (df["Global_reactive_power"] >= 0).all(), "Negative reactive power detected!"
    assert (df["Global_intensity"] >= 0).all(), "Negative current intensity detected!"
    assert (df["Sub_metering_1"] >= 0).all(), "Negative Sub_metering_1 detected!"
    assert (df["Sub_metering_2"] >= 0).all(), "Negative Sub_metering_2 detected!"
    assert (df["Sub_metering_3"] >= 0).all(), "Negative Sub_metering_3 detected!"


def test_injected_anomalies_present(sample_dataset):
    """Ensures ground truth anomaly labels exist and are reasonable."""
    df, _ = sample_dataset
    anomaly_count = int(df[GROUND_TRUTH_COLUMN].sum())
    total_records = len(df)
    anomaly_rate = anomaly_count / total_records

    assert anomaly_count > 0, "No synthetic anomalies were injected!"
    assert 0.005 <= anomaly_rate <= 0.03, f"Unexpected anomaly rate: {anomaly_rate * 100:.2f}%"


def test_reproducibility(tmp_path):
    """Verifies that using the same random seed produces identical datasets."""
    p1 = tmp_path / "seed1.csv"
    p2 = tmp_path / "seed2.csv"
    df1 = generate_synthetic_dataset(output_path=p1, num_days=1, random_seed=99)
    df2 = generate_synthetic_dataset(output_path=p2, num_days=1, random_seed=99)
    pd.testing.assert_frame_equal(df1, df2)

