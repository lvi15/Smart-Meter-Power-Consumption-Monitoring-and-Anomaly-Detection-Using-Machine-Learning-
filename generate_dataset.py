"""
Synthetic Smart Meter Dataset Generator
Generates realistic electricity consumption data with daily/hourly patterns,
appliance sub-metering, and injected anomalies for machine learning validation.
"""

import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import pandas as pd

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).resolve().parent))
from config.settings import DATASET_PATH, GROUND_TRUTH_COLUMN

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("DatasetGenerator")


def generate_synthetic_dataset(
    output_path: Path = DATASET_PATH,
    num_days: int = 7,
    sampling_minutes: int = 1,
    random_seed: int = 42
) -> pd.DataFrame:
    """
    Generates a realistic smart-meter electricity dataset with 1-minute intervals.
    
    Includes realistic diurnal cycles:
    - Overnight baseline (00:00 - 06:00): Standby appliances (~0.2 - 0.5 kW)
    - Morning peak (07:00 - 09:30): Breakfast, kettle, water heating (~1.5 - 3.8 kW)
    - Midday lull (10:00 - 17:00): Moderate consumption (~0.6 - 1.4 kW)
    - Evening peak (18:00 - 22:30): Dinner cooking, entertainment, lighting (~2.0 - 4.5 kW)
    - Night cooldown (22:30 - 23:59): Transition to baseline (~0.4 - 0.8 kW)
    
    Injected anomalies (~1% of total records) are tagged in `synthetic_anomaly_ground_truth`
    for offline evaluation. This ground-truth column MUST NOT be used for model training.
    """
    np.random.seed(random_seed)
    total_records = num_days * 24 * 60 // sampling_minutes
    logger.info(f"Generating {total_records} synthetic smart-meter readings ({num_days} days @ {sampling_minutes} min interval)...")

    # Fixed chronological baseline timestamps
    start_time = datetime(2024, 1, 1, 0, 0, 0)
    timestamps = [start_time + timedelta(minutes=i * sampling_minutes) for i in range(total_records)]

    active_power = np.zeros(total_records, dtype=float)
    reactive_power = np.zeros(total_records, dtype=float)
    intensity = np.zeros(total_records, dtype=float)
    sub_1 = np.zeros(total_records, dtype=float)  # Kitchen
    sub_2 = np.zeros(total_records, dtype=float)  # Laundry
    sub_3 = np.zeros(total_records, dtype=float)  # Climate / Water Heater
    ground_truth = np.zeros(total_records, dtype=int)

    # 1. Base consumption patterns
    for i, ts in enumerate(timestamps):
        hour = ts.hour + ts.minute / 60.0
        is_weekend = ts.weekday() >= 5
        
        # Diurnal pattern calculation
        if 0 <= hour < 6:
            # Low-use night base
            base = 0.25 + 0.10 * np.sin(np.pi * hour / 6.0)
            p_sub1, p_sub2, p_sub3 = 0.0, 0.0, 0.5
        elif 6 <= hour < 9.5:
            # Morning peak
            t_norm = (hour - 6) / 3.5
            base = 1.6 + 1.8 * np.sin(np.pi * t_norm)
            p_sub1 = 12.0 * np.sin(np.pi * t_norm) if np.random.rand() > 0.4 else 0.0
            p_sub2 = 18.0 if is_weekend and np.random.rand() > 0.6 else 0.0
            p_sub3 = 14.0 * np.sin(np.pi * t_norm)
        elif 9.5 <= hour < 17.5:
            # Daytime activity
            weekend_boost = 0.7 if is_weekend else 0.0
            base = 0.7 + weekend_boost + 0.3 * np.sin(np.pi * (hour - 9.5) / 8.0)
            p_sub1 = 4.0 if np.random.rand() > 0.7 else 0.0
            p_sub2 = 15.0 if is_weekend and np.random.rand() > 0.5 else 0.0
            p_sub3 = 8.0 + 4.0 * np.sin(np.pi * (hour - 9.5) / 8.0)
        elif 17.5 <= hour < 22.5:
            # Evening high-use peak
            t_norm = (hour - 17.5) / 5.0
            base = 2.2 + 2.0 * np.sin(np.pi * t_norm)
            p_sub1 = 15.0 * np.sin(np.pi * t_norm) if np.random.rand() > 0.3 else 0.0
            p_sub2 = 10.0 if np.random.rand() > 0.7 else 0.0
            p_sub3 = 16.0 * np.sin(np.pi * t_norm)
        else:
            # Late night wind-down
            base = 0.6 - 0.3 * ((hour - 22.5) / 1.5)
            p_sub1, p_sub2, p_sub3 = 0.0, 0.0, 2.0

        # Small stochastic Gaussian fluctuations
        noise = np.random.normal(0.0, 0.08)
        act_p = max(0.05, base + noise)

        # Reactive power (~5% to 15% of active power with light noise)
        react_p = max(0.02, act_p * (0.08 + 0.04 * np.random.rand()) + np.random.normal(0, 0.01))

        # Intensity (Current in Amperes at ~230V nominal: I = P * 1000 / (230 * cos_phi))
        cos_phi = 0.92 + 0.05 * np.random.rand()
        current_intensity = max(0.2, (act_p * 1000.0) / (230.0 * cos_phi) + np.random.normal(0, 0.1))

        active_power[i] = round(act_p, 4)
        reactive_power[i] = round(react_p, 4)
        intensity[i] = round(current_intensity, 3)
        sub_1[i] = round(max(0.0, p_sub1 + np.random.normal(0, 0.5)), 2)
        sub_2[i] = round(max(0.0, p_sub2 + np.random.normal(0, 0.5)), 2)
        sub_3[i] = round(max(0.0, p_sub3 + np.random.normal(0, 0.5)), 2)

    # 2. Inject Controlled Anomalies (~1.0% = ~100 distinct anomalous observations)
    # Target approximately 105 anomaly records across different anomaly categories
    num_anomalies_target = int(total_records * 0.0105)
    anomaly_indices = set()
    while len(anomaly_indices) < num_anomalies_target:
        idx = np.random.randint(100, total_records - 50)
        anomaly_indices.add(idx)

    for idx in sorted(anomaly_indices):
        ground_truth[idx] = 1
        anomaly_type = np.random.choice(["extreme_surge", "night_anomaly", "reactive_fault", "intensity_mismatch"])
        
        if anomaly_type == "extreme_surge":
            # Extreme sudden spike (7.5 - 11.5 kW), well above normal max of ~4.5 kW
            active_power[idx] = round(np.random.uniform(7.8, 11.5), 4)
            reactive_power[idx] = round(active_power[idx] * np.random.uniform(0.12, 0.20), 4)
            intensity[idx] = round((active_power[idx] * 1000) / (230 * 0.95), 3)
            sub_1[idx] = round(np.random.uniform(25.0, 40.0), 2)
        elif anomaly_type == "night_anomaly":
            # Sustained high consumption (4.5 - 6.5 kW) at normally quiet hours (01:00 - 05:00)
            active_power[idx] = round(np.random.uniform(4.5, 6.8), 4)
            reactive_power[idx] = round(np.random.uniform(0.4, 0.9), 4)
            intensity[idx] = round((active_power[idx] * 1000) / (230 * 0.9), 3)
            sub_3[idx] = round(np.random.uniform(20.0, 35.0), 2)
        elif anomaly_type == "reactive_fault":
            # Severe power factor defect: High reactive power (> 2.5 kW) with low active power
            active_power[idx] = round(np.random.uniform(0.3, 0.8), 4)
            reactive_power[idx] = round(np.random.uniform(2.6, 4.2), 4)
            intensity[idx] = round(np.random.uniform(12.0, 19.0), 3)
        elif anomaly_type == "intensity_mismatch":
            # High current surge / harmonic distortion without proportional registered active wattage
            active_power[idx] = round(np.random.uniform(1.0, 1.8), 4)
            reactive_power[idx] = round(np.random.uniform(0.1, 0.3), 4)
            intensity[idx] = round(np.random.uniform(26.0, 38.0), 3)

    # 3. Assemble DataFrame
    df = pd.DataFrame({
        "Date_Time": [ts.strftime("%Y-%m-%d %H:%M:%S") for ts in timestamps],
        "Global_active_power": active_power,
        "Global_reactive_power": reactive_power,
        "Global_intensity": intensity,
        "Sub_metering_1": sub_1,
        "Sub_metering_2": sub_2,
        "Sub_metering_3": sub_3,
        GROUND_TRUTH_COLUMN: ground_truth
    })

    # Ensure output directory exists and save CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info(f"Dataset successfully created and saved to: {output_path}")

    # 4. Display dataset summary metrics
    print_dataset_summary(df, output_path)
    return df


def print_dataset_summary(df: pd.DataFrame, output_path: Path):
    """Prints a clear summary of dataset characteristics."""
    total_records = len(df)
    start_time = df["Date_Time"].iloc[0]
    end_time = df["Date_Time"].iloc[-1]
    injected_anomalies = int(df[GROUND_TRUTH_COLUMN].sum())
    min_power = df["Global_active_power"].min()
    max_power = df["Global_active_power"].max()
    avg_power = df["Global_active_power"].mean()
    valid_records = df.dropna().shape[0]

    summary_text = f"""
================================================================================
SMART METER DATASET GENERATION SUMMARY
================================================================================
Output File Path         : {output_path}
Total Records Generated  : {total_records:,}
Start Timestamp          : {start_time}
End Timestamp            : {end_time}
Total Injected Anomalies : {injected_anomalies:,} ({injected_anomalies / total_records * 100:.2f}%)
Minimum Active Power     : {min_power:.4f} kW
Maximum Active Power     : {max_power:.4f} kW
Average Active Power     : {avg_power:.4f} kW
Valid Non-Null Records   : {valid_records:,}
Evaluation Column        : {GROUND_TRUTH_COLUMN} (Excluded from ML Training)
================================================================================
"""
    print(summary_text)


if __name__ == "__main__":
    generate_synthetic_dataset()

