"""
Smart Meter Stream Processor (Layer 3)
Consumes real-time smart meter events from Apache Kafka, computes 30-reading
rolling window averages, updates session peak power, scores anomalies using
the pre-trained Isolation Forest model, and persists records into MySQL.
"""

import sys
import time
import json
import logging
from collections import deque
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd
import joblib
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).resolve().parent))
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_CONSUMER_GROUP,
    KAFKA_AUTO_OFFSET_RESET,
    ROLLING_WINDOW_SIZE,
    MODEL_PATH,
    MODEL_FEATURES,
    DB_TYPE,
    get_db_url
)
from init_db import print_mysql_troubleshooting

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("StreamProcessor")


class StreamProcessor:
    """
    Core Stream Processing Engine:
    - Maintains sliding window for rolling average calculation.
    - Tracks session peak power.
    - Executes Isolation Forest real-time inference.
    - Idempotently writes processed events and KPI metrics to database.
    """

    def __init__(self, window_size: int = ROLLING_WINDOW_SIZE, model_path: Path = MODEL_PATH):
        self.window_size = window_size
        self.rolling_window = deque(maxlen=window_size)
        self.running_peak_power = 0.0
        self.total_processed = 0
        self.total_anomalies = 0

        # Load ML model
        self.model = self._load_model(model_path)

        # Database Engine
        self.db_engine = create_engine(
            get_db_url(include_db=True),
            pool_pre_ping=True,
            pool_recycle=3600
        )
        self._sync_session_state_from_db()

    def _load_model(self, model_path: Path):
        """Loads trained Isolation Forest model from disk."""
        if not model_path.exists():
            logger.warning(
                f"Model file not found at {model_path}. Please run train_model.py. "
                "Stream processor will use threshold heuristic until model is available."
            )
            return None
        logger.info(f"Loading Isolation Forest model from {model_path}...")
        try:
            model = joblib.load(model_path)
            logger.info("ML model successfully loaded!")
            return model
        except Exception as e:
            logger.error(f"Failed to load ML model: {e}")
            return None

    def _sync_session_state_from_db(self):
        """Initializes internal counters from existing database state if restarting."""
        try:
            with self.db_engine.connect() as conn:
                result = conn.execute(text("SELECT total_readings, anomaly_count, peak_power FROM meter_metrics WHERE metric_id = 1;")).fetchone()
                if result:
                    self.total_processed = int(result[0] or 0)
                    self.total_anomalies = int(result[1] or 0)
                    self.running_peak_power = float(result[2] or 0.0)
                    logger.info(
                        f"Synced session state from DB: Processed={self.total_processed:,}, "
                        f"Anomalies={self.total_anomalies:,}, Peak={self.running_peak_power:.3f} kW"
                    )
        except Exception as e:
            logger.warning(f"Could not sync existing session state from DB (will start fresh): {e}")

    def update_rolling_window(self, power: float) -> float:
        """
        Updates fixed-size deque and calculates rolling average.
        Handles early startup (<30 items) by averaging available readings.
        """
        self.rolling_window.append(power)
        return float(sum(self.rolling_window) / len(self.rolling_window))

    def update_peak_power(self, power: float) -> float:
        """Updates and returns session peak power."""
        if power > self.running_peak_power:
            self.running_peak_power = power
        return self.running_peak_power

    def predict_anomaly(self, active_p: float, reactive_p: float, intensity: float):
        """
        Runs ML anomaly inference using exact feature vector:
        [Global_active_power, Global_reactive_power, Global_intensity]
        
        Returns:
            anomaly_flag (int): 0 = NORMAL, 1 = ANOMALY
            anomaly_status (str): 'NORMAL' or 'ANOMALY'
            anomaly_score (float): Decision score from Isolation Forest (lower = more abnormal)
        """
        if self.model is not None:
            features_df = pd.DataFrame([[active_p, reactive_p, intensity]], columns=MODEL_FEATURES)
            raw_pred = self.model.predict(features_df)[0]
            # Isolation Forest convention: +1 = Inlier, -1 = Outlier
            flag = 1 if raw_pred == -1 else 0
            score = float(self.model.decision_function(features_df)[0])
        else:
            # Fallback heuristic if model is not yet trained
            flag = 1 if active_p > 6.0 or reactive_p > 2.0 else 0
            score = -1.0 if flag == 1 else 0.5

        status = "ANOMALY" if flag == 1 else "NORMAL"
        return flag, status, score

    def persist_reading(self, record: dict) -> bool:
        """
        Idempotently inserts processed reading and updates metrics table.
        Prevents duplicate rows on Kafka consumer offset retries.
        """
        is_mysql = DB_TYPE == "mysql"

        # SQL for idempotent reading insertion
        if is_mysql:
            insert_sql = text("""
                INSERT INTO meter_readings (
                    event_id, reading_time, global_active_power, global_reactive_power,
                    global_intensity, sub_metering_1, sub_metering_2, sub_metering_3,
                    rolling_avg_30, anomaly_flag, anomaly_status, anomaly_score, processed_at
                )
                VALUES (
                    :event_id, :reading_time, :global_active_power, :global_reactive_power,
                    :global_intensity, :sub_metering_1, :sub_metering_2, :sub_metering_3,
                    :rolling_avg_30, :anomaly_flag, :anomaly_status, :anomaly_score, :processed_at
                )
                ON DUPLICATE KEY UPDATE event_id = event_id;
            """)
            metrics_sql = text("""
                INSERT INTO meter_metrics (
                    metric_id, current_power, rolling_avg_30, peak_power,
                    total_readings, anomaly_count, latest_event_time, last_updated_at
                )
                VALUES (
                    1, :current_power, :rolling_avg_30, :peak_power,
                    :total_readings, :anomaly_count, :latest_event_time, NOW()
                )
                ON DUPLICATE KEY UPDATE
                    current_power = :current_power,
                    rolling_avg_30 = :rolling_avg_30,
                    peak_power = :peak_power,
                    total_readings = :total_readings,
                    anomaly_count = :anomaly_count,
                    latest_event_time = :latest_event_time,
                    last_updated_at = NOW();
            """)
        else:
            # SQLite syntax
            insert_sql = text("""
                INSERT OR IGNORE INTO meter_readings (
                    event_id, reading_time, global_active_power, global_reactive_power,
                    global_intensity, sub_metering_1, sub_metering_2, sub_metering_3,
                    rolling_avg_30, anomaly_flag, anomaly_status, anomaly_score, processed_at
                )
                VALUES (
                    :event_id, :reading_time, :global_active_power, :global_reactive_power,
                    :global_intensity, :sub_metering_1, :sub_metering_2, :sub_metering_3,
                    :rolling_avg_30, :anomaly_flag, :anomaly_status, :anomaly_score, :processed_at
                );
            """)
            metrics_sql = text("""
                INSERT INTO meter_metrics (
                    metric_id, current_power, rolling_avg_30, peak_power,
                    total_readings, anomaly_count, latest_event_time, last_updated_at
                )
                VALUES (
                    1, :current_power, :rolling_avg_30, :peak_power,
                    :total_readings, :anomaly_count, :latest_event_time, CURRENT_TIMESTAMP
                )
                ON CONFLICT(metric_id) DO UPDATE SET
                    current_power = :current_power,
                    rolling_avg_30 = :rolling_avg_30,
                    peak_power = :peak_power,
                    total_readings = :total_readings,
                    anomaly_count = :anomaly_count,
                    latest_event_time = :latest_event_time,
                    last_updated_at = CURRENT_TIMESTAMP;
            """)

        try:
            with self.db_engine.begin() as conn:
                # 1. Insert meter reading
                conn.execute(insert_sql, record)
                # 2. Update real-time aggregate KPI metrics
                conn.execute(metrics_sql, {
                    "current_power": record["global_active_power"],
                    "rolling_avg_30": record["rolling_avg_30"],
                    "peak_power": self.running_peak_power,
                    "total_readings": self.total_processed,
                    "anomaly_count": self.total_anomalies,
                    "latest_event_time": record["reading_time"]
                })
            return True
        except SQLAlchemyError as se:
            logger.error(f"Database error writing event {record.get('event_id')}: {se}")
            return False

    def process_event(self, raw_payload: dict) -> dict:
        """Validates and processes a single incoming event payload."""
        event_id = raw_payload.get("event_id")
        reading_time_str = raw_payload.get("reading_time")
        active_power = float(raw_payload.get("global_active_power", 0.0))
        reactive_power = float(raw_payload.get("global_reactive_power", 0.0))
        intensity = float(raw_payload.get("global_intensity", 0.0))

        # Enforce non-negative measurements
        active_power = max(0.0, active_power)
        reactive_power = max(0.0, reactive_power)
        intensity = max(0.0, intensity)

        # 1. Rolling Average & Peak
        rolling_avg = self.update_rolling_window(active_power)
        peak = self.update_peak_power(active_power)

        # 2. ML Anomaly Prediction
        flag, status, score = self.predict_anomaly(active_power, reactive_power, intensity)

        # 3. Increment counters
        self.total_processed += 1
        if flag == 1:
            self.total_anomalies += 1

        # 4. Construct enriched record
        processed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        processed_record = {
            "event_id": event_id,
            "reading_time": reading_time_str,
            "global_active_power": active_power,
            "global_reactive_power": reactive_power,
            "global_intensity": intensity,
            "sub_metering_1": float(raw_payload.get("sub_metering_1", 0.0)),
            "sub_metering_2": float(raw_payload.get("sub_metering_2", 0.0)),
            "sub_metering_3": float(raw_payload.get("sub_metering_3", 0.0)),
            "rolling_avg_30": round(rolling_avg, 4),
            "anomaly_flag": flag,
            "anomaly_status": status,
            "anomaly_score": round(score, 5),
            "processed_at": processed_at
        }

        # 5. Persist to Database
        success = self.persist_reading(processed_record)
        if not success:
            logger.warning(f"Failed to persist event {event_id} to database.")

        return processed_record


def run_stream_processor():
    """Main consumer loop reading from Kafka and streaming through processor."""
    logger.info("Initializing Smart Meter Stream Processor...")
    processor = StreamProcessor()

    # Initialize Kafka Consumer
    logger.info(f"Connecting to Kafka broker '{KAFKA_BOOTSTRAP_SERVERS}', topic '{KAFKA_TOPIC}'...")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            group_id=KAFKA_CONSUMER_GROUP,
            auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
            enable_auto_commit=False,  # Manual commit after database persist
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=1000
        )
        logger.info("Connected to Kafka! Waiting for smart-meter events...")
    except NoBrokersAvailable as nba:
        logger.error(f"Kafka broker not available at {KAFKA_BOOTSTRAP_SERVERS}.")
        print_kafka_troubleshooting(KAFKA_BOOTSTRAP_SERVERS, nba)
        return

    print("\n" + "=" * 75)
    print(f" STREAM PROCESSOR ACTIVE -> TOPIC: '{KAFKA_TOPIC}'")
    print(" Listening for incoming power readings... Press Ctrl+C to terminate.")
    print("=" * 75 + "\n")

    try:
        while True:
            # Poll for messages
            messages = consumer.poll(timeout_ms=1000)
            if not messages:
                continue

            for topic_partition, record_list in messages.items():
                for record in record_list:
                    raw_data = record.value
                    processed = processor.process_event(raw_data)

                    # Log processed event
                    flag_tag = " [ANOMALY DETECTED!]" if processed["anomaly_flag"] == 1 else ""
                    logger.info(
                        f"Processed event {processed['event_id']} | Power: {processed['global_active_power']:.3f} kW | "
                        f"RollingAvg: {processed['rolling_avg_30']:.3f} kW | Status: {processed['anomaly_status']}"
                        f"{flag_tag}"
                    )

            # Commit offset only after all records in poll batch are safely persisted
            consumer.commit()

    except KeyboardInterrupt:
        logger.info(f"Stream processor paused by user. Processed {processor.total_processed:,} events.")
    except Exception as e:
        logger.error(f"Unexpected error in stream processor: {e}")
        raise
    finally:
        logger.info("Closing Kafka consumer connection...")
        consumer.close()
        logger.info("Stream processor closed.")


if __name__ == "__main__":
    run_stream_processor()

