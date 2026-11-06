"""
Unit Tests for Database Layer and Idempotency (Layer 5)
Validates schema initialization, idempotent record insertion, and metric aggregations.
Runs against an isolated test SQLite engine to guarantee test repeatability.
"""

import sys
from pathlib import Path
from datetime import datetime
import pytest
from sqlalchemy import create_engine, text

# Add parent directory to path
sys.path.append(str(Path(__file__).resolve().parent.parent))


@pytest.fixture
def test_db(tmp_path):
    """Creates a temporary SQLite database with project schema for testing."""
    db_file = tmp_path / "test_smart_meter.db"
    engine = create_engine(f"sqlite:///{db_file.as_posix()}", isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE meter_readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE NOT NULL,
                reading_time TIMESTAMP NOT NULL,
                global_active_power REAL NOT NULL,
                global_reactive_power REAL NOT NULL,
                global_intensity REAL NOT NULL,
                sub_metering_1 REAL NOT NULL DEFAULT 0.0,
                sub_metering_2 REAL NOT NULL DEFAULT 0.0,
                sub_metering_3 REAL NOT NULL DEFAULT 0.0,
                rolling_avg_30 REAL NOT NULL,
                anomaly_flag INTEGER NOT NULL DEFAULT 0,
                anomaly_status TEXT NOT NULL DEFAULT 'NORMAL',
                anomaly_score REAL,
                processed_at TIMESTAMP NOT NULL
            );
        """))
        conn.execute(text("""
            CREATE TABLE meter_metrics (
                metric_id INTEGER PRIMARY KEY,
                current_power REAL NOT NULL DEFAULT 0.0,
                rolling_avg_30 REAL NOT NULL DEFAULT 0.0,
                peak_power REAL NOT NULL DEFAULT 0.0,
                total_readings INTEGER NOT NULL DEFAULT 0,
                anomaly_count INTEGER NOT NULL DEFAULT 0,
                latest_event_time TIMESTAMP,
                last_updated_at TIMESTAMP NOT NULL
            );
        """))
        conn.execute(text("""
            INSERT INTO meter_metrics (
                metric_id, current_power, rolling_avg_30, peak_power,
                total_readings, anomaly_count, latest_event_time, last_updated_at
            )
            VALUES (1, 0.0, 0.0, 0.0, 0, 0, NULL, CURRENT_TIMESTAMP);
        """))
    return engine


def test_idempotent_insert_prevents_duplicates(test_db):
    """
    Validates that attempting to insert the exact same event_id multiple times
    is safely handled without throwing duplicate key errors or creating duplicate rows.
    """
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

    record = {
        "event_id": "evt_test_000001",
        "reading_time": "2024-01-01 00:01:00",
        "global_active_power": 2.45,
        "global_reactive_power": 0.18,
        "global_intensity": 10.6,
        "sub_metering_1": 0.0,
        "sub_metering_2": 0.0,
        "sub_metering_3": 5.0,
        "rolling_avg_30": 2.45,
        "anomaly_flag": 0,
        "anomaly_status": "NORMAL",
        "anomaly_score": 0.12,
        "processed_at": "2024-01-01 00:01:01"
    }

    with test_db.connect() as conn:
        # First insertion
        conn.execute(insert_sql, record)
        # Duplicate insertion of the same record (e.g. Kafka retry)
        conn.execute(insert_sql, record)

        # Verify only 1 row exists
        count = conn.execute(text("SELECT COUNT(*) FROM meter_readings WHERE event_id = 'evt_test_000001';")).scalar()
        assert count == 1, f"Expected 1 unique record, found {count} (Idempotency failed!)"


def test_metrics_table_single_row_update(test_db):
    """Validates that meter_metrics updates the single-row KPI summary correctly."""
    update_sql = text("""
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

    params = {
        "current_power": 3.75,
        "rolling_avg_30": 2.80,
        "peak_power": 4.50,
        "total_readings": 150,
        "anomaly_count": 3,
        "latest_event_time": "2024-01-01 02:30:00"
    }

    with test_db.connect() as conn:
        conn.execute(update_sql, params)

        # Check total rows in meter_metrics remains exactly 1
        total_rows = conn.execute(text("SELECT COUNT(*) FROM meter_metrics;")).scalar()
        assert total_rows == 1, f"Expected exactly 1 row in meter_metrics, got {total_rows}"

        # Check updated values
        row = conn.execute(text("SELECT current_power, peak_power, total_readings FROM meter_metrics WHERE metric_id = 1;")).fetchone()
        assert row[0] == pytest.approx(3.75)
        assert row[1] == pytest.approx(4.50)
        assert row[2] == 150

