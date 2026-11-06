-- ==============================================================================
-- Schema for Smart Meter Real-Time Streaming Analytics & Anomaly Detection
-- Database: smart_meter_db
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS smart_meter_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE smart_meter_db;

-- ------------------------------------------------------------------------------
-- Table 1: meter_readings
-- Stores individual streaming observations enriched with ML anomaly predictions
-- and sliding window rolling statistics.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meter_readings (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL,
    reading_time DATETIME NOT NULL,
    global_active_power DOUBLE NOT NULL,
    global_reactive_power DOUBLE NOT NULL,
    global_intensity DOUBLE NOT NULL,
    sub_metering_1 DOUBLE NOT NULL DEFAULT 0.0,
    sub_metering_2 DOUBLE NOT NULL DEFAULT 0.0,
    sub_metering_3 DOUBLE NOT NULL DEFAULT 0.0,
    rolling_avg_30 DOUBLE NOT NULL,
    anomaly_flag INT NOT NULL DEFAULT 0,
    anomaly_status VARCHAR(16) NOT NULL DEFAULT 'NORMAL',
    anomaly_score DOUBLE NULL,
    processed_at DATETIME NOT NULL,
    CONSTRAINT uq_meter_readings_event_id UNIQUE (event_id),
    INDEX idx_reading_time (reading_time),
    INDEX idx_anomaly_flag (anomaly_flag),
    INDEX idx_processed_at (processed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ------------------------------------------------------------------------------
-- Table 2: meter_metrics
-- Maintains real-time single-row aggregated KPI counters for ultra-fast queries
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meter_metrics (
    metric_id INT PRIMARY KEY,
    current_power DOUBLE NOT NULL DEFAULT 0.0,
    rolling_avg_30 DOUBLE NOT NULL DEFAULT 0.0,
    peak_power DOUBLE NOT NULL DEFAULT 0.0,
    total_readings INT NOT NULL DEFAULT 0,
    anomaly_count INT NOT NULL DEFAULT 0,
    latest_event_time DATETIME NULL,
    last_updated_at DATETIME NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Initialize default single metric row if it does not exist
INSERT INTO meter_metrics (
    metric_id, current_power, rolling_avg_30, peak_power, 
    total_readings, anomaly_count, latest_event_time, last_updated_at
)
VALUES (1, 0.0, 0.0, 0.0, 0, 0, NULL, NOW())
ON DUPLICATE KEY UPDATE metric_id = 1;

