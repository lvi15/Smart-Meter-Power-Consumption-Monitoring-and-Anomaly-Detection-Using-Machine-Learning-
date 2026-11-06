"""
Smart Meter Kafka Producer (Layer 1)
Reads chronological smart-meter readings from CSV, formats them as JSON events,
and publishes them to the Apache Kafka topic at a configurable streaming frequency.
"""

import sys
import time
import json
import argparse
import logging
from pathlib import Path
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).resolve().parent))
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PRODUCER_DELAY_SECONDS,
    DATASET_PATH,
    GROUND_TRUTH_COLUMN
)
from generate_dataset import generate_synthetic_dataset

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("SmartMeterProducer")


def create_kafka_producer(bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS, retries: int = 3) -> KafkaProducer:
    """
    Initializes and returns a KafkaProducer instance configured for JSON serialization.
    """
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Connecting to Kafka broker at '{bootstrap_servers}' (Attempt {attempt}/{retries})...")
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                request_timeout_ms=10000
            )
            logger.info("Successfully connected to Kafka broker!")
            return producer
        except NoBrokersAvailable as nba:
            logger.warning(f"Kafka broker not available at {bootstrap_servers}: {nba}")
            if attempt < retries:
                time.sleep(2)
            else:
                print_kafka_troubleshooting(bootstrap_servers, nba)
                raise


def stream_smart_meter_events(
    csv_path: Path = DATASET_PATH,
    topic: str = KAFKA_TOPIC,
    delay_seconds: float = PRODUCER_DELAY_SECONDS,
    replay: bool = False,
    limit: int = None
):
    """
    Streams smart meter records from CSV to Kafka.
    
    Guarantees:
    - Never publishes synthetic ground-truth anomaly labels (prevents ML data leakage).
    - Uses deterministic, unique event IDs for idempotent consumption.
    - Supports configurable stream pacing.
    """
    if not csv_path.exists():
        logger.warning(f"Dataset not found at {csv_path}. Generating synthetic dataset automatically...")
        generate_synthetic_dataset(output_path=csv_path)

    logger.info(f"Loading readings from {csv_path}...")
    df = pd.read_csv(csv_path)
    total_rows = len(df)
    logger.info(f"Loaded {total_rows:,} records from dataset.")

    # Initialize Kafka producer
    producer = create_kafka_producer()

    published_count = 0
    start_time = time.time()
    logger.info(f"Starting event stream to topic '{topic}' (Delay: {delay_seconds:.2f}s per event)...")
    print("\n" + "=" * 75)
    print(f" STREAMING ACTIVE -> TOPIC: '{topic}' | Press Ctrl+C to safely pause/stop")
    print("=" * 75)

    try:
        while True:
            for index, row in df.iterrows():
                if limit and published_count >= limit:
                    logger.info(f"Reached configured limit of {limit} messages.")
                    return

                # Construct clean event payload
                # EXCLUDE ground-truth column to prevent any target leakage to processor
                event_id = f"evt_{index + 1:07d}"
                payload = {
                    "event_id": event_id,
                    "reading_time": str(row["Date_Time"]),
                    "global_active_power": float(row["Global_active_power"]),
                    "global_reactive_power": float(row["Global_reactive_power"]),
                    "global_intensity": float(row["Global_intensity"]),
                    "sub_metering_1": float(row.get("Sub_metering_1", 0.0)),
                    "sub_metering_2": float(row.get("Sub_metering_2", 0.0)),
                    "sub_metering_3": float(row.get("Sub_metering_3", 0.0))
                }

                # Publish message to Kafka
                future = producer.send(topic, key=event_id, value=payload)
                # Ensure delivery
                future.get(timeout=10)

                published_count += 1
                power_kw = payload["global_active_power"]

                if published_count % 10 == 0 or published_count <= 5:
                    elapsed = time.time() - start_time
                    rate = published_count / max(1.0, elapsed)
                    logger.info(
                        f"[Published #{published_count:,}] ID: {event_id} | Time: {payload['reading_time']} | "
                        f"Active Power: {power_kw:.3f} kW | Rate: {rate:.1f} msg/sec"
                    )

                if delay_seconds > 0:
                    time.sleep(delay_seconds)

            if not replay:
                logger.info("Reached end of dataset. Streaming complete.")
                break
            else:
                logger.info("Replay mode enabled: restarting stream from beginning of dataset...")

    except KeyboardInterrupt:
        logger.info(f"Stream paused by user after publishing {published_count:,} messages.")
    except Exception as e:
        logger.error(f"Error during Kafka streaming: {e}")
        raise
    finally:
        logger.info("Flushing remaining Kafka messages and closing producer connection...")
        producer.flush(timeout=5)
        producer.close()
        logger.info(f"Producer shutdown cleanly. Total published: {published_count:,} events.")


def print_kafka_troubleshooting(broker: str, error: Exception):
    """Helpful troubleshooting output for Kafka connection errors."""
    msg = f"""
================================================================================
APACHE KAFKA CONNECTION TROUBLESHOOTING GUIDE
================================================================================
Could not establish a connection to Kafka broker:
  Broker address: {broker}
  Error detail  : {error}

To resolve this on your machine:
1. Ensure Apache Kafka is running locally:
   - With Docker Compose (Recommended):
       docker compose up -d kafka
   - Or native Windows/WSL Kafka command:
       .\\bin\\windows\\kafka-server-start.bat .\\config\\kraft\\server.properties

2. Verify that Kafka is listening on port 9092:
   - Run in PowerShell:
       Test-NetConnection -ComputerName localhost -Port 9092

3. If running Kafka on another hostname/port, specify it in '.env':
   KAFKA_BOOTSTRAP_SERVERS=your_host:9092
================================================================================
"""
    print(msg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smart Meter Kafka Event Producer")
    parser.add_argument("--delay", "-d", type=float, default=PRODUCER_DELAY_SECONDS,
                        help=f"Delay between messages in seconds (default: {PRODUCER_DELAY_SECONDS})")
    parser.add_argument("--replay", action="store_true",
                        help="Continuously replay the dataset in a loop")
    parser.add_argument("--limit", "-l", type=int, default=None,
                        help="Maximum number of events to publish (default: all)")
    parser.add_argument("--direct", action="store_true",
                        help="Direct pipeline mode: stream events directly through StreamProcessor into MySQL if Kafka is offline")
    args = parser.parse_args()

    if args.direct:
        from stream_processor import StreamProcessor
        logger.info("Running in DIRECT STREAMING MODE (Streaming directly into StreamProcessor & MySQL)...")
        processor = StreamProcessor()
        df = pd.read_csv(DATASET_PATH)
        published_count = 0
        try:
            while True:
                for index, row in df.iterrows():
                    if args.limit and published_count >= args.limit:
                        break
                    event_id = f"evt_{index + 1:07d}"
                    payload = {
                        "event_id": event_id,
                        "reading_time": str(row["Date_Time"]),
                        "global_active_power": float(row["Global_active_power"]),
                        "global_reactive_power": float(row["Global_reactive_power"]),
                        "global_intensity": float(row["Global_intensity"]),
                        "sub_metering_1": float(row.get("Sub_metering_1", 0.0)),
                        "sub_metering_2": float(row.get("Sub_metering_2", 0.0)),
                        "sub_metering_3": float(row.get("Sub_metering_3", 0.0))
                    }
                    processed = processor.process_event(payload)
                    published_count += 1
                    flag_tag = " [ANOMALY DETECTED!]" if processed["anomaly_flag"] == 1 else ""
                    logger.info(
                        f"[Direct Stream #{published_count:,}] ID: {event_id} | Time: {payload['reading_time']} | "
                        f"Power: {payload['global_active_power']:.3f} kW | Status: {processed['anomaly_status']}{flag_tag}"
                    )
                    if args.delay > 0:
                        time.sleep(args.delay)
                if not args.replay:
                    break
        except KeyboardInterrupt:
            logger.info(f"Direct stream paused after {published_count:,} events.")
    else:
        stream_smart_meter_events(
            delay_seconds=args.delay,
            replay=args.replay,
            limit=args.limit
        )

