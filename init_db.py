"""
Database Initialization Script
Sets up the MySQL schema, creating tables and indexes for streaming storage.
Provides clear diagnostics, configuration instructions, and idempotent setup.
"""

import sys
import logging
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).resolve().parent))
from config.settings import (
    DB_TYPE,
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_DATABASE,
    MYSQL_USER,
    MYSQL_PASSWORD,
    SQLITE_DB_PATH,
    get_db_url
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("DatabaseInitializer")


def init_database(force_recreate: bool = False) -> bool:
    """
    Initializes the database schema for the smart meter streaming pipeline.
    Primary database: MySQL.
    Creates:
      1. Database `smart_meter_db` (if MySQL)
      2. Table `meter_readings` with unique `event_id` constraint for idempotency
      3. Table `meter_metrics` initialized with default single row
    """
    logger.info(f"Connecting to database backend: {DB_TYPE.upper()}...")

    if DB_TYPE == "mysql":
        return _init_mysql(force_recreate)
    elif DB_TYPE == "sqlite":
        return _init_sqlite(force_recreate)
    else:
        logger.error(f"Unsupported DB_TYPE '{DB_TYPE}'. Supported values are 'mysql' or 'sqlite'.")
        return False


def _init_mysql(force_recreate: bool = False) -> bool:
    """Initializes MySQL schema using PyMySQL/SQLAlchemy."""
    server_url = get_db_url(include_db=False)
    db_url = get_db_url(include_db=True)

    try:
        # Step 1: Connect to MySQL server and ensure database exists
        logger.info(f"Verifying MySQL server at {MYSQL_HOST}:{MYSQL_PORT} (User: {MYSQL_USER})...")
        server_engine = create_engine(server_url, isolation_level="AUTOCOMMIT")
        with server_engine.connect() as conn:
            if force_recreate:
                logger.warning(f"Dropping existing database '{MYSQL_DATABASE}'...")
                conn.execute(text(f"DROP DATABASE IF EXISTS `{MYSQL_DATABASE}`;"))
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
            logger.info(f"Database '{MYSQL_DATABASE}' verified/created.")
        server_engine.dispose()

        # Step 2: Connect to target database and create tables
        db_engine = create_engine(db_url, isolation_level="AUTOCOMMIT")
        with db_engine.connect() as conn:
            # Create meter_readings table
            logger.info("Creating table 'meter_readings' if not exists...")
            conn.execute(text("""
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
            """))

            # Create meter_metrics table
            logger.info("Creating table 'meter_metrics' if not exists...")
            conn.execute(text("""
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
            """))

            # Initialize default metric row
            conn.execute(text("""
                INSERT INTO meter_metrics (
                    metric_id, current_power, rolling_avg_30, peak_power, 
                    total_readings, anomaly_count, latest_event_time, last_updated_at
                )
                VALUES (1, 0.0, 0.0, 0.0, 0, 0, NULL, NOW())
                ON DUPLICATE KEY UPDATE metric_id = 1;
            """))

            logger.info("Schema initialization successfully completed on MySQL!")
            print("\n>>> MySQL Database and Tables initialized successfully! <<<")
        db_engine.dispose()
        return True

    except OperationalError as oe:
        logger.error(f"Failed to connect to MySQL at {MYSQL_HOST}:{MYSQL_PORT}.")
        print_mysql_troubleshooting(oe)
        return False
    except SQLAlchemyError as se:
        logger.error(f"SQL error during database initialization: {se}")
        return False


def _init_sqlite(force_recreate: bool = False) -> bool:
    """Initializes SQLite schema for standalone local testing / validation."""
    SQLITE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if force_recreate and SQLITE_DB_PATH.exists():
        SQLITE_DB_PATH.unlink()

    engine = create_engine(get_db_url(db_type="sqlite"), isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS meter_readings (
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
            CREATE TABLE IF NOT EXISTS meter_metrics (
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
            INSERT OR IGNORE INTO meter_metrics (
                metric_id, current_power, rolling_avg_30, peak_power, 
                total_readings, anomaly_count, latest_event_time, last_updated_at
            )
            VALUES (1, 0.0, 0.0, 0.0, 0, 0, NULL, CURRENT_TIMESTAMP);
        """))
        logger.info(f"SQLite fallback database initialized at: {SQLITE_DB_PATH}")
        print(f"\n>>> SQLite Database initialized at: {SQLITE_DB_PATH} <<<")
    engine.dispose()
    return True


def print_mysql_troubleshooting(error: Exception):
    """Prints clear, beginner-friendly instructions to resolve MySQL connection issues."""
    msg = f"""
================================================================================
MYSQL CONNECTION TROUBLESHOOTING GUIDE
================================================================================
Encountered connection error:
  {error}

To resolve this on your machine:
1. Verify that the MySQL service is installed and currently running:
   - On Windows: Open Services (services.msc) -> Find 'MySQL80' -> Click 'Start'
   - Or in PowerShell as Administrator:
       Start-Service MySQL80
   - Or start MySQL via Docker Compose:
       docker compose up -d mysql

2. Check your credentials in '.env':
   - Current MYSQL_HOST : {MYSQL_HOST}
   - Current MYSQL_PORT : {MYSQL_PORT}
   - Current MYSQL_USER : {MYSQL_USER}
   - Current MYSQL_PASSWORD : {'*' * len(MYSQL_PASSWORD) if MYSQL_PASSWORD else '(empty)'}

3. If your MySQL root password is different, create a '.env' file in this folder:
   MYSQL_PASSWORD=your_actual_password

4. To test application logic offline while MySQL is being started, set:
   DB_TYPE=sqlite
   in your .env file, then run 'python init_db.py'.
================================================================================
"""
    print(msg)


if __name__ == "__main__":
    init_database()
