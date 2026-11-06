"""
Centralized Configuration Settings
Loads configuration from environment variables (.env file supported)
with production-grade defaults for local development.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Base Directory of the Project
BASE_DIR = Path(__file__).resolve().parent.parent

# Load .env if present
ENV_FILE = BASE_DIR / ".env"
if ENV_FILE.exists():
    load_dotenv(ENV_FILE)
else:
    load_dotenv()

# --- Kafka Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "smartmeter-events")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "smartmeter-processor-group")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")

# --- Database Configuration ---
DB_TYPE = os.getenv("DB_TYPE", "mysql").lower()  # 'mysql' or 'sqlite'
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "smart_meter_db")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "rootpassword")
SQLITE_DB_PATH = BASE_DIR / os.getenv("SQLITE_DB_PATH", "data/smart_meter_dev.db")

def get_db_url(include_db: bool = True, db_type: str = None) -> str:
    """
    Constructs a SQLAlchemy database URL based on the current configuration.
    If include_db is False, connects to the server without selecting a specific database.
    """
    target_type = (db_type or DB_TYPE).lower()
    if target_type == "sqlite":
        SQLITE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        return f"sqlite:///{SQLITE_DB_PATH.as_posix()}"
    
    # Primary MySQL connection string using PyMySQL driver
    user_pass = f"{MYSQL_USER}:{MYSQL_PASSWORD}" if MYSQL_PASSWORD else MYSQL_USER
    base_url = f"mysql+pymysql://{user_pass}@{MYSQL_HOST}:{MYSQL_PORT}"
    if include_db:
        return f"{base_url}/{MYSQL_DATABASE}?charset=utf8mb4"
    return base_url

# --- Producer Configuration ---
PRODUCER_DELAY_SECONDS = float(os.getenv("PRODUCER_DELAY_SECONDS", 1.0))
DATASET_PATH = BASE_DIR / os.getenv("DATASET_PATH", "data/smart_meter_readings.csv")

# --- Machine Learning Configuration ---
MODEL_DIR = BASE_DIR / "models"
MODEL_PATH = BASE_DIR / os.getenv("MODEL_PATH", "models/anomaly_model.pkl")
MODEL_CONTAMINATION = float(os.getenv("MODEL_CONTAMINATION", 0.01))
EVALUATION_REPORT_PATH = BASE_DIR / os.getenv("EVALUATION_REPORT_PATH", "reports/model_evaluation.json")

# Model feature definitions - strictly aligned between training and inference
MODEL_FEATURES = [
    "Global_active_power",
    "Global_reactive_power",
    "Global_intensity"
]

# Ground-truth anomaly column in synthetic data (EXCLUSIVELY FOR OFFLINE EVALUATION)
GROUND_TRUTH_COLUMN = "synthetic_anomaly_ground_truth"

# --- Stream Processor Configuration ---
ROLLING_WINDOW_SIZE = int(os.getenv("ROLLING_WINDOW_SIZE", 30))

# --- Dashboard Configuration ---
DASHBOARD_REFRESH_INTERVAL = int(os.getenv("DASHBOARD_REFRESH_INTERVAL", 3))

# --- Ensure Essential Directories Exist ---
(BASE_DIR / "data").mkdir(parents=True, exist_ok=True)
(BASE_DIR / "models").mkdir(parents=True, exist_ok=True)
(BASE_DIR / "reports").mkdir(parents=True, exist_ok=True)
(BASE_DIR / "logs").mkdir(parents=True, exist_ok=True)
