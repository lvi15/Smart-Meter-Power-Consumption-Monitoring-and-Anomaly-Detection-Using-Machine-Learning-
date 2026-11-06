<<<<<<< HEAD
# Real-Time Streaming Analytics for Smart-Meter Power Consumption Monitoring and Anomaly Detection Using Machine Learning

[![Python Version](https://img.shields.io/badge/Python-3.11%2B-blue.svg)](https://www.python.org/)
[![Streaming](https://img.shields.io/badge/Streaming-Apache%20Kafka-red.svg)](https://kafka.apache.org/)
[![Database](https://img.shields.io/badge/Database-MySQL%208.0-orange.svg)](https://www.mysql.com/)
[![Machine Learning](https://img.shields.io/badge/ML-Isolation%20Forest-green.svg)](https://scikit-learn.org/)
[![Dashboard](https://img.shields.io/badge/UI-Streamlit%20%7C%20Plotly-ff69b4.svg)](https://streamlit.io/)

---

## A. Project Introduction

Welcome to the **Smart Meter Real-Time Streaming Analytics & Anomaly Detection** project!

In modern electrical smart grids, smart meters record electricity consumption at high frequencies (every minute or second). Traditional batch-oriented systems analyze this data hours or days after it is recorded, making it impossible to detect sudden power surges, equipment failures, or hazardous electrical defects in real time.

This project implements an **end-to-end real-time data streaming pipeline**. It ingests continuous high-frequency smart-meter readings, performs sliding-window stream processing, runs unsupervised machine-learning anomaly detection on the live stream using an **Isolation Forest** model, persists all enriched records into a **MySQL** relational database, and visualizes real-time metrics and alerts on an interactive **Streamlit + Plotly** live dashboard.

---

## B. Project Objectives

1. **Continuous Ingestion:** Publish high-frequency smart-meter readings into an event-driven messaging backbone using **Apache Kafka**.
2. **Sliding-Window Aggregations:** Maintain a 30-reading sliding window in memory to compute dynamic rolling averages and track session peak electricity demand.
3. **Machine-Learning Anomaly Detection:** Score streaming readings in real time using an **Isolation Forest** model to detect power surges, sudden load shifts, and power-factor distortions without human supervision.
4. **Reliable Persistence:** Store readings idempotently into **MySQL**, preventing duplicate records even during stream consumer rebalances or retries.
5. **Interactive Live Visualization:** Provide an auto-refreshing web dashboard displaying live KPIs, multi-trace time-series charts, anomaly alert panels, historical baseline comparisons, and pipeline health statuses.

---

## C. Six-Layer Architecture

The system is organized into six decoupled architectural layers:

```text
+-------------------------------------------------------------------------------+
|                        SMART METER SIX-LAYER ARCHITECTURE                     |
+-------------------------------------------------------------------------------+

  [ LAYER 1: DATA SOURCE & PRODUCER ]
     |  - Reads 'data/smart_meter_readings.csv' (7 days of 1-minute readings)
     |  - Formats records as JSON (excludes ground-truth labels)
     |  - Publishes to Kafka at configurable pacing (1 msg/sec)
     v
  [ LAYER 2: APACHE KAFKA STREAMING BACKBONE ]
     |  - Topic: 'smartmeter-events'
     |  - Consumer Group: 'smartmeter-processor-group'
     |  - KRaft Mode (No ZooKeeper required)
     v
  [ LAYER 3: PYTHON STREAM PROCESSOR ]
     |  - 30-Reading Sliding Window (deque) -> Rolling Average Calculation
     |  - Session Peak Power Tracker
     |  - Real-time feature extraction: [Active_Power, Reactive_Power, Intensity]
     v
  [ LAYER 4: MACHINE LEARNING INFERENCE (Isolation Forest) ]
     |  - Evaluates decision function & outlier prediction
     |  - Converts: +1 (Inlier) -> 0 (NORMAL) | -1 (Outlier) -> 1 (ANOMALY)
     |  - Computes anomaly score for alert prioritization
     v
  [ LAYER 5: MYSQL RELATIONAL PERSISTENCE ]
     |  - Table 'meter_readings': Stores individual enriched records (Idempotent)
     |  - Table 'meter_metrics': Single-row real-time aggregated KPI metrics
     |  - Manual Kafka offset commits after successful DB writes
     v
  [ LAYER 6: STREAMLIT LIVE DASHBOARD ]
     |  - Real-time KPI Cards (Power, Rolling Avg, Peak, Anomalies, Rate)
     |  - Interactive Plotly Time-Series Graph with Anomaly Scatter Markers
     |  - Appliance Sub-Metering Breakdown & Anomaly Inspection Panel
     |  - Historical Baseline Analytics & Offline ML Evaluation Metrics
+-------------------------------------------------------------------------------+
```

---

## D. Technology Stack

| Technology | Role & Purpose |
| :--- | :--- |
| **Python 3.11** | Core application logic, stream orchestration, and numerical processing |
| **Apache Kafka** | Distributed publish-subscribe event broker decoupling producer and consumer |
| **Scikit-Learn** | Unsupervised machine learning library implementing the Isolation Forest algorithm |
| **Isolation Forest** | Tree-based outlier detection isolating anomalous power spikes in multi-dimensional feature space |
| **MySQL 8.0** | Relational database providing indexed storage and idempotent upsert guarantees |
| **SQLAlchemy / PyMySQL** | Python SQL toolkit and database connectivity drivers |
| **Streamlit** | Reactive web framework rendering the live operator dashboard |
| **Plotly** | High-performance interactive graphing library for streaming time-series charts |
| **Pandas & NumPy** | Fast vector operations, sliding-window calculations, and dataset formatting |
| **Joblib** | Serialization and loading of the trained machine learning model artifact |
| **Docker Compose** | Containerized orchestration for local Kafka and MySQL services |

---

## E. Prerequisites

Before running the project, ensure you have the following installed on your computer:

1. **Python 3.10+** (Python 3.11 recommended): Verify with `python --version`.
2. **Git**: Verify with `git --version`.
3. **Docker Desktop** (Recommended for starting Kafka & MySQL in one command): Verify with `docker --version`.
   - *If Docker is not installed*, you can run a local MySQL Server (listening on port 3306) and local Kafka broker (listening on port 9092), or test offline with the built-in SQLite developer mode (`DB_TYPE=sqlite`).
4. **Network Ports Available:**
   - `9092`: Apache Kafka Broker
   - `3306`: MySQL Database
   - `8501`: Streamlit Web Dashboard

---

## F. Installation & Environment Setup

### 1. Open PowerShell / Command Prompt and Navigate to Workspace
```powershell
cd "d:\vyshu\4th year\7th sem\DSA\Mini project"
```

### 2. Create and Activate a Virtual Environment
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```
*(On Linux/macOS: `source venv/bin/activate`)*

### 3. Install Python Dependencies
```powershell
pip install -r requirements.txt
```

### 4. Configure Environment Variables
Create your local configuration by copying `.env.example`:
```powershell
Copy-Item .env.example .env
```
Open `.env` and verify your MySQL password and Kafka broker settings if necessary.

---

## G. Dataset Generation

Since no pre-existing dataset is required, the project includes an automated synthetic generator that simulates 7 full days of 1-minute smart-meter readings (10,080 observations) with realistic human circadian patterns, appliance sub-metering, and controlled electrical anomalies.

Run the dataset generator:
```powershell
python generate_dataset.py
```

### Dataset Characteristics:
- **Total Records:** 10,080 rows (7 days @ 1-minute intervals).
- **Features Included:**
  - `Date_Time`: Chronological timestamp (`YYYY-MM-DD HH:MM:SS`).
  - `Global_active_power`: Active power demand in kilowatts (kW).
  - `Global_reactive_power`: Reactive power in kilowatts (kW).
  - `Global_intensity`: Current intensity in amperes (A).
  - `Sub_metering_1`: Kitchen appliances (dishwasher, microwave in watt-hours).
  - `Sub_metering_2`: Laundry room (washing machine, tumble-dryer in watt-hours).
  - `Sub_metering_3`: Climate control & electric water heater (in watt-hours).
  - `synthetic_anomaly_ground_truth`: 0 (normal) or 1 (injected anomaly). **Used exclusively for offline evaluation report generation; never used as a model training input feature.**
- **Injected Anomalies:** ~1% of dataset (~105 events) covering extreme power surges (8–11 kW), overnight high-usage spikes, severe power factor distortions, and current mismatches.

---

## H. Machine Learning Model Training

Train the **Isolation Forest** model on historical smart-meter readings.

Run the training script:
```powershell
python train_model.py
```

### What Happens:
1. Loads `data/smart_meter_readings.csv`.
2. Strictly selects physical features: `['Global_active_power', 'Global_reactive_power', 'Global_intensity']`.
3. Excludes `synthetic_anomaly_ground_truth` to prevent data leakage.
4. Splits chronologically: 80% Training (8,064 records), 20% Testing (2,016 records).
5. Fits an `IsolationForest(n_estimators=100, contamination=0.01)`.
6. Serializes the trained model to `models/anomaly_model.pkl`.
7. Evaluates predictions against the ground truth and writes `reports/model_evaluation.json`.

### Actual Offline Training Results:
- **Test Subset Evaluated:** 2,016 records (18 injected anomalies).
- **Test Precision:** 100.0% (No false positives).
- **Test Recall:** 94.4% (Detected 17 of 18 injected anomalies).
- **Test F1-Score:** 0.9714.
- **Overall Dataset:** Evaluated 10,080 records; detected 98 anomalies out of 105 injected.

---

## I. Infrastructure Setup (Kafka & MySQL)

### Option 1: Docker Compose (Fastest & Recommended)
Start both Apache Kafka (KRaft mode) and MySQL 8.0 in the background:
```powershell
docker compose up -d
```
Check that both containers are running and healthy:
```powershell
docker compose ps
```

### Option 2: Local Installed Services
If you have local services installed:
1. **MySQL:** Start Windows service `MySQL80` (or `Start-Service MySQL80`). Ensure the root password in `.env` matches your MySQL installation.
2. **Kafka:** Start your local Kafka broker listening on `localhost:9092`.

---

## J. Database Schema Initialization

Initialize the database tables (`meter_readings` and `meter_metrics`):
```powershell
python init_db.py
```
This script connects to MySQL, creates the database `smart_meter_db` if it does not exist, and creates the tables with unique `event_id` constraints for idempotent stream writing.

*(Note: If MySQL is not running on your computer, `init_db.py` will print a helpful troubleshooting guide. You can set `DB_TYPE=sqlite` in `.env` to test offline).*

---

## K. Complete Execution Sequence (Live Demonstration)

To run the complete real-time streaming pipeline, open **three separate terminals** in the project directory:

```text
+-------------------------------------------------------------------------------+
|                             EXECUTION TERMINALS                               |
+-------------------------------------------------------------------------------+
|  TERMINAL 1: Stream Processor                                                |
|  Command: python stream_processor.py                                          |
|  Role   : Consumes Kafka messages, computes rolling average, runs ML inference|
+-------------------------------------------------------------------------------+
|  TERMINAL 2: Live Streamlit Dashboard                                        |
|  Command: streamlit run dashboard.py                                         |
|  Role   : Visualizes live metrics, streaming graphs, and anomaly alerts       |
+-------------------------------------------------------------------------------+
|  TERMINAL 3: Smart Meter Producer                                             |
|  Command: python producer.py --delay 1.0                                      |
|  Role   : Streams smart-meter readings to Kafka at 1 reading per second       |
+-------------------------------------------------------------------------------+
```

### Step-by-Step Commands:

#### Terminal 1: Start the Stream Processor
```powershell
python stream_processor.py
```
*The processor will connect to Kafka, load `models/anomaly_model.pkl`, connect to MySQL, and wait for incoming events.*

#### Terminal 2: Start the Live Dashboard
```powershell
streamlit run dashboard.py
```
*Your default web browser will open `http://localhost:8501`. If no events have arrived yet, it will display a clean "Waiting for incoming smart-meter readings" status.*

#### Terminal 3: Start the Smart Meter Producer
```powershell
python producer.py --delay 1.0
```
*The producer will read `data/smart_meter_readings.csv` and stream JSON events into the `smartmeter-events` Kafka topic at a rate of 1 message per second.*

---

## L. Live Dashboard Features & Tour

Heading: **SMART METER — REAL-TIME STREAMING ANALYTICS**  
Subtitle: *Live electricity consumption monitoring, streaming data processing, and machine-learning-based anomaly detection.*

### 1. Top KPI Banner
- **Current Power (kW):** The exact active power demand of the most recent streaming event.
- **Rolling Average (kW):** The arithmetic mean of the last 30 valid readings, smoothing out transient spikes.
- **Peak Power (kW):** The highest valid power reading recorded during the current stream processing session.
- **Total Readings:** The total count of unique events processed and persisted in the database.
- **Anomalies Detected:** The count of events flagged as anomalous by the Isolation Forest model.
- **Anomaly Rate (%):** `(Anomalies Detected / Total Readings) * 100`.

### 2. Live Power-Consumption Chart
- Interactive Plotly time-series graph displaying:
  - **Blue line:** Instantaneous active power consumption (kW).
  - **Yellow dotted line:** 30-reading rolling window average.
  - **Red markers:** Anomalous events flagged by the ML model with hover tooltips displaying the exact timestamp, active power, and anomaly score.

### 3. Appliance Sub-Metering Breakdown
- Multi-trace time-series visualization tracking energy usage across:
  - **Sub 1 (Green):** Kitchen appliances (dishwasher, microwave).
  - **Sub 2 (Purple):** Laundry room (washing machine, tumble-dryer).
  - **Sub 3 (Pink):** Climate control & water heating.

### 4. Anomaly Inspection Panel
- Highlights the latest anomaly details and lists the 5 most recent anomalous events in a structured table.
- Explains why anomaly detection is essential and emphasizes that statistical anomalies do not always denote physical electrical faults.

### 5. Live Processed Stream Table
- Real-time tabular log of the latest 100 processed events showing Event ID, Timestamps, Active Power, Rolling Average, Anomaly Status, Score, and Database Commit Timestamp.

### 6. Historical Baseline & Model Evaluation
- Summarizes the historical 7-day dataset with hourly consumption distributions and histograms.
- Displays the live offline evaluation report metrics (Precision, Recall, F1-Score, Confusion Matrix) loaded directly from `reports/model_evaluation.json`.

---

## M. Step-by-Step Demonstration Script (For Presentation to Instructor)

Follow this script to demonstrate the live, reactive nature of the project:

1. **Show Initial State:**
   - Launch the Streamlit dashboard before starting the producer. Point out the "Waiting for incoming readings" indicator and empty KPI state. This proves that numbers are not hardcoded or fabricated.
2. **Start the Stream Producer:**
   - In Terminal 3, run `python producer.py --delay 0.5`.
   - Watch the dashboard immediately respond: Current Power changes, Total Readings increments, and the Plotly time-series curve starts dynamically advancing across the screen.
3. **Show Rolling Average Smoothing:**
   - Show how the yellow dotted rolling average line remains stable and lags instantaneous spikes, demonstrating the 30-reading sliding-window calculation.
4. **Demonstrate Machine-Learning Anomaly Detection:**
   - As an anomalous reading arrives (e.g. an extreme spike of 8+ kW or a severe reactive power surge), point to the **red circular marker** appearing on the graph.
   - Show how the Anomaly Counter increments and the event appears in the **Anomaly Inspection Panel** with its negative decision score.
5. **Demonstrate Stream Pause:**
   - In Terminal 3, press `Ctrl+C` to pause the producer.
   - Show that the dashboard continues to refresh, but the Total Readings and latest values remain frozen, demonstrating true stream dependency rather than random number generation.
6. **Resume Stream:**
   - Restart the producer with `python producer.py --replay`. The readings resume immediately.
7. **Verify Database Idempotency:**
   - Open MySQL or inspect the database table: show that every event has a unique `event_id` and that replaying past events does not create duplicates.

---

## N. Troubleshooting Guide

### 1. `pymysql.err.OperationalError: Can't connect to MySQL server on 'localhost'`
- **Cause:** MySQL service is not running or the password in `.env` is incorrect.
- **Solution:**
  - Start MySQL with Docker: `docker compose up -d mysql`
  - Or start Windows service: in an Administrator PowerShell, run `Start-Service MySQL80`
  - Update `MYSQL_PASSWORD` in `.env` with your actual password.
  - Or switch to local testing mode: set `DB_TYPE=sqlite` in `.env` and rerun `python init_db.py`.

### 2. `kafka.errors.NoBrokersAvailable: NoBrokersAvailable`
- **Cause:** Kafka broker is not running on port 9092.
- **Solution:**
  - Start Kafka with Docker: `docker compose up -d kafka`
  - Verify Kafka is running: `docker compose ps`
  - Test port connectivity: `Test-NetConnection -ComputerName localhost -Port 9092`

### 3. Missing Dataset or Model File
- **Solution:**
  - Run `python generate_dataset.py` to create `data/smart_meter_readings.csv`.
  - Run `python train_model.py` to train and save `models/anomaly_model.pkl`.

### 4. Port Conflict on 8501 or 9092
- **Solution:**
  - Streamlit: Run on another port using `streamlit run dashboard.py --server.port 8502`.
  - Kafka: Stop conflicting services or modify the port mapping in `docker-compose.yml`.

---

## O. Automated Testing Suite

The project includes an automated test suite implemented with `pytest`:

```powershell
pytest -v
```

### Tests Covered:
- `tests/test_dataset.py`: Validates dataset generation, row count (>=10,000), column names, non-negative values, chronological timestamps, and reproducibility.
- `tests/test_anomaly_model.py`: Validates model training, Joblib serialization, absence of target leakage, prediction mapping (`+1` -> `0`, `-1` -> `1`), and evaluation metrics.
- `tests/test_processing.py`: Validates rolling-average startup (<30 readings), 30-reading window boundary shift, session peak power tracking, and anomaly status mapping.
- `tests/test_database.py`: Validates schema creation, single-row metric updates, and idempotent record insertion (inserting the same `event_id` twice produces exactly 1 row).

---

## P. Limitations and Ethical Considerations

1. **Synthetic vs. Real-World Data:** The dataset generated in this project is synthetic and designed for educational streaming pipeline demonstrations. Real-world electrical smart-meter data exhibits grid voltage fluctuations, reactive power shifts, and seasonal variations that require domain-specific calibration.
2. **Unsupervised Outliers vs. Genuine Faults:** An Isolation Forest model identifies statistical outliers based on historical training data. In a physical power grid, a flagged anomaly may represent an innocent event (such as turning on an electric vehicle fast-charger or high-power kiln) rather than a short circuit or meter tamper. Domain verification is mandatory before taking protective action.

---

## Q. Future Enhancements

1. **Predictive Load Forecasting:** Integrate an LSTM or Prophet model to forecast energy demand 1 hour ahead.
2. **Distributed Processing Engines:** Scale the Python stream processor to Apache Flink or Apache Spark Streaming for multi-node distributed processing.
3. **Automated Alert Webhooks:** Configure Slack, Discord, or SMS webhooks to alert electrical maintenance engineers immediately when consecutive anomalies occur.

=======
# Smart-Meter-Power-Consumption-Monitoring-and-Anomaly-Detection-Using-Machine-Learning-
>>>>>>> 2c30d14e382d116c5ff9ba21a3f59c19b37e0c6e
