# NYC 311 Service Requests Real-Time Analytics

This project implements a real-time analytics pipeline for NYC 311 service requests using Apache Kafka and Apache Spark Structured Streaming.

## Prerequisites

- Python 3.7+
- Docker Desktop for Windows (or Docker Engine for Linux/Mac)
- PowerShell, Windows Terminal, or Bash
- pip (Python package installer)
- virtualenv or venv (Python virtual environment)

## Environment Setup

### Windows (PowerShell)

1. **Open PowerShell as Administrator**:

   - Right-click on PowerShell
   - Select "Run as Administrator"

2. **Enable Script Execution** (if not already enabled):

   ```powershell
   Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
   ```

3. **Create and Activate Virtual Environment**:

   ```powershell
   cd "D:\projects\kafka_codes"
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   ```

4. **Install Dependencies**:
   ```powershell
   python -m pip install --upgrade pip
   pip install -r requirements.txt
   ```

### Linux/Mac (Bash)

1. **Create and Activate Virtual Environment**:

   ```bash
   cd /path/to/kafka_codes
   python3 -m venv venv
   source venv/bin/activate
   ```

2. **Install Dependencies**:
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

## Kafka Infrastructure

### Start Kafka and Zookeeper with Docker Compose

1. **Run Docker Compose**:

   ```bash
   docker-compose up -d
   ```

   This will start:

   - Zookeeper (port 2181)
   - Kafka (port 9092)

2. **Verify Kafka Containers**:

   ```bash
   docker ps
   ```

   Ensure the Kafka and Zookeeper containers are running.

3. **Stop Docker Compose** (when done):
   ```bash
   docker-compose down
   ```

## Running the Producer

1. **Open a new terminal**.
2. **Activate the virtual environment**:
   ```powershell
   .\venv\Scripts\Activate.ps1
   ```
3. **Run the Producer**:

   ```powershell
   python scripts/nyc311-service-requests.py
   ```

   The producer fetches the latest NYC 311 requests every 60 seconds and sends them to the Kafka topic `nyc311-service-requests`.

## Running the Consumer

1. **Open another terminal**.
2. **Activate the virtual environment**:
   ```powershell
   .\venv\Scripts\Activate.ps1
   ```
3. **Run the Consumer**:

   ```powershell
   python scripts/nyc311_kafka_to_mongo.py
   ```

   The consumer reads messages from the Kafka topic `nyc311-service-requests` and writes them to a MongoDB collection named `service_requests`.

## Visualizing Data

1. **Run the Visualization Script**:

   ```bash
   python scripts/visualize_complaints.py
   ```

   This script connects to MongoDB, retrieves data from the `service_requests` collection, and generates visualizations such as complaint type distributions.

## Project Structure

```
kafka_codes/
├── scripts/                # Python scripts
│   ├── batching.py         # Kafka consumer with batch processing
│   ├── plot_batches.py     # Visualization of batch ingestion
│   ├── nyc311-service-requests.py # Kafka producer for NYC 311 requests
│   ├── nyc311_kafka_to_mongo.py # Kafka consumer to MongoDB pipeline
│   ├── api_health_check.py # API health check script
│   └── visualize_complaints.py  # Data visualization script
├── logs/                   # Log files
│   └── batch_log.csv       # Batch ingestion logs
├── data/                   # Data files
│   └── sample_data.json    # Example data for testing
├── tmp/                    # Temporary files
│   └── spark_checkpoints/  # Spark checkpoint directory
├── docker-compose.yml      # Kafka infrastructure setup
├── requirements.txt        # Python dependencies
├── README.md               # Project documentation
└── .venv/                  # Virtual environment (optional, not committed to version control)
```

## Notes

- Ensure Docker is running before starting Kafka and Zookeeper.
- MongoDB must be running locally or accessible remotely for the consumer and visualization scripts.
- The producer creates the topic `nyc311-service-requests` if it does not exist.
