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
   cd "C:\Users\foxtrot\OneDrive - SGH\Semester 4\rta\kafka_nyc311_project\labs\kafka_codes"
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
   cd /path/to/kafka_nyc311_project/labs/kafka_codes
   python3 -m venv venv
   source venv/bin/activate
   ```

2. **Install Dependencies**:
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

## Kafka Infrastructure

1. **Start Kafka and Zookeeper with Docker Compose**:

   ```bash
   docker-compose up -d
   ```

   This will start:

   - Zookeeper (port 2181)
   - Kafka (port 9092)

   The default topic created is `test-topic`. The producer will use `nyc311_requests` (created automatically if it doesn't exist).

2. **Check Kafka containers**:
   ```bash
   docker ps
   ```

## Running the Producer

1. **Start the Producer**:

   ```powershell
   python nyc311_producer.py
   ```

   or (Linux/Mac):

   ```bash
   python nyc311_producer.py
   ```

   The producer will fetch the latest NYC 311 requests every 60 seconds and send them to the Kafka topic `nyc311_requests`.

## Project Structure

```
labs/kafka_codes/
├── venv/                   # Virtual environment (after setup)
├── docker-compose.yml      # Kafka infrastructure setup
├── requirements.txt        # Python dependencies
├── nyc311_producer.py     # Main producer code
└── README.md              # This file
```

## Notes

- The producer will create the topic `nyc311_requests` if it does not exist.
- The Docker Compose file uses wurstmeister images for Kafka and Zookeeper.
- No Kafka UI is included; use command-line tools or your own UI if needed.
- All dependencies are now minimal and compatible with the provided code.

## Cleaning Up

1. **Stop the Services**:
   ```powershell
   docker-compose down
   ```
2. **Deactivate Virtual Environment**:
   ```powershell
   deactivate
   ```

## Data Source

**NYC Open Data – 311 Service Requests Dataset**

- **API Endpoint:** https://data.cityofnewyork.us/resource/erm2-nwe9.json
- **Dataset ID:** erm2-nwe9
- **Documentation:** [NYC Open Data – 311 Service Requests](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9/about_data)

The dataset contains all service requests made through 311 in New York City. Each record includes:

- Unique identifier
- Created date and time
- Complaint type
- Location (address, ZIP code, borough)
- Status
- Responding agency

## Running the Pipeline

1. Start the Kafka producer:

```bash
python nyc311_producer.py
```

2. In a separate terminal, start the Spark consumer:

```bash
python nyc311_consumer.py
```

## Analytics Output

The consumer processes the data and outputs three types of analytics:

1. **Complaint Type Analysis**: Counts of complaints by type in 1-hour windows
2. **ZIP Code Hotspots**: Areas with high complaint volumes (>10 complaints in 30 minutes)
3. **Agency Performance**: Complaint status by agency in 1-hour windows
