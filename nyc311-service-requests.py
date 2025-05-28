import requests
import json
import time
import logging
import datetime
from confluent_kafka import Producer

# --- Configuration ---
KAFKA_BROKER = "localhost:9092"
TOPIC = "nyc311-service-requests"
NYC_311_ENDPOINT = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
QUERY_LIMIT = 1000  # API allows up to 1000 per request (official max)
PAUSE_BETWEEN_BATCHES_SEC = 3

# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Kafka Producer setup ---
producer = Producer({"bootstrap.servers": KAFKA_BROKER})

def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Delivery failed: {err}")

def fetch_311_data(since_datetime_iso):
    params = {
        "$limit": QUERY_LIMIT,
        "$order": "created_date ASC",
        "$where": f"created_date > '{since_datetime_iso}'"
    }
    response = requests.get(NYC_311_ENDPOINT, params=params)
    response.raise_for_status()
    return response.json()

if __name__ == "__main__":
    # Start from 90 days ago
    N_DAYS = 90
    since_dt = datetime.datetime.utcnow() - datetime.timedelta(days=N_DAYS)
    since = since_dt.strftime("%Y-%m-%dT%H:%M:%S")
    logging.info(f"NYC 311 Producer: starting from {since} (90 days ago)")

    while True:
        logging.info(f"Querying for created_date > '{since}'")
        try:
            data = fetch_311_data(since)
        except Exception as e:
            logging.error(f"API fetch failed: {e}")
            time.sleep(PAUSE_BETWEEN_BATCHES_SEC)
            continue

        if not data:
            logging.info("No new records, waiting for new data...")
            time.sleep(PAUSE_BETWEEN_BATCHES_SEC)
            continue

        logging.info(f"Fetched {len(data)} records")
        success, failed = 0, 0
        for entry in data:
            try:
                producer.produce(TOPIC, json.dumps(entry).encode("utf-8"), callback=delivery_report)
                success += 1
            except Exception as e:
                logging.error(f"Kafka produce failed: {e}")
                failed += 1

        producer.flush()
        logging.info(f"Published {success} records, failed {failed}")

        # Advance since to latest record in this batch
        since = data[-1].get("created_date", since)
        time.sleep(PAUSE_BETWEEN_BATCHES_SEC)
