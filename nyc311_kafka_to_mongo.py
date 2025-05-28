import sys
import os
import json
import time
import csv
import datetime
from confluent_kafka import Consumer
from pymongo import MongoClient
from collections import Counter

BATCH_SIZE = 100  # Set your preferred batch size
BATCH_LOG_FILE = "batch_log.csv"
batch_log = []

print("PYTHON EXE:", sys.executable)
print("PATH:", os.environ.get("PATH"))

# Kafka configuration
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'nyc311-mongo-group',
    'auto.offset.reset': 'earliest'
}

# MongoDB configuration
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['nyc311_db']
collection = db['service_requests']

consumer = Consumer(kafka_conf)
consumer.subscribe(['nyc311-service-requests'])

print("Consuming from Kafka and writing to MongoDB...")

batch = []
total_inserted = 0
batch_num = 1
start_time = time.time()

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        try:
            doc = json.loads(msg.value().decode('utf-8'))
            batch.append(doc)
            if len(batch) >= BATCH_SIZE:
                collection.insert_many(batch)
                total_inserted += len(batch)
                elapsed = time.time() - start_time
                now = datetime.datetime.now()
                # Log this batch info
                batch_log.append([batch_num, now.strftime('%Y-%m-%d %H:%M:%S'), len(batch), elapsed])
                print(f"Batch {batch_num}: Inserted {len(batch)} records (total: {total_inserted}) - Elapsed: {elapsed:.1f}s")
                complaint_types = [d.get('complaint_type', 'Unknown') for d in batch]
                top_types = Counter(complaint_types).most_common(3)
                print("  Top complaint types in batch:", top_types)
                batch = []
                batch_num += 1
                start_time = time.time()
        except Exception as e:
            print("Failed to insert:", e)
finally:
    # Insert any leftovers
    if batch:
        collection.insert_many(batch)
        now = datetime.datetime.now()
        batch_log.append([batch_num, now.strftime('%Y-%m-%d %H:%M:%S'), len(batch), time.time() - start_time])
        total_inserted += len(batch)
        print(f"Final batch: Inserted {len(batch)} records (total: {total_inserted})")
    # Write batch log to CSV
    with open(BATCH_LOG_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['batch_num', 'timestamp', 'size', 'elapsed_sec'])
        writer.writerows(batch_log)
    consumer.close()
    mongo_client.close()
