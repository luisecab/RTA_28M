import pandas as pd
import matplotlib.pyplot as plt
import logging
from pymongo import MongoClient

# -- Logging for debugging --
logging.basicConfig(filename='plot_batches.log', level=logging.INFO, format='%(asctime)s,%(message)s')

# -- 1. Plot from batch_log.csv --
try:
    df = pd.read_csv("batch_log.csv")
    plt.figure(figsize=(10,5))
    plt.plot(df['batch_num'], df['size'], marker='o')
    plt.title("Kafka Batch Ingestion Size Over Time")
    plt.xlabel("Batch Number")
    plt.ylabel("Records in Batch")
    plt.grid()
    plt.tight_layout()
    plt.show()

    # Batch sizes over time (by timestamp)
    plt.figure(figsize=(10,5))
    plt.plot(pd.to_datetime(df['timestamp']), df['size'], marker='o')
    plt.title("Kafka Batch Ingestion Size vs. Time")
    plt.xlabel("Timestamp")
    plt.ylabel("Records in Batch")
    plt.grid()
    plt.tight_layout()
    plt.show()
    logging.info("Batch log plots generated successfully.")
except Exception as e:
    logging.error(f"Failed to plot batch_log.csv: {e}")
    print("Could not plot batch_log.csv. Reason:", e)

# -- 2. Plot created_date histogram from MongoDB --
try:
    MONGO_URL = "mongodb://localhost:27017/"
    DB_NAME = "nyc311_db"
    COLLECTION_NAME = "service_requests"
    client = MongoClient(MONGO_URL)
    coll = client[DB_NAME][COLLECTION_NAME]

    cursor = coll.find({}, {"created_date": 1, "_id": 0})
    dates = [doc["created_date"] for doc in cursor if "created_date" in doc]
    client.close()

    if dates:
        df_dates = pd.DataFrame({"created_date": dates})
        df_dates["created_date"] = pd.to_datetime(df_dates["created_date"], errors="coerce")
        df_dates = df_dates.dropna(subset=["created_date"])

        # Plot as histogram of counts per day
        counts = df_dates['created_date'].dt.date.value_counts().sort_index()

        plt.figure(figsize=(14,6))
        counts.plot(kind='bar')
        plt.title("NYC311 Service Requests: Count by Created Date")
        plt.xlabel("Date")
        plt.ylabel("Number of Requests")
        plt.tight_layout()
        plt.xticks(rotation=45)
        plt.grid()
        plt.show()
        logging.info("MongoDB created_date histogram generated successfully.")
    else:
        logging.warning("No created_date records found in MongoDB.")
        print("No created_date records found in MongoDB.")
except Exception as e:
    logging.error(f"Failed to plot created_date histogram: {e}")
    print("Could not plot created_date histogram. Reason:", e)
