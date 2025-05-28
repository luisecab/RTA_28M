import pandas as pd
import matplotlib.pyplot as plt
import logging

logging.basicConfig(filename='batch_log.csv', level=logging.INFO, format='%(asctime)s,%(message)s')

df = pd.read_csv("batch_log.csv")
plt.figure(figsize=(10,5))
plt.plot(df['batch_num'], df['size'], marker='o')
plt.title("Kafka Batch Ingestion Size Over Time")
plt.xlabel("Batch Number")
plt.ylabel("Records in Batch")
plt.grid()
plt.show()

# Optional: Batch sizes over time
plt.figure(figsize=(10,5))
plt.plot(pd.to_datetime(df['timestamp']), df['size'], marker='o')
plt.title("Kafka Batch Ingestion Size vs. Time")
plt.xlabel("Timestamp")
plt.ylabel("Records in Batch")
plt.grid()
plt.show()
