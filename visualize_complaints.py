import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt

# Connect to MongoDB
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['nyc311_db']
collection = db['service_requests']

# Load a sample of documents into a DataFrame
docs = list(collection.find().limit(1000))  # Limit as needed
df = pd.DataFrame(docs)

# Show basic info
print(df.head())

# Example: Visualize complaint types
df['complaint_type'].value_counts().head(10).plot(kind='bar')
plt.title("Top 10 Complaint Types")
plt.ylabel("Count")
plt.xlabel("Complaint Type")
plt.show()