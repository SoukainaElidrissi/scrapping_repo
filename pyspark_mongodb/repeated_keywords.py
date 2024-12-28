import pandas as pd
import os
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import warnings

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# --- Step 1: Set Up Environment Variables ---

# Configure Java for Spark (Java 8 required for Hadoop)
os.environ['JAVA_HOME'] = 'C:\\Program Files\\Java\\jdk1.8.0_271'
os.environ['PATH'] += os.pathsep + os.path.join(os.environ['JAVA_HOME'], 'bin')

# Configure Hadoop and Python for Spark
os.environ['HADOOP_HOME'] = 'C:\\hadoop-3.3.6\\hadoop-3.3.6'
os.environ['hadoop.home.dir'] = 'C:\\hadoop-3.3.6\\hadoop-3.3.6'
os.environ['PATH'] += os.pathsep + os.path.join(os.environ['HADOOP_HOME'], 'bin')
os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\DELL\\AppData\\Local\\Programs\\Python\\Python311\\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:\\Users\\DELL\\AppData\\Local\\Programs\\Python\\Python311\\python.exe'

# --- Step 2: Connect to MongoDB ---

# Define MongoDB connection details
mongodb_uri = "mongodb+srv://marouaamal2002:OpHSXGS8MWWZbJnq@ClusterBI.zruer.mongodb.net"

try:
    client = MongoClient(mongodb_uri)
    print("Successfully connected to MongoDB.")
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")
    exit()

# Access the database and collection
db = client["Scrapping"]
collection = db["Merged"]

# --- Step 3: Fetch Data from MongoDB ---

# Convert MongoDB documents to a list of dictionaries, ensuring '_id' is stringified
data = [{**doc, "_id": str(doc["_id"])} for doc in collection.find()]

# Load data into a Pandas DataFrame
df = pd.DataFrame(data)

# Display the first few rows and column names
print("DataFrame Preview:")
print(df.head())
print("\nColumn Names:")
print(df.columns)

# --- Step 4: Flatten Keywords ---

# Explode the 'keywords' list into separate rows
keywords_df = df.explode('keywords')

# Clean the keywords and proceed with further steps
keywords_df = keywords_df.dropna(subset=['keywords'])  # Remove nulls
keywords_df['keywords'] = keywords_df['keywords'].str.lower().str.strip()  # Clean up keywords

# --- Step 5: Count Keyword Frequencies ---

# Count the occurrences of each keyword
keyword_counts = keywords_df['keywords'].value_counts().reset_index()
keyword_counts.columns = ['keyword', 'count']

# Sort the keyword counts in descending order
keyword_counts_sorted = keyword_counts.sort_values(by="count", ascending=False)

# --- Step 6: Save to CSV ---

# Save the most repeated keywords to a CSV file
keyword_counts_sorted.to_csv("most_repeated_keywords.csv", index=False)

# Display the most repeated keywords
print("Most Repeated Keywords: ")
print(keyword_counts_sorted.head())