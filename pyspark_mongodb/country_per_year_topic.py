# Import necessary libraries
import pandas as pd
import os
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
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

# Convert MongoDB documents to a list of dictionaries, ensuring _id is stringified
data = [{**doc, "_id": str(doc["_id"])} for doc in collection.find()]

# Load data into a Pandas DataFrame
df = pd.DataFrame(data)

# Display the first few rows and column names
print("DataFrame Preview:")
print(df.head())
print("\nColumn Names:")
print(df.columns)

# --- Step 4: Data Cleaning ---

# Ensure 'locations' is always a list, even if it's a single float value
df['locations'] = df['locations'].apply(lambda x: [x] if isinstance(x, float) else x)

# Convert 'Day', 'Year', and 'Downloads' columns to integers, handling errors gracefully
df['Day'] = pd.to_numeric(df['Day'], errors='coerce').fillna(0).astype(int)
df['Year'] = pd.to_numeric(df['Year'], errors='coerce').fillna(0).astype(int)
df['Downloads'] = pd.to_numeric(df['Downloads'], errors='coerce').fillna(0).astype(int)

# --- Step 5: Create a Spark DataFrame ---

# Initialize a SparkSession
spark = SparkSession.builder.appName("PyMongo-Spark").getOrCreate()
spark.catalog.clearCache()

# Convert Pandas DataFrame to Spark DataFrame
spark_df = spark.createDataFrame(df)

# Display the Spark DataFrame and its schema
print("Spark DataFrame:")
spark_df.show()
spark_df.printSchema()

# --- Step 6: Analyze Data with Spark ---

# Extract unique Year-Topic combinations, sorted by Year
year_topic_df = spark_df.select("Year", "topic").distinct().orderBy("Year", ascending=True)
print("Year-Topic Combinations:")
year_topic_df.show()

# Group by Year and Topic to count the number of articles for each combination
articles_per_topic_year = spark_df.groupBy("Year", "topic").count()

# Convert the result to a Pandas DataFrame for further processing
articles_per_topic_year_pd = articles_per_topic_year.toPandas()

# --- NEW KPI: Analyze Countries by Topic ---

# Explode the 'countries' array to create one row per country
country_topic_df = spark_df.withColumn("country", F.explode("countries"))

# --- Step 4.1: Data Cleaning for Countries ---

# Remove rows where 'country' is null or empty
country_topic_df = country_topic_df.filter(F.col("country").isNotNull() & (F.col("country") != ""))

# Clean country names (remove leading/trailing spaces and convert to title case for consistency)
country_topic_df = country_topic_df.withColumn("country", F.trim(F.col("country")))  # Trim spaces
country_topic_df = country_topic_df.withColumn("country", F.initcap(F.col("country")))  # Convert to title case

# Ensure the 'country' column contains valid country names
# You can create a list of valid countries to filter out invalid names if necessary
valid_countries = ['USA', 'Canada', 'Mexico', 'Brazil', 'India', 'China', 'Australia', 'Germany', 'United Kingdom', 'France', 'Morocco']  # Example list
country_topic_df = country_topic_df.filter(F.col("country").isin(valid_countries))

# Group by country and topic to count the number of articles
country_topic_counts_cleaned = country_topic_df.groupBy("country", "topic").count()

# Convert the result to a Pandas DataFrame for further processing
country_topic_counts_cleaned_pd = country_topic_counts_cleaned.toPandas()

# Create a pivot table showing the count of articles for each country and topic
country_topic_pivot_cleaned = country_topic_counts_cleaned_pd.pivot(index="country", columns="topic", values="count").fillna(0).astype(int)

# Save the cleaned pivot table to a CSV file
country_topic_pivot_cleaned.to_csv("country_per_topic.csv", index=True)

# Display the cleaned pivot table
print("Cleaned Country-Topic Pivot Table:")
print(country_topic_pivot_cleaned)

# --- Step 7: Create and Save a Pivot Table for Year-Topic ---
  
# Create a pivot table showing the count of articles for each Year and Topic
pivot_table = articles_per_topic_year_pd.pivot(index="Year", columns="topic", values="count").fillna(0).astype(int)

# Save the pivot table to a CSV file
pivot_table.to_csv("country_per_year.csv", index=True)

# Display the pivot table
print("Pivot Table for Year-Topic:")
print(pivot_table)

# --- End of Script --