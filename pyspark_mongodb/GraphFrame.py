import sys
import os
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, year
from graphframes import GraphFrame
import pandas as pd
import warnings
import matplotlib.pyplot as plt
import numpy as np

# Set JAVA_HOME and HADOOP_HOME environment variables
os.environ['JAVA_HOME'] = 'C:\\Program Files\\Java\\jdk1.8.0_271'
os.environ['HADOOP_HOME'] = 'C:\\hadoop-3.3.6\\hadoop-3.3.6'
os.environ['hadoop.home.dir'] = 'C:\\hadoop-3.3.6\\hadoop-3.3.6'
os.environ['PATH'] += os.pathsep + os.path.join(os.environ['HADOOP_HOME'], 'bin')
os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\DELL\\AppData\\Local\\Programs\\Python\\Python311\\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:\\Users\\DELL\\AppData\\Local\\Programs\\Python\\Python311\\python.exe'

# Suppress warnings and error messages
warnings.filterwarnings("ignore", category=UserWarning)
sys.tracebacklimit = 0  # Hide tracebacks in the terminal

# Suppress specific connection error
try:
    client = MongoClient("mongodb+srv://marouaamal2002:OpHSXGS8MWWZbJnq@ClusterBI.zruer.mongodb.net/Scrapping.Blockchain")
    db = client["Scrapping"]
    collection = db["Merged"]

    data = [{**doc, "_id": str(doc["_id"])} for doc in collection.find()]
except ConnectionResetError:
    data = []  # Ignore and continue without MongoDB data

# Convert MongoDB data to Pandas DataFrame if available
if data:
    df = pd.DataFrame(data)
    df['locations'] = df['locations'].apply(lambda x: [x] if isinstance(x, float) else x)

    # Convert 'Day', 'Year', and 'Downloads' columns to integers
    df['Day'] = pd.to_numeric(df['Day'], errors='coerce').fillna(0).astype(int)
    df['Year'] = pd.to_numeric(df['Year'], errors='coerce').fillna(0).astype(int)
    df['Downloads'] = pd.to_numeric(df['Downloads'], errors='coerce').fillna(0).astype(int)
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("PyMongo-Spark") \
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
        .getOrCreate()

    # Convert Pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(df)

    # Print the schema of the Spark DataFrame
    spark_df.printSchema()

    # Show the data
    spark_df.show()

    # Example: Convert the 'Year' column to an integer if it's not already
    spark_df = spark_df.withColumn("Year", spark_df["Year"].cast("int"))

    # 1. Filter and extract data based on temporal dimension (Year or Month)
    temporal_df = spark_df.filter(spark_df['Year'].isNotNull())

    # 2. Extract authors and collaborations between them (Edges)
    edges = temporal_df.select("authors", "Year") \
        .withColumn("author1", explode(col("authors"))) \
        .withColumn("author2", explode(col("authors"))) \
        .filter(col("author1") != col("author2")) \
        .dropDuplicates()

    # 3. Create vertices (authors)
    authors_df = edges.select("author1").union(edges.select("author2")).distinct()
    authors_df = authors_df.withColumnRenamed("author1", "id")

    # 4. Create edges dataframe (author collaborations)
    edges_df = edges.withColumnRenamed("author1", "src").withColumnRenamed("author2", "dst")

    
    graph = GraphFrame(authors_df, edges_df)    
    results = graph.pageRank(resetProbability=0.15, maxIter=50)
     
    # Show top 50 influential authors across all years
    print("Top 50 Influential Authors")
    top_50_authors = results.vertices.select("id", "pagerank") \
                                      .orderBy("pagerank", ascending=False) \
                                      .limit(50)  
    top_50_authors.show()

    # Optional: Plot or analyze the results further
    pagerank_data = top_50_authors.toPandas()
    output_file = "top_50_influential_authors.csv"
    pagerank_data.to_csv(output_file, index=False)

    print(f"Top 50 influential authors saved to {output_file}")

    # Visualization (optional)
    plt.figure(figsize=(10, 6))
    plt.bar(pagerank_data['id'], pagerank_data['pagerank'])
    plt.title('Top 50 Influential Authors (Across All Years)')
    plt.xlabel('Authors')
    plt.ylabel('PageRank')
    plt.xticks(rotation=90)
    plt.show()

else:
    print("Aucune donnée à afficher.")
