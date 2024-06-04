from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import col
from graphframes import GraphFrame
import time

# Create a Spark session
spark = SparkSession.builder \
    .appName("Traffic Jam") \
    .master("local[*]") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .getOrCreate()

# Define the initial vertices
vertices = spark.createDataFrame([
    ("A", "A"), ("B", "B"), ("C", "C"), ("D", "D"),
    ("E", "E"), ("F", "F"), ("G", "G"), ("H", "H"),
    ("I", "I"), ("Z", "Z")
], ["id", "name"])

# Define the initial edges with weights
edges = spark.createDataFrame([
    ("A", "B", 1), ("A", "C", 4), ("B", "C", 2), ("B", "D", 5),
    ("C", "E", 1), ("D", "F", 3), ("E", "D", 1), ("E", "G", 2),
    ("F", "H", 1), ("G", "F", 2), ("G", "I", 3), ("H", "Z", 1),
    ("I", "H", 2), ("I", "Z", 4)
], ["src", "dst", "weight"])

# Create the initial graph
graph = GraphFrame(vertices, edges)

# Function to update the graph and calculate the shortest path
def update_graph_and_calculate_shortest_path(batch_df, batch_id):
    global graph

    # Process each row in the batch
    for row in batch_df.collect():
        src, dst, new_weight = row['src'], row['dst'], row['weight']
        # Update the edge weight
        edges_df = graph.edges.withColumn("weight", col("weight"))
        edges_df = edges_df.withColumn("weight",
                                       col("weight").when((col("src") == src) & (col("dst") == dst), new_weight).otherwise(col("weight")))
        graph = GraphFrame(graph.vertices, edges_df)

        # Recalculate the shortest path from 'A' to 'Z'
        try:
            results = graph.bfs(fromExpr="id = 'A'", toExpr="id = 'Z'", edgeFilter="weight", maxPathLength=10)
            print("Updated shortest path from A to Z:")
            results.show(truncate=False)
        except Exception as e:
            print("Failed to calculate the shortest path:", e)

# Create a streaming context with a batch interval of 10 seconds
ssc = StreamingContext(spark.sparkContext, 10)

# Create a DStream that connects to a socket
lines = ssc.socketTextStream("localhost", 9999)

# Parse the incoming data
updates = lines.map(lambda line: line.split(",")).map(lambda parts: (parts[0], parts[1], int(parts[2])))

# Convert each RDD to a DataFrame
updates.foreachRDD(lambda rdd: update_graph_and_calculate_shortest_path(spark.createDataFrame(rdd, ["src", "dst", "weight"]), None))

# Start the streaming context
ssc.start()

# Simulate a network stream with sleep
time.sleep(60)

# Stop the streaming context
ssc.stop(stopSparkContext=True, stopGraceFully=True)
