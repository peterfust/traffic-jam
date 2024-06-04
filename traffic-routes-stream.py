from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import col
from graphframes import GraphFrame
import time

# Create a Spark session
spark = SparkSession.builder \
    .appName("Traffic Jam") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .getOrCreate()

# Places from Zürich to Basel
vertices = spark.createDataFrame([
    ("ZUERICH", "ZUERICH"), ("BASEL", "BASEL"), ("AARAU", "AARAU"), ("OLTEN", "OLTEN"),
    ("BADEN", "BADEN"), ("FRICK", "FRICK")
], ["id", "name"])

# Define the initial edges with weights
edges = spark.createDataFrame([
    ("ZUERICH", "BADEN", 1), ("BADEN", "FRICK", 1), ("FRICK", "BASEL", 1), ("ZUERICH", "AARAU", 1),
    ("AARAU", "FRICK", 1), ("FRICK", "BASEL", 1), ("ZUERICH", "AARAU", 1), ("AARAU", "EGERKINGEN", 1),
    ("EGERKINGEN", "BASEL", 1)
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
