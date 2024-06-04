from pyspark.sql import SparkSession
from graphframes import GraphFrame

# Create a Spark session
spark = SparkSession.builder \
    .appName("Shortest Path with GraphFrames") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .getOrCreate()

# Define vertices
vertices = spark.createDataFrame([
    ("A", "A"), ("B", "B"), ("C", "C"), ("D", "D"),
    ("E", "E"), ("F", "F"), ("G", "G"), ("H", "H"),
    ("I", "I"), ("Z", "Z")
], ["id", "name"])

# Define edges with weights
edges = spark.createDataFrame([
    ("A", "B", 1), ("A", "C", 4), ("B", "C", 2), ("B", "D", 5),
    ("C", "E", 1), ("D", "F", 3), ("E", "D", 1), ("E", "G", 2),
    ("F", "H", 1), ("G", "F", 2), ("G", "I", 3), ("H", "Z", 1),
    ("I", "H", 2), ("I", "Z", 4)
], ["src", "dst", "weight"])

# Create the graph
graph = GraphFrame(vertices, edges)

# Calculate shortest path from 'A' to 'Z' using BFS
results = graph.bfs(
    fromExpr="id = 'A'",
    toExpr="id = 'Z'",
    edgeFilter="weight <= 3",
    maxPathLength=10
)

# Show the results
results.show(truncate=False)

spark.stop()
