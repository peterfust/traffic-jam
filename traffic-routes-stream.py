import signal
from dbm.ndbm import library

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import col, when
from graphframes import GraphFrame
from common_values import demo_edges, demo_vertices, route
import logging
import warnings

# Logging und Warnings konfigurieren, damit die hässlichen Meldungen nicht erscheinen aus den Bibliotheken
logging.basicConfig(level=logging.ERROR)
warnings.filterwarnings("ignore")


def calculate_and_print_route(row):
    duration = 0
    path = f"{row['from'][0]}"
    for col in [c for c in row.asDict() if c.startswith('e')]:
        duration += row[col]['total_distance']
        path += f" -> ({row[col]['total_distance']}) -> {row[col]['dst']}"
    print(path)
    return duration


# Funktion zum Updaten des Graphen und Berechnen des kürzesten Pfades
def update_graph_and_calculate_shortest_path(batch_df, rout_to_calculate):
    global graph

    # Update verarbeiten
    for row in batch_df.collect():
        src, dst, new_traffic = row['src'], row['dst'], row['traffic']
        print(f"Neue Staumeldung: Der Stau zwischen {src} und {dst} beträgt {new_traffic} min")

        # Stauwerte anpassen
        edges_df = graph.edges.withColumn("traffic",
                                          when((col("src") == src) & (col("dst") == dst), new_traffic).otherwise(
                                              col("traffic")))
        # Kombinierter Wert aus Distanz und Stau berechnen
        edges_df = edges_df.withColumn("total_distance", col("distance") + col("traffic"))
        graph = GraphFrame(graph.vertices, edges_df)

        # Kürzester Pfad berechnen aufgrund der Stauprognosen
        try:
            results = graph.bfs(fromExpr=f"id = '{rout_to_calculate[0]}'",
                                toExpr=f"id = '{rout_to_calculate[1]}'",
                                maxPathLength=10)

            print(f"Mögliche Wege zwischen {rout_to_calculate[0]} und {rout_to_calculate[1]} sind:")
            # Berechnung des kürzesten Weges
            shortest_duration = 1000000
            shortest_row = None
            for row in results.collect():
                duration = calculate_and_print_route(row)
                if duration < shortest_duration:
                    shortest_duration = duration
                    shortest_row = row
            print(
                f"Der aktuelle beste Weg zwischen {row['from'][1]} und {row['to'][1]} dauert {shortest_duration} Minuten.")
            calculate_and_print_route(shortest_row)
        except Exception as e:
            print("Die Berechnung ist fehlgeschlagen:", e)


def start_spark_service():
    # Spark Session initialisieren
    spark = SparkSession.builder \
        .appName("Traffic Jam") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Graph initialisieren mit den Demo-Daten
    vertices = spark.createDataFrame(demo_vertices, ["id", "name"])
    edges = spark.createDataFrame(demo_edges, ["src", "dst", "distance", "traffic"])
    global graph
    graph = GraphFrame(vertices, edges)

    # Spark Stream vorbereiten für Producer
    spark_streaming_context = StreamingContext(spark.sparkContext, 10)
    lines = spark_streaming_context.socketTextStream("localhost", 9999)
    updates = lines.map(lambda line: line.split(",")).map(lambda parts: (parts[0], parts[1], int(parts[2])))

    # RDDs updaten und Pfad berechnen
    updates.foreachRDD(
        lambda rdd: update_graph_and_calculate_shortest_path(spark.createDataFrame(rdd, ["src", "dst", "traffic"]),
                                                             route) if not rdd.isEmpty() else None)
    spark_streaming_context.start()
    return spark_streaming_context


print("Starte Spark Service")
print("Drücke Strg+C zum Beenden")
keep_running = True
graph = None
ssc = start_spark_service()

def console_handler(sig, frame):
    global keep_running
    print('Du hast Strg+C gedrückt und den Spark Service beendet')
    keep_running = False
    # Stop the streaming context
    ssc.stop(stopSparkContext=True, stopGraceFully=True)


signal.signal(signal.SIGINT, console_handler)

# Warten, bis der Service mit Ctr + C beendet wird
while True:
    try:
        pass
    except KeyboardInterrupt:
        break
