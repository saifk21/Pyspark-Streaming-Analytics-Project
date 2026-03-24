# In this script we will see everything:
# normal events -> late events -> session window behavior -> watermark enforcement

# that way: We dont need to juggle multiple files
# story of the "data" in one single run
# We 'll have a single contained file which can be shown as complete streaming simulation project

# 1. It generates streaming date with Faker
# 2. Processes in tumbling, sliding and session windows
# 3. Simulate late events and apllies watermark
# Shows how late data is discarded or included based on thresholds

#  Step 1 — Streaming Setup & Data Schema.

# 🔍 📘 Why?
# We need a robust streaming foundation before adding logic(windows, enrichment, late event simulation)
# This Step: 
# Boost a spark session for local streaming demos
# Declares configuration knobs (throughput, watermark, sessiongap, DRY_RUN)
# Defined schemas (typed column structure) Defines schemas (typed column structure) used consistently for both synthetic streams and later parsing
# (important for correctness and performance).

# Prepares small helper functions and constants used later.

# Having explicit schema + controlled Spark config prevents subtle streaming bugs and avoids type surprises when switching between rate/Kafka/file sources.


# 🧱 👨🏫 What I did
# Created local SparkSession with sensible defaults for demo machines
# Declared global config constants (thoughput, session gap, watermark, DRY_RUN)
# Built StructType schemas for :
# Enriched event rows (event_time, cutomer_id, product_id, quantity, unit_price, total_amount, region, category, product_name)
# product/ customer dims(if needed)
# Implemented a small helper to pretty-print config at startup.



# ================= Step 1: Streaming Setup & Data Schema =================
# Purpose: Create SparkSession, config knobs and typed scehma for streaming pipeline
# Safe:DRY_RUN flag present . this block does not alone start any stream or write files

from pyspark.sql import SparkSession, types as T, functions as F
import os
import random
from faker import Faker
import sys

# =========================================================
# ESSENTIAL ENVIRONMENT CONFIGURATION (from previous steps)
# =========================================================
os.environ['SPARK_HOME'] = r'D:\Uncodemy\Pyspark\pyspark_env\Lib\site-packages\pyspark'
os.environ['PYSPARK_PYTHON'] = r'D:\Uncodemy\Pyspark\pyspark_env\Scripts\python.exe'
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python/lib/py4j-0.10.9.5-src.zip'))


# ----------------- CONFIG KNOBS -----------------
DRY_RUN = True                # True => don't persist outputs; False => persist when explicitly used
OUTPUT_BASE = "out/project3_full"   # used only if DRY_RUN = False
CHECKPOINT_DIR = os.path.join(OUTPUT_BASE, "_checkpoints")

# Throughput and timing knobs
ROWS_PER_SECOND = 12              # how many synthetic events per second (rate source)
SHUFFLE_PARTITIONS = 8           # lower for local demos
SESSION_GAP = "30 seconds"          # inactivity gap to delineate a session
WATERMARK = "2 minutes"             # watermark for event-time lateness tolerance

# Product/ customer dim sizes  (small; broadcast-friendly)
NUM_CUSTOMERS = 3000
NUM_PRODUCTS = 300

# Seed for reproducibility
SEED = 42
random.seed(SEED)
Faker.seed(SEED)
fake = Faker()


# ----------------- SPARK SESSION -----------------
spark = (
    SparkSession.builder
    .appName("Project3-FullIntegrated")
    .master("local[*]")                     # use all local cores for demo
    .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
    .getOrCreate()
    
)

# Print nice startup banner
print("="*60)
print(f"Spark Version: {spark.version} | DRY_RUN={DRY_RUN}")
print(f"ROWS_PER_SECOND={ROWS_PER_SECOND}, SESSION_GAP={SESSION_GAP}, WATERMARK={WATERMARK}")
print(f"NUM_CUSTOMERS={NUM_CUSTOMERS}, NUM_PRODUCTS={NUM_PRODUCTS}")
print("="*60)

# ----------------- SCHEMA DEFINITIONS -----------------
# Schema for enriched streaming events after joins (final single row schemas we will sue downstream)
event_schema = T.StructType([
    T.StructField("event_time", T.TimestampType(), True),
    T.StructField("customer_id", T.IntegerType(), True),
    T.StructField("product_id", T.IntegerType(), True),
    T.StructField("quantity", T.IntegerType(), True),
    T.StructField("unit_price", T.DoubleType(), True),
    T.StructField("total_amount", T.DoubleType(), True),
    T.StructField("region", T.StringType(), True),
    T.StructField("category", T.StringType(), True),
    T.StructField("product_name", T.StringType(), True)
])

# small dimension scehmas (useful if reading/writing dims later)
customer_schema = ([
    T.StructField("customer_id", T.IntegerType(), False),
    T.StructField("customer_name", T.StringType(), True),
    T.StructField("region", T.StringType(), True)
])


product_schema = T.StructType([
    T.StructField("product_id", T.IntegerType(), False),
    T.StructField("product_name", T.StringType(), True),
    T.StructField("category", T.StringType(), True),
    T.StructField("base_price", T.DoubleType(), True)
])


# ----------------- HELPER: pretty config printer -----------------

def print_config():
    cfg = {
        "DRY_RUN": DRY_RUN,
        "OUTPUT_BASE": OUTPUT_BASE,
        "CHECKPOINT_DIR": CHECKPOINT_DIR,
        "ROWS_PER_SECOND": ROWS_PER_SECOND,
        "SHUFFLE_PARTITIONS": SHUFFLE_PARTITIONS,
        "SESSION_GAP": SESSION_GAP,
        "WATERMARK": WATERMARK,
        "NUM_CUSTOMERS": NUM_CUSTOMERS,
        "NUM_PRODUCTS": NUM_PRODUCTS
    }
    print("Active config:")
    for k,v in cfg.items():
        print(f"  - {k}: {v}")
    print("-" * 60)
    
    # Run config print once at startup
print_config()

# End of Step 1 block

# 🔁 Full Syntax Breakdown
#  SparkSession.builder.master("local[*]") - > run Spark locally using every available core
# .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS)) -> set shuffle partitions default (tune for small machines)
# T.StructType([...]) / T.StructField(...) -> explicit typed schemas : prevents runtime type/mapping errors and improves performance
# Faker.seed() and rnadom.seed() - > ensure reproducible synthetic generation when we create dim/events later


# 🪞 Visual Imagination

# Picture a control panel with dials:

# A dial for rowsPerSecond,

# A slider for sessionGap,

# A readout showing Spark version and schema cards pinned to the board.
# Everything set — ready to hit “Start Stream” in the next step.

# 🧾 Summary Table
# Item	                                             Purpose
# SparkSession config	                       Local streaming runtime & shuffle tuning
# event_schema	                               Typed contract for enriched event rows (used downstream)
# customer_schema, product_schema	           Dim typing for future joins/persistence
# DRY_RUN	                                   Safety: avoid accidental writes during demos
# ROWS_PER_SECOND	                           Stream throughput control for reproducible demos

# In the next step, Step 2  - Steam generation & enrichment skeleton, this will:
#  create the rate source:
#  map ticks -> event fields
# build small in-memory dims (customers / products) with Faker
# perform boradcast join to enrich steam



#  Step 2 — Stream Generation & Enrichment Skeleton

# 🔍 📘 Why?
# Step 1 gave us schemas + Spark session + config knobs, we need to:
# Generate a synthetic stream using rate source (controlled by ROWS_PER_SECOND)
# Convert ticks -> transaction like events (customer_id, product_id, quantity, price etc)
# Create small in-memory dimension tables (customers, products) using Faker/ random
# Broadcast join stream with dims to enrich events (add product_name, category, region)
# Inject optional late events (simulate real-world delays, respecting WATERMARK)

# This gives us a realistic enriched stream we can then window, sessionize, and analyze in Step 3+.


# 🧱 👨🏫 What we did (plain words)

# Used Spark’s rate source to tick rows at ~ROWS_PER_SECOND.

# Added random mappings to generate event fields (customer/product IDs, quantities).

# Built small products_df and customers_df with Faker/random categories.

# Joined stream with dims → get enriched events with readable names.

# Added a late-event injector: once every ~50 events, push one with event_time 
# artificially delayed by 90 sec (beyond watermark tolerance).


# ================= Step 2: Stream Generation & Enrichment Skeleton =================


from pyspark.sql import DataFrame


# ----------------- DIMENSION GENERATION -----------------
def generate_products(num_products=NUM_PRODUCTS) -> DataFrame:
    """Generate a static products dimension table."""
    categories = ["Electronics", "Clothing", "Home", "Beauty", "Sports"]
    rows = []
    for pid in range(1, num_products+1):
        rows.append((
            pid,
            f"Product-{pid}",
            random.choice(categories),
            round(random.uniform(5.0, 500.0), 2)
        ))
    return spark.createDataFrame(rows, schema=product_schema)

def generate_customers(num_customers=NUM_CUSTOMERS) -> DataFrame:
    """Generate a static customers dimension table."""
    regions = ["North", "South", "East", "West"]
    rows = []
    for cid in range(1, num_customers+1):
        rows.append((
            cid,
            fake.name(),
            random.choice(regions)
        ))
    return spark.createDataFrame(rows, schema=customer_schema)

products_df = generate_products()
customers_df = generate_customers()

# Broadcast for efficient joining
products_b = spark.sparkContext.broadcast(products_df.collect())
customers_b = spark.sparkContext.broadcast(customers_df.collect())
products_df = spark.createDataFrame(products_b.value, schema=product_schema)   
customers_df = spark.createDataFrame(customers_b.value, schema=customer_schema)


# ----------------- STREAMING SOURCE -----------------
# rate: produces two cols: timestamp,  value (monotonically increasing)
raw_stream = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", ROWS_PER_SECOND)
    .load()
) 

# ----------------- EVENT GENERATOR -----------------
# Map raw ticks to synthetic events

def map_event(df: DataFrame) -> DataFrame:
       return (df
        .withColumnRenamed("timestamp", "event_time")
        .withColumn("customer_id", (F.rand(SEED)*NUM_CUSTOMERS).cast("int")+1)
        .withColumn("product_id", (F.rand(SEED+1)*NUM_PRODUCTS).cast("int")+1)
        .withColumn("quantity", (F.rand(SEED+2)*5).cast("int")+1)
        .withColumn("unit_price", (F.rand(SEED+3)*500).cast("double"))
        .withColumn("total_amount", F.col("quantity") * F.col("unit_price"))
    )
       
mapped_stream = map_event(raw_stream)

# ----------------- LATE EVENT INJECTOR -----------------
# Once in ~50 rows backdate event_time by 90 sec (beyond WATERMARK)

def inject_late(df: DataFrame) -> DataFrame:
  return df.withColumn(
        "event_time",
        F.when(F.col("value") % 50 == 0,
               F.col("event_time") - F.expr("INTERVAL 90 seconds"))
         .otherwise(F.col("event_time"))
    )
  

late_injected = inject_late(mapped_stream)

# ----------------- ENRICH WITH DIMS -----------------
enriched = (
    late_injected
    .join(products_df, "product_id")
    .join(customers_df, "customer_id")
    .select(
        "event_time", "customer_id", "product_id", "quantity", "unit_price",
        "total_amount", "region", "category", "product_name"
    )
)

# Sanity check query (console only; DRY_RUN-safe)
query = (
    enriched.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .option("numRows", 5)
    .start()
)


# Let run for ~15s then stop automatically (demo mode)
import time
time.sleep(15)
query.stop()

print("Step 2 completed: Stream generation + enrichment + late events working ✅")


# 🔁 Full Syntax Breakdown
#  spark.readStream.format("rate") -> generates monotonically increasing ticks (value, timestamp)
# .withColumn("customer_id", (F.rand(SEED)*NUM_CUSTOMERS)cast("int")+1) -> pseudo random assignment of Ids
#  inject_late: modifies every 50th row to shift event_time by 90 seconds -> simulates late events
# .join(products_df, "product_id") -> enrich stream with static dims
# .writeStream.format("console") -> print rows for debugging instead of writing to disk


# 🧠 Real-Life Analogy

# Think of this as a movie set:
#  rate = metronome ticking  like a camera shutter
# map_event = actors getting costumes (turning ticks into events)
# products / customers = props and set dressing
# inject_late = a time glitch  where sometimes  a scene is backdated
# enriched = final, fully dressed  actor stepping  onto stage



# 🪞 Visual Imagination

# Imagine a conveyor belt:
# Each tick → an empty box.
# Workers fill it with random items (customer/product).
# Some boxes are stamped with a wrong time (late events).
# Before leaving, boxes pass through a labeling station (enrichment joins).
# Conveyor prints them on a console.

# 🧾 Summary Table
# Item	                           Purpose
# rate source	                   Generates steady tick stream
# map_event	                       Converts ticks → transaction-like events
# inject_late	                   Simulates late events
# products/customers_df	           Static dimensions
# .join()	                       Enrich stream with dims
# .writeStream.console	           Debug output only (safe, no files)


# ✅ At this point,we have a working enriched stream with late-event simulation.
# Next (Step 3) we’ll do Sessionization + Windowed Aggregations using withWatermark and 
# session/window functions.


# Step 3 — Sessionization + Windowed Aggregations
# Theory: why we need sessionization, difference between fixed windows vs session windows, 
# how late events interact with withWatermark.
# Code Part: implement both fixed and windows and session windows


# 1. Why Do We Need Sessionization?

# In streaming, users or devices don’t generate events at a constant rate.

# Example: A customer visits your e-commerce site at 10:02, browses till 10:15, and then comes back again at 11:00.

# If you only aggregate by fixed windows (say, 10 minutes), you split natural sessions across multiple windows.

# 👉 Sessionization solves this by grouping events that happen close together into one session.


# 2. Fixed Windows (Tumbling & Sliding)

# Tumbling window: Fixed size, non-overlapping. (e.g., every 10 minutes).

# Sliding window: Fixed size, overlapping. (e.g., 10 min window sliding every 5 minutes).

# Good for time-based metrics like “total sales per 5 min”.


# 3. Session Windows

# Defined by inactivity gap.

# If no new event comes after a set time (say 15 minutes), we close the session.

# This groups related activity (like a single shopping session).




# 4. Handling Late Events

# In real systems, events may arrive late (network lag, device offline, etc.).
# Spark handles this with withWatermark("eventTimeCol", "delay").
# This allows Spark to keep state only for a safe period (say 30 mins), and drop older late events.



# 5. Use Cases

# Fixed windows: Monitoring dashboards (sales/min, traffic/sec).
# Session windows: Customer activity analysis, fraud detection, user engagement.


# ✅ Prime Takeaway:

# Fixed windows → great for time-based metrics.
# Session windows → great for user/session analysis.
# Always use withWatermark in streaming for late event handling + memory safety.

#  Now we will do coding exmaples for :
# 1. Fixed Tumbling Window (5 minutes sales aggregation)
# 2. Session Window (grouping user activity with inactivity gap of 15 minutes)


# PySpark Streaming Example: Fixed vs Session Windows

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, TimestampType, IntegerType
import random
import os
import sys

# =========================================================
# FORCE SPARK_HOME to point to the correct installation path
# This is the definitve  fix for the 'JavaPackage' error
# =========================================================

# The path should be up to the 'pypark' folder within site-packages
os.environ['SPARK_HOME'] = r'D:\Uncodemy\Pyspark\pyspark_env\Lib\site-packages\pyspark'

# Set the Python Interpreter for PsySpark
os.environ['PYSPARK_PYTHON'] = r'D:\Uncodemy\Pyspark\pyspark_env\Scripts\python.exe'

# Ensure the Spark Libraries are on the path
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python/lib/py4j-0.10.9.5-src.zip'))

# 1 .Create Spark Session
spark = SparkSession.builder.appName("StreamingWindows").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Define schema for incoming data
scehma = StructType() \
    .add("user_id", StringType()) \
    .add("event_time", TimestampType()) \
    .add("action", StringType()) \
    .add("amount", IntegerType())
    
    
# 3. Define schema for incoming data
events = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load() \
    .selectExpr("split(value, ',')[0] as user_id",
                "timestamp(split(value, ',')[1]) as event_time",
                "split(value, ',')[2] as action",
                "int(split(value, ',')[3]) as amount")
    
# 4. Fixed Tumbling Window (5 min)
session_window = events \
    .withWatermark("event_time", "30 minutes") \
    .groupBy(
        F.session_window("event_time", "15 minutes"),
        "user_id"
    ).agg(F.sum("amount").alias("session_amount"))
    
# 6. Output both streams to console
# query1 = fixed_window.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", False) \
#     .start()

query2 = session_window.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()
    
# query1.awaitTermination()
query2.awaitTermination()


# How to Test It

# Run the script.

# In another terminal, start a socket stream:

# nc -lk 9999


# Paste events like CSV format"
# u1,2025-08-16 10:01:00,purchase,50
# u1,2025-08-16 10:03:00,purchase,30
# u1,2025-08-16 10:20:00,purchase,70
# u2,2025-08-16 10:05:00,click,10


# Watch results:

# Fixed window (5 min): aggregates every 5 mins.

# Session window (15 min): groups events into sessions.


# ✅ Practice Questions + Answers

# Q1. What’s the difference between fixed windows and session windows?
# A1. Fixed windows split time into fixed intervals; session windows group events based on inactivity gap.

# Q2. Why is withWatermark important?
# A2. It handles late data and clears old state to save memory.

# Q3. If inactivity gap = 15 mins, and user clicks at 10:01, 10:05, 10:20 — how many sessions?
# A3. Two sessions: [10:01–10:05] and [10:20]



# let’s proceed to Step 4 – Handling Late Data & Watermarking.

# This is the most crucial streaming concept because in real-world data, events don’t always 
# arrive in order — network delays, retries, and system lag can push events minutes (or even hours) late.




# Step 4 – Handling Late Data & Watermarking.
# 📖 Theory
# Event Time Vs Processing Time:
# Event-time → when the event actually happened.
# Processing-time → when Spark received it.
# Example: A user clicked at 10:01, but Spark got it at 10:04.
# Problem: Without control, Spark would keep wiating ingefinitely for late events
# Solution : Watermarking 

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, TimestampType

# 1. Spark session
spark = SparkSession.builder.appName("LateDataWatermarking").getOrCreate()
spark.sparkContext.setLoglevel("WARN")

# 2. Schema for events
schema = StructType() \
    .add("user_id", StringType()) \
    .add("event_time", TimestampType()) \
    .add("action", StringType())
    
# 3. Streaming from socket
events = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load() \
    .selectExpr(
        "split(value, ',')[0] as user_id",
        "timestamp(split(value, ',')[1]) as event_time",
        "split(value, ',')[2] as action"
    )
    

# 4. Apply watermark (wait 5 minutes for late events)
windowed_counts = events \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(
        F.window("event_time", "5 minutes"),
        "action"
    ).count()
    
    
# 5. Output results
query = windowed_counts.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()


# 🔎 Testing

# Run the script → open a socket with:

# nc -lk 9999

# Send events (note the timestamps!):

# u1,2025-08-16 10:01:00,click
# u2,2025-08-16 10:02:00,purchase
# u3,2025-08-16 10:01:30,click
# u4,2025-08-16 10:06:00,click   ← late (arrives after 5 mins)

# ⚡ Spark will:

# Count events within the 5-minute window (10:00–10:05).
# Discard u4 if it comes after watermark expiry.


# ✅ Practice Q&A

# Q1. Why do we need withWatermark?
# A1. To handle late data and prevent Spark from storing old windows forever.

# Q2. What happens if data arrives after watermark limit?
# A2. It is discarded (not counted).

# Q3. If watermark = 5 min, and event at 10:01 arrives at 10:09, will Spark count it?
# A3. No, it’s beyond the 5-min grace period.


# Next step (Step 5) would be Output Sinks — writing streaming results to files, Kafka, or memory.


# 🔹 Step 5 — Output Sinks in PySpark
# 📖 Theory
# Output sink = where your processed streaming results are delivered.
# Spark Structured Streaming supports:
# 1. Console -> for debugging (not for production)\
# 2. File sink -> writes output to files (CSV, Parquet, etc)
# 3. Memory sink -> keep reuslts in spark for adhoc queries
# 4. Kafak sink -> publish results to Kafka topics for downstream consumption
# 5. Foreach sink -> custom logic (e.x write to a database API)

#  Each sink has rules for ouput mode (append, update, or complete)


#  Output Sink Examples:

# 1 .Console Sink (debugging only)
# query = df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", False) \
#     .start()


# 2 .File Sink (Parquet storage)
# query = df.writeStream \
#     .outputMode("append") \
#     .format("parquet") \
#     .option("path", "output/events") \
#     .option("checkpointlocation", "chk/events") \
#     .start()

# ⚠️ Note: Checkpointing is mandatory for file sink — it stores offsets so Spark can resume after a restart.


# 3. Memeory sink (SQL queries on Live stream)
# query = df.writeStream \
#     .outputMode("complete") \
#     .format("memory") \
#     .queryName("live_table") \
#     .start()
    
# # Query the in-memory table
# spark.sql("SELECT * FROM live_table").show()  


# 4. Kafka Sink (real-time pipelines)
# query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("topic", "processed_topic") \
#     .option("checkpointlocation", "chk/kafka_out") \
#     .start()  



# ✅ Practice Q&A

# Q1. Why do we need a checkpoint location when writing to file/Kafka?
# A1. To track processed data and ensure exactly-once guarantees in case of failure/restart.

# Q2. Which sink is best for debugging?
# A2. Console sink.

# Q3. If you want to keep results in Spark memory for ad-hoc queries, which sink do you choose?
# A3. Memory sink.

# Q4. What happens if you write to a Parquet sink without a checkpoint?
# A4. Spark will fail (streaming file sink requires checkpoints).


# 👉 Next step will be Step 6 — Checkpointing & Fault Tolerance, where we’ll cover how Spark ensures 
# exactly-once processing even with failures.

#  This is the safety net of any streaming job. Without it your pipeline may duplicate, lose or reprocess data after failure.


#  🔹 Step 6 — Checkpointing & Fault Tolerance in Structured Streaming


# 📖 Theory
# A checkpoint is Spark’s way of storing the progress and metadata of a streaming query.
# Stored on disk (HDFS, local, S3, etc.).
# Contains:
# Offsets → which data has been read.
# Committed batches → ensures no duplication.
# Query plan & schema metadata → Spark knows how to restart.

# ✅ With checkpointing, Spark provides exactly-once guarantees.


# Example 1 — Writing to Parquet with Checkpoint
# query = df.writeStream \
#     .format("parquet") \
#     .option("path", "output/events") \
#     .option("checkpointLocation", "chk/events") \
#     .outputMode("append") \
#     .start()

# 📌 If the job crashes and you restart → Spark resumes from the last committed offset in chk/events.

# Example 2 — Using Checkpoint in Kafka Sink
# query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("topic", "processed_topic") \
#     .option("checkpointLocation", "chk/kafka_out") \
#     .start()


# 🧩 Best Practices
# Always set a checkpoint directory for sinks other than console/memory.
# Use a persistent location (HDFS, S3, or external disk) — not temp folders.
# Don’t share the same checkpoint folder between different queries.
# ✅ Practice Q&A

# Q1. What is stored in a Spark checkpoint?
# A1. Offsets, committed batches, and query metadata.

# Q2. If you restart a streaming job with checkpointing, what happens?
# A2. Spark resumes exactly where it left off.

# Q3. Which sinks require checkpointing?
# A3. File sink, Kafka sink, and any sink requiring exactly-once delivery.

# 👉 Next step is Step 7 — Watermarking & Late Data Handling ⏰


# 🔹 Step 7 — Watermarking & Late Data Handling
# 📖 Theory
# Problem: Suppose you’re aggregating sales per minute, but some transactions arrive 5 minutes late. Should you include them?
# Without control → Spark would keep infinite state in memory (unbounded).

# Watermark is a threshold that tells Spark:

# “I will wait X minutes for late data, then drop it.”

# Keeps memory usage low and avoids infinite waiting.


# Example 1 — Count Events per 1 Minute Window, Allow 5 Minutes Late

from pyspark.sql import fucntions as F

#  Streaming data with timestamp
# events = df.withColumn("event_time", F.current_timestamp())

# query = events \
#     .withWatermark("event_time", "5 minutes") \
#     .groupBy(F.window("event_time", "1 minute")) \
#     .count() \
#     .writeStream \
#     .format("console") \
#     .outputMode("update") \
#     .start()
    
# 📌 Here:

# Spark will wait 5 minutes for late events.
# After 5 minutes, if a late event comes → it is discarded.


# Example 2 — Watermark with Aggregation
# sales_stream = df.withColumn("event_time", "10 minutes") \
#     .groupBy(
#         F.window("event_time", "15 minutes"),
#         "region"
#     ).agg(F.sum("amount").alias("total_sales"))
    
# query = sales_stream.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()
    
    
# This waits 10 minutes for late data.
# Each window is 15 minutes long.
# 🧩 Best Practices
# Always set a watermark when using aggregations on event-time.
# Watermark duration should match how much lateness you expect.
# If events are too late (beyond watermark) → they are dropped.
# ✅ Practice Q&A

# Q1. Why do we need watermarks in streaming?
# A1. To bound memory usage and avoid infinite waiting for late events.

# Q2. If watermark = 10 min and window = 5 min, what happens to events arriving 12 min late?
# A2. They are dropped (not included).

# Q3. Can you use watermark without event-time column?
# A3. No, watermarks apply only on timestamp columns.


# Next up: Step 8 – End-to-End Project 3 Assembly 🎯
# We’ll integrate tumbling, sliding, session windows, checkpointing, and watermarking into one final
# fraud-detection style streaming job.


# 🏗️ Step 8 — Project 3 Assembly (Fraud Detection Pipeline)

# 🎯 Objective

# Detect suspicious transactions in real-time using PySpark Structured Streaming by combining:

# ✅ Event-time processing
# ✅ Tumbling / sliding windows
# ✅ Session windows
# ✅ Watermarking (late data handling)
# ✅ Checkpointing (fault tolerance) 


# 1. Data Simulation

# We’ll simulate streaming transactions:

# transaction_id (unique ID)
# user_id (customer)
# amount (purchase amount)
# region (geo)
# timestamp (event time)
# 🔹 2. Fraud Rules

# We’ll flag fraud if:

# High-value transactions (e.g., amount > 900).
# Too many transactions in a short window (burst activity).
# Multiple regions in one session (suspicious location switch).

# End-to-End Code (Compact Assembly)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Spark Session
spark = SparkSession.builder \
    .appName("FraudDetectionStreaming") \
    .getOrCreate()

# Simulated Streaming Source
transactions = spark.readStream.format("rate").option("rowsPerSecond", 5).load() \
    .withColumn("transaction_id", F.monotonically_increasing_id()) \
    .withColumn("user_id", (F.rand()*100).cast("int")) \
    .withColumn("amount", (F.rand()*1000).cast("int")) \
    .withColumn("region", F.when(F.rand() > 0.5, "US").otherwise("EU")) \
    .withColumn("event_time", F.current_timestamp())
    
# -------------------------------
# 1️⃣ Rule 1 – High-Value Transaction
high_value = transactions.filter(F.col("amount") > 900) \
    .withColumn("fraud_reason", F.lit("High Value > 900"))
    
# 2️⃣ Rule 2 – Burst Activity (more than 5 txns per 1 min)
burst_activity = transactions \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(F.window("event_time", "1 minute"), "user_id") \
    .count() \
    .filter("count > 5") \
    .withColumn("fraud_reason", F.lit("Burst Activity > 5 txns/min"))
    

# 3️⃣ Rule 3 – Multiple Regions in One Session
session_activity = transactions \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(F.session_window("event_time", "2 minutes"), "user_id") \
    .agg(F.countDistinct("region").alias("unique_regions")) \
    .filter("unique_regions > 1") \
    .withColumn("fraud_reason", F.lit("Multiple Regions in Session"))
    
# -------------------------------
# Union All Fraud Rules
fraud_alerts = high_value.select("transaction_id","user_id","amount","region","event_time","fraud_reason") \
    .union(burst_activity.select("user_id","fraud_reason")) \
    .union(session_activity.select("user_id","fraud_reason"))
    
    
# Write output to console
query = fraud_alerts.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoint/fraud") \
    .start()

query.awaitTermination()

# 🔹 4. Key Takeaways
# Multiple fraud rules combined → shows real-world streaming use case.
# Watermarking ensures Spark doesn’t store infinite state.
# Checkpointing guarantees recovery if job restarts.
# OutputMode=append → only new fraud alerts are printed.
# ✅ Practice Q&A

# Q1. Why union the fraud rules at the end?
# A1. So Spark produces a single fraud alert stream regardless of which rule triggered.

# Q2. What happens if a transaction arrives 10 minutes late with a 5 min watermark?
# A2. It is ignored, not processed.

# Q3. Why use session windows for region changes?
# A3. Because region-switching can happen in bursts, and session windows capture “sessions of activity” better than fixed tumbling windows.
    

# 🏁 Final Report — Project 3: Real-Time Fraud Detection with PySpark Streaming
# 🎯 Objective

# To build a real-time fraud detection pipeline using PySpark Structured Streaming that identifies suspicious transactions based on multiple rules.

# 🔧 Tools & Techniques Used
# PySpark Structured Streaming → continuous data ingestion.
# Faker + rate source → simulating live transactions.
# Windows (tumbling, session) → time-based grouping.
# Watermarking → handling late data.
# Checkpointing → fault tolerance and recovery.
# Union of multiple detection rules → consolidated fraud alerts stream.
# 📊 Fraud Detection Rules Implemented
# High-Value Transaction → amount > 900.
# Burst Activity → more than 5 transactions by the same user within 1 minute.
# Multiple Regions in a Session → same user appears in >1 region within a 2-minute session.
# 🖥️ Pipeline Architecture
# Input Layer → Fake streaming transactions.
# Processing Layer → Spark applies fraud detection logic with windows + watermarking.
# Output Layer → Real-time fraud alerts displayed in console (could be Kafka / DB in real-world).
# ✅ Key Learnings
# Streaming jobs require event-time + watermarking to handle late data.
# Session windows are powerful for detecting behavioral anomalies (e.g., region switches).
# Checkpointing is crucial for fault tolerance.
# Unioning multiple fraud-detection logics allows modular rule expansion.
# 📝 Practice Questions (with Answers)

# Q1. Why do we use checkpointing in Structured Streaming?
# A1. To ensure stateful jobs can recover after restart without losing progress.

# Q2. Which window type is best for "bursty activity"?
# A2. Tumbling or sliding windows.

# Q3. Which window type is best for "irregular session-like activity"?
# A3. Session windows.

# Q4. What happens if watermark is smaller than actual late data delay?
# A4. Late events get dropped (ignored by Spark).
    









