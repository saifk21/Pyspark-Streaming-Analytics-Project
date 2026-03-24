# Session windows let you group events into sessions per key (e.g., per customer) where a session 
# is defined by a gap of inactivity (e.g., 30 seconds). This is perfect for user-session analytics like
# “how many purchases in each shopping session” or “session revenue


# project3_streaming_session_windows.py: a full script that simulates a live stream (Spark rate), 
# enriches it, computes session windows per customer, aggregates revenue & units per session, and writes 
# to console + memory sink — no files.

# project3_streaming_session_windows.py
# Real-time session-window analytics (no disk writes)
# Requirements: pyspark, faker (faker only used for static dims)
# Run: python project3_streaming_session_windows.py

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from faker import Faker
import random
import signal
import sys
import os

# =========================================================
# FORCE SPARK_HOME to point ot the correct installation path
# =========================================================
os.environ['SPARK_HOME'] = r'D:\Uncodemy\Pyspark\pyspark_env\Lib\site-packages\pyspark'
os.environ['PYSPARK_PYTHON'] = r'D:\Uncodemy\Pyspark\pyspark_env\Scripts\python.exe'

sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python/lib/py4j-0.10.9.5-src.zip'))

# =========================================


# ---------------- 0) CONFIG ----------------
ROWS_PER_SECOND = 8          # stream throughput
NUM_CUSTOMERS    = 2000      # small static dim for broadcast
NUM_PRODUCTS     = 200
SESSION_GAP       = "30 seconds"   # inactivity gap that closes a session
TOP_N_PER_SESSION = 5

# ---------------- 1) Spark session ----------------
spark = (
    SparkSession.builder
    .appName("Project3-SessionWindows")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

# Graceful shutdown handler so Ctrl+C stops streams cleanly
def _shutdown(signum, frame):
    print("\nStopping streaming queries and Spark...")
    for q in spark.streams.active:
        try:
            q.stop()
        except Exception:
            pass
    spark.stop()
    sys.exit(0)
    
signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)


# ---------------- 2) Build tiny static dims (broadcast) ----------------
fake = Faker()
random.seed(42); Faker.seed(42)

regions = ["North","South","East","West"]
# customers: id -> region
customers_py = [(i, fake.name(), regions[i % len(regions)]) for i in range(1, NUM_CUSTOMERS + 1)]
dim_customers = spark.createDataFrame(customers_py, "customer_id INT, customer_name STRING, region STRING")

# products
products_py = []
for pid in range(1, NUM_PRODUCTS + 1):
    name = f"Product-{pid:04d}"
    price = round(random.uniform(5, 1500), 2)
    products_py.append((pid, name, float(price)))
dim_products = spark.createDataFrame(products_py, "product_id INT, product_name STRING, base_price DOUBLE")

# ---------------- 3) Create live source via rate (no files) ----------------
ticks = (
    spark.readStream
         .format("rate")
         .option("rowsPerSecond", ROWS_PER_SECOND)
         .load()
)

# convert tick -> event, map to customer/product, random qty
events = (
    ticks
    .withColumn("event_time", F.col("timestamp"))
    .withColumn("customer_id", (F.col("value") % NUM_CUSTOMERS) + 1)
    .withColumn("product_id", (F.col("value") % NUM_PRODUCTS) + 1)
    .withColumn("quantity", (F.floor(F.rand()*5) + 1).cast("int"))
    .select("event_time","customer_id","product_id","quantity")
)


# ---------------- 4) Enrich events (broadcast join) ----------------
stream_enriched = (
    events
    .join(F.broadcast(dim_customers.select("customer_id","region")), on="customer_id", how="left")
    .join(F.broadcast(dim_products.select("product_id","product_name","base_price")), on="product_id", how="left")
    .withColumn("unit_price", F.coalesce(F.col("base_price"), F.lit(0.0)))
    .withColumn("total_amount", F.col("unit_price") * F.col("quantity"))
    .select("event_time","customer_id","region","product_id","product_name","quantity","unit_price","total_amount")
)


# ---------------- 5) Session windows aggregation ----------------
# Use session window: group events belonging to the same customer session (gap = SESSION_GAP)
# Spark provides session_window function -> it produces a column similar to window but representing sessions
sessionized_stream = (
    stream_enriched
    .withWatermark("event_time", "1 minute")
    .groupBy(
        F.session_window("event_time", SESSION_GAP).alias("session"),
        F.col("customer_id"),
        F.col("region")
    )
    .agg(
        F.count("*").alias("events_count"),
        F.sum("quantity").alias("units"),
        F.sum("total_amount").alias("session_revenue")
        # REMOVED F.min("session") here - it caused the error!
    )
    .select(
        F.col("session.start").alias("session_start"), # Access the struct directly
        F.col("session.end").alias("session_end"),
        "customer_id",
        "region",
        "events_count",
        "units",
        F.round(F.col("session_revenue"), 2).alias("session_revenue")
    )
)


# ---------------- 6) Top-N per session (rank within each session and region) ----------------
# We cannot run dense_rank on the stream directly. We must do it in the batch function.
def process_batch_with_ranking(df,epoch_id):
    print(f"--- Processing Batch: {epoch_id} ---")
    df.persist()
    
    if df.count() > 0:
        # 1. Define Window for Ranking (Static context)
        rank_window = Window.partitionBy("session_start", "session_end", "region").orderBy(F.desc("session_revenue"), F.desc("units"))
        
        # Apply Rank
        topn_batch = (
            df
            .withColumn("rank", F.dense_rank().over(rank_window))
            .filter(F.col("rank") <= TOP_N_PER_SESSION)
            .orderBy("session_start", "region", "rank")
        )
        
        # 3. Sink to Console
        topn_batch.show(100, truncate=False)
        
        # Sink to memory
        topn_batch.createOrReplaceGlobalTempView("sessions_topn_live")
    else:
        print("No sessions closed in this batch yet.")

    df.unpersist()
    
    
# ---------------- 7) Start streaming query ----------------
# We only need ONE query now because foreachBatch handles both Console and Memory
query = (
    sessionized_stream.writeStream
    .outputMode("complete") 
    .foreachBatch(process_batch_with_ranking)
    .trigger(processingTime="15 seconds")
    .start()
)

print("Streaming session-window Top-N started. Press Ctrl+C to stop.")
query.awaitTermination()  


# 🔍 📘 Why? (Purpose)

# We want to detect sessions of user activity (periods of continuous interaction separated by inactivity).
# This answers questions like:

# “Per customer session, how many purchases and how much revenue?”

# “Per region, which sessions generated the most revenue right now?”

# Session windows are essential for user-activity analytics 
# (web sessions, shopping carts, user journeys).  


# 🧱 👨🏫 What we Did (Plain words)

# Start Spark streaming session (rate source) to simulate events.

# Create small customers and products lookup tables (broadcast-friendly).

# Convert ticks into events (map tick → customer/product/quantity).

# Broadcast-join to enrich each event with region and price.

# Use session_window(event_time, gap) to group consecutive events per customer into sessions 
# where gap of inactivity is SESSION_GAP (30s).

# Aggregate per session: count events, sum units, sum revenue.

# Use a window function to rank sessions per region and keep top N per session-window.

# Output aggregated Top‑N sessions to the console and memory sinks for live inspection.


# 🔁 Full Syntax Breakdown (important parts)

# spark.readStream.format("rate").option("rowsPerSecond", n).load()
# → built‑in streaming source producing (timestamp, value).

# F.session_window("event_time", SESSION_GAP)
# → groups rows into session windows: a session is a continuous sequence of events where adjacent events are no more than SESSION_GAP apart. Produces a struct with start and end.

# withWatermark("event_time", "1 minute")
# → bounds watermark to limit state; here we allow up to 1 minute for late arrival and then can drop state.

# .groupBy(F.session_window(...), customer_id, region)
# → grouping key: session (start & end) + customer + region.

# Window.partitionBy("session_start","session_end","region").orderBy(desc("session_revenue"))
# → rank sessions per region for each session interval (session start & end identify the session).

# writeStream.format("console").outputMode("complete")
# → console sink prints full current aggregated table each trigger.



# 🧠 Real-Life Analogies

# Session window = shopping session at a mall. Every time a customer makes a purchase within 30s of the previous one, it’s the same shopping trip. If they come back after >30s, a new trip begins.

# Watermark = closing time for bookkeeping. After you decide a session is old enough, you close it and don’t accept late receipts for that past session.

# 🪞 Visual Imagination

# Picture a timeline per customer with little dots (events). Connect dots that are within 30s of each other → each connected cluster is a session. Each cluster becomes a session box with start & end timestamps; you compute revenue inside each box.




# 🧾 Summary Table
# Component	Purpose
# rate source	Simulate live events with controlled throughput
# session_window(..., gap)	Detect variable-length sessions per key based on inactivity gap
# withWatermark	Bound state growth and control late data handling
# groupBy(session, customer, region)	Aggregate per session & per customer
# Window.partitionBy(session_start, session_end, region)	Rank/Top‑N per session-window and region
# console + memory sinks	Real-time results + ad-hoc SQL access




# 🧪 Practice Questions + Answers

# Q1. What exactly is a session window?
# A1. It groups consecutive events into a session if each subsequent event occurs within a configured inactivity gap (e.g., 30s) from the previous event for that key.

# Q2. Why use withWatermark with session windows?
# A2. To limit how long Spark keeps state for open sessions; otherwise state grows unbounded.

# Q3. How would you change the script to measure sessions per user (customer) only, not per region?
# A3. Remove region from groupBy and ranking partition keys (groupBy(session, customer_id)).

# Q4. What are pitfalls of too‑small vs too‑large SESSION_GAP?
# A4. Too small: you may split a real user visit into multiple sessions. Too large: unrelated visits could be merged into a single session, and state stays open longer.




       
    
    