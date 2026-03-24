# Now we will create he exact runnable streaming script that performs session-window aggregation + session-level enrichment + per-session top-N and writes to a durable sink (with checkpointing)
#  (it will include checkpointLocation and option("path", ...) but will default to DRY mode)

# Key constraints honored:
# Safe by default: DRY_RUN = True — no files written unless you set it to False.

# Checkpointing included (for production runs) but only used when DRY_RUN = False.

# Console + Memory sinks used during DRY runs so we  can validate live results without persistence.

# 🔍 Short summary

# This script:
# Simlulates live event using Spark rate
# Enriches events via boradcasted static dims(customers/products)
# Build session windows per customer(SESSION_GAP), aggreagtes session metrics(events counts, units revenue)
# Computes Top-N sessions per region and per-session product mix(top products in session)
# By default prints result to console and memory(no files). If DRY_RUN = False,  it will persist session result to Parquet/Delta
# with a checkpoint for fault tolerance

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from faker import Faker
import random, os, time, sys
from pyspark import StorageLevel

# =========================================================
# FORCE SPARK_HOME to point ot the correct installation path
# =========================================================

os.environ['SPARK_HOME'] = r'D:\Uncodemy\Pyspark\pyspark_env\Lib\site-packages\pyspark'
os.environ['PYSPARK_PYTHON'] = r'D:\Uncodemy\Pyspark\pyspark_env\Scripts\python.exe'

sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python/lib/py4j-0.10.9.5-src.zip'))

# =========================================



# ---------------- CONFIG ----------------
DRY_RUN = True                      # True => no files written; False => will write outputs
OUTPUT_BASE = "out/project3_sessions"  # only used if DRY_RUN=False
CHECKPOINT_DIR = os.path.join(OUTPUT_BASE, "_checkpoints")  # checkpoint location for durability
ROWS_PER_SECOND = 12                # stream throughput
NUM_CUSTOMERS = 3000
NUM_PRODUCTS = 300
SESSION_GAP = "30 seconds"          # inactivity gap to close a session
WATERMARK = "2 minutes"             # watermark to bound late arrivals/state
TOP_N_SESSIONS = 5
TOP_PRODUCTS_PER_SESSION = 5
SHUFFLE_PARTITIONS = 8              # adjust for your machine

# ---------------- SPARK SESSION ----------------

spark = (
    SparkSession.builder
    .appName("Project3-SessionWindows-Full")
    .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
    .getOrCreate()
    
)

# nice banner
print(f"Spark {spark.version} | DRY_RUN={DRY_RUN} | OUTPUT_BASE={OUTPUT_BASE}")


# ---------------- STATIC DIMENSIONS (broadcast-friendly) ----------------
random.seed(42); Faker.seed(42)
fake = Faker()
regions = ["North", "South", "East", "West"]

# customers: (customer_id, customer_name, region)
customers_py = [(i, fake.name(), regions[i % len(regions)])for i in range(1, NUM_CUSTOMERS + 1)]
dim_customers = spark.createDataFrame(customers_py, schema="customer_id INT, customer_name STRING, region STRING")

# products: (product_id, product_name, base_price)
products_py = []
for pid in range(1, NUM_PRODUCTS + 1):
    pname = f"Product-{pid:04d}"
    price = round(random.uniform(5.0, 1500.0), 2)
    products_py.append((pid, pname, float(price)))
dim_products = spark.createDataFrame(products_py, schema="product_id INT, product_name STRING, base_price DOUBLE")


# ---------------- STREAM SOURCE: rate (no files) ----------------
ticks = (
    spark.readStream
         .format("rate")
         .option("rowsPerSecond", ROWS_PER_SECOND)
         .load()
)


# map ticks to events
events = (
    ticks
    .withColumn("event_time", F.col("timestamp"))
    .withColumn("customer_id", (F.col("value") % NUM_CUSTOMERS) + 1)
    .withColumn("product_id",  (F.col("value") % NUM_PRODUCTS) + 1)
    .withColumn("quantity",    (F.floor(F.rand()*5) + 1).cast("int"))
    .select("event_time", "customer_id" ,"product_id", "quantity")
    
)


# ---------------- ENRICH EVENTS (broadcast joins) ----------------
stream_enriched = (
    events
    .join(F.broadcast(dim_customers.select("customer_id","region")), on="customer_id", how="left")
    .join(F.broadcast(dim_products.select("product_id","product_name","base_price")), on="product_id", how="left")
    .withColumn("unit_price", F.coalesce(F.col("base_price"), F.lit(0.0)))
    .withColumn("total_amount", F.col("unit_price") * F.col("quantity"))
    .select("event_time","customer_id","region","product_id","product_name","quantity","unit_price","total_amount")
)


# ---------- SESSION AGGREGATION ----------------
# 1) compute sessions per customer
session_agg = (
    stream_enriched
    .withWatermark("event_time", WATERMARK)
    .groupBy(
        F.session_window("event_time", SESSION_GAP).alias("session"),
        F.col("customer_id"),
        F.col("region")
    )
    .agg(
        F.count("*").alias("events_count"),
        F.sum("quantity").alias("units"),
        F.round(F.sum("total_amount"),2).alias("session_revenue")
    )
    .select(
        F.col("session").start.alias("session_start"),
        F.col("session").end.alias("session_end"),
        "customer_id","region","events_count","units","session_revenue"
    )
)


# 2) Top-N sessions per region (rank sessions within (session_start,session_end) and region)
rank_win =  Window.partitionBy("session_start","session_end","region").orderBy(F.desc("session_revenue"), F.desc("units"))
top_sessions = (
    session_agg
    .withColumn("rank", F.dense_rank().over(rank_win))
    .filter(F.col("rank") <= F.lit(TOP_N_SESSIONS))
    .orderBy("session_start", "region", "rank")
)

# ---------------- PER-SESSION PRODUCT MIX ----------------

# 1) compute product-level aggregates grouped by session (we need product totals inside each session
prod_by_session = (
    stream_enriched
    .withWatermark("event_time", WATERMARK)
    .groupBy(
        F.session_window("event_time", SESSION_GAP).alias("session"),
        F.col("customer_id"),
        F.col("region"),
        F.col("product_id"),
        F.col("product_name")
        
    )
    .agg(
        F.sum("quantity").alias("product_units"),
        F.round(F.sum("total_amount"),2).alias("product_revenue")
    )
    .select(
        F.col("session").start.alias("session_start"),
        F.col("session").end.alias("session_end"),
        "customer_id","region","product_id","product_name","product_units","product_revenue"
    )
)


# 2) For each session produce an ordered list of top products
#   approach: collect list of structs (revenue, product_name, units), sort and slice top K
prod_struct = F.struct(F.col("product_revenue").alias("r"), F.col("product_units").alias("u"), F.col("product_name").alias("n"))
prod_grouped_lists = (
    prod_by_session
    .groupBy("session_start", "session_end", "customer_id", "region")
    .agg(F.collect_list(prod_struct).alias("prod_list"))
    .withColumn("prod_list_sorted", F.expr("array_sort(prod_list, (left, right) -> case when left.r = right.r then right.u - left.u else right.r - left.r end )"))
    .withColumn("top_products", F.expr(f"slice(prod_list_sorted, 1, {TOP_PRODUCTS_PER_SESSION})"))
    .select("session_start","session_end","customer_id","region","top_products")
)

# ---------------- OUTPUTS & SINKS ----------------
# Two outputs:
#  A) top_sessions (session KPI leaderboard) -> console + memory
#  B) prod_grouped_lists (per-session top products) -> console + memory

# Select columns for printing
top_sessions_select = top_sessions.select("session_start","session_end","region","customer_id","rank","session_revenue","units","events_count")
prod_lists_select = prod_grouped_lists.select("session_start","session_end","region","customer_id","top_products")

# Console & memory sinks (safe default)
q1_console = (
    top_sessions_select.writeStream
    .format("console")
    .outputMode("complete")
    .option("truncate", "false")
    .option("numRows", "50")
    .trigger(processingTime="5 seconds")
    .start()
)

q1_memory = (
    top_sessions_select.writeStream
    .format("memory")
    .queryName("top_sessions_live")
    .outputMode("complete")
    .trigger(processingTime="5 seconds")
    .start()
)

q2_console = (
    prod_lists_select.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", "false")
      .option("numRows", "50")
      .trigger(processingTime="5 seconds")
      .start()
)

q2_memory = (
    prod_lists_select.writeStream
      .format("memory")
      .queryName("top_products_session_live")
      .outputMode("complete")
      .trigger(processingTime="5 seconds")
      .start()
)

# ---------------- OPTIONAL PERSISTENCE (when DRY_RUN=False) ----------------
if not DRY_RUN:
    # ensure checkpoint dir exists
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    
    #  persist top sessions to parquet (with checkpointing)
    q1_persist = (
        top_sessions_select.writeStream
        .format("parquet")
        .option("path", os.path.join(OUTPUT_BASE, "top_sessions"))
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "top_sessions"))
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )
    
    # persist per-session product lists as parquet (serialize top_products as JSON string)
    prod_for_write = prod_lists_select.withColumn("top_products_json", F.to_json("top_products")).select("session_start","session_end","region","customer_id","top_products_json")
    q2_persist = (
        prod_for_write.writeStream
          .format("parquet")
          .option("path", os.path.join(OUTPUT_BASE, "top_products_sessions"))
          .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "top_products_sessions"))
          .outputMode("append")
          .trigger(processingTime="10 seconds")
          .start()
    )
    
# ---------------- RUN & WAIT ----------------
print("Streaming started. Console + memory sinks active. DRY_RUN =", DRY_RUN)
try:
    # block until user interrupts
    q1_console.awaitTermination()
except KeyboardInterrupt:
    print("Stopping streams...")
finally:
    # stop any persistent queries
    for q in spark.streams.active:
        try:
            q.stop()
        except Exception:
            pass
    spark.stop()
    print("Stopped.")


# 🔍 Why this script (detailed)

# Session windows compute per-customer sessions based on SESSION_GAP. This gives realistic shopping 
# sessions.

# Broadcast joins for tiny dims (customers/products) avoid shuffles and make enrichment cheap.

# prod_grouped_lists aggregates product-level totals inside each session and produces a sorted 
# top-products list for each session (great for basket analysis).

# Memory + console sinks let you validate results without writing. If you’re ready for persistence, 
# flip DRY_RUN=False and the script will write Parquet and create checkpoints so stateful streaming 
# recovers after failures. 


# 🧱 What you did (plain words)

# Created a stream of simulated purchases.

# Enriched with region & product info.

# Grouped events into sessions based on inactivity gap.

# Calculated session KPIs and ranked sessions per region.

# Produced per-session product top lists.

# Exposed both session KPIs and session product lists to console and memory for real-time exploration.

# Optional persistent writes with checkpointing when you set DRY_RUN=False.


# 🔁 Full syntax breakdown (key constructs)

# readStream.format("rate") — generates synthetic stream (timestamp, value).

# withWatermark("event_time", WATERMARK) — controls allowed lateness and state retention.

# session_window("event_time", SESSION_GAP) — builds a session struct {start,end} per key when grouping.

# groupBy(session, key) — aggregate per session.

# collect_list(struct(...)), array_sort(...), slice(...) — produce ordered top-k lists inside aggregation result.

# writeStream.format("memory").queryName("...") — exposes results as a temporary table you can query with spark.sql(...).

# writeStream.format("parquet").option("checkpointLocation", ...) — durable write for production.


# 🧠 Real-life analogy

# Sessions = shopping trips.

# prod_grouped_lists = the shopper’s basket summary at checkout (top items by spend).

# Checkpointing = the store’s ledger that ensures you don’t lose session records if the cash register restarts.

# 🪞 Visual imagination

# Each customer timeline has clustered events; a box forms around each cluster (session).

# Inside each box we compute totals and list items by descending spend.

# A live dashboard refreshes every few seconds showing top sessions and their basket contents.


# 🧾 Quick summary table
# Component	Purpose
# rate source	Fast local stream for testing
# Broadcast dims	Cheap enrichments
# session_window	Detect user sessions by inactivity gap
# prod_grouped_lists	Per-session product mix (top-K)
# memory sink	Ad-hoc query + debug
# parquet + checkpoint	Durable persistence & recovery (when DRY_RUN=False)  


# Practice & Interview Questions (session + enrichment + top-products)

# Q1. Why do we use withWatermark when doing session windows?
# A1. To bound state lifetime and allow Spark to drop old sessions — prevents unbounded memory growth and enables finalizing sessions after watermark passes.

# Q2. Why collect product lists then sort client-side (using array_sort) instead of ranking rows and joining?
# A2. For per-session basket summaries, collecting a compact list (TOP_PRODUCTS_PER_SESSION) and sorting is efficient and produces a compact payload for reporting. Ranking every product row can be heavier.

# Q3. What are the trade-offs when switching DRY_RUN to False to persist outputs?
# A3. + Durable results and recovery. − Need disk space, longer startup (checkpoint replay), must keep checkpoint directory intact; performance depends on I/O.

# Q4. How to debug session merging or late-arriving events?
# A4. Use Spark UI Streaming tab to inspect state rows and metrics. Inject controlled late events and watch whether session intervals merge; adjust watermark or session gap accordingly.