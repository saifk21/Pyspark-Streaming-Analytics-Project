# Project 3 isnt done yet We’ve only covered tumbling windows and sliding windows so far.
# Still to go before it’s complete:

# Remaining steps in Project 3:
# Session windows theory → how Spark detects dynamic, bursty sessions.

# Session windows code → live aggregation per user/session.

# Enrichment joins in streaming → merging with static dimension tables.

# Advanced per-session metrics → top N items, avg spend, total items.

# Optional watermarks → handling late data in streaming.

# Final review & optimization → checkpointing, trigger settings, fault tolerance.



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



# 🔎 Step 1 — Session Window Theory
# Session windows let you group events that belong to the same user interaction when the interaction 
# length is variable and unknown ahead of time.They’re used for analytics like:

# shopping sessions (how long, how much revenue per session)

# user visit sessions on a website

# multi-event transactions within a short time gap

# Why not tumbling/sliding windows?

# Tumbling / sliding windows use fixed time buckets — good for continuous metrics 
# (per-minute, per-5s), but they can split a user’s real session across bucket boundaries.

# Session windows produce natural user sessions (gaps define breaks) — better for session metrics
# (session length, basket composition).


# 🧱 👨🏫 Theory, plain words

# A session is a sequence of events for the same key (e.g., customer_id) where the time between 
# successive events is ≤ SESSION_GAP.

# If two events for the same customer are more than SESSION_GAP apart, they belong to two different 
# sessions.

# Spark’s session_window(event_time, gap) creates a struct column with start and end for each session.

#  groupBy(session_window(...), key) and aggregate per session.



# 🧾 Key Concepts & Semantics
# Concept	Meaning
# Session gap	Max inactivity allowed between consecutive events in a session (e.g., 30 seconds).
# Session window	Dynamic window defined by consecutive events — represented as {start, end}.
# Watermark	Tells Spark how long to wait for late events before finalizing a session and dropping state.
# Stateful operator	Session windows require Spark to store partial session state until the session closes or watermark passes.
# Session merge	If late events arrive bridging two sessions (within gap), Spark may merge them.


# 🔁 Full Syntax Breakdown (Spark functions)

# F.session_window(timeCol, gap)

# timeCol: a timestamp column (event time).

# gap: a duration string like "30 seconds" or "5 minutes".

# Returns a column of type struct<start:timestamp,end:timestamp>.

# withWatermark("timeCol", "duration")

# Sets the allowed lateness for event time. Retains state until watermark passes.

# Example groupBy:

spark = SparkSession.builder.appName("SessionWindowTheory").getOrCreate()

df = spark.readStream.format("...").load("...")

df.groupBy(F.session_window("event_time", "30 seconds"), F.col("customer_id")).agg(F.count("*").alias("events"), F.sum("amount").alias("revenue"))

# Ranking with sessions:
from pyspark.sql.window import Window
win = Window.partitionBy("session_start", "session_end", "region").orderBy(F.desc("revenue"))
df_with_rank = df.withColumn("rank", F.dense_rank().over(win))


# 🧠 Real-Life Analogy (Required)

# Imagine a customer walking through a supermarket.

# They pick items; each pick is an event.

# If they pause for more than 30 seconds and then pick another item, the store may treat it as 
# a new visit (a new session).

# Session windows are like marking a shopping trip box around consecutive picks.

# Watermark = the cashier’s rule: “we’ll accept returns/updates for 1 minute after the checkout; 
# after that the receipt is closed.”

# 🪞 Visual Imagination

# Draw a horizontal time axis per customer:

# Small dots = events at different timestamps.

# Connect consecutive dots if gaps ≤ SESSION_GAP → shaded region = session.

# Each shaded region shows session_start and session_end.
# Repeat per customer; sessions vary in length.


# ✅ State, Watermark & Merging — Deep Dive
# State

# Session windows require state because Spark must keep partial windows open until it can safely 
# finalize them.

# State size grows with number of active sessions (open but not yet finalized).

# Always monitor the Storage/State size in Spark UI.

# Watermark

# Watermark bounds state growth. Example:

# withWatermark("event_time", "1 minute") → spark will keep session state until the maximum event_time
# observed advances beyond session_end + watermark.

# Watermark must be used with event-time aggregations. Without it state can grow unbounded.

# Merging sessions

# If events come late and bridge two sessions 
# (i.e., late event falls between two sessions and reduces gap below threshold),
# Spark will merge sessions into a larger one, and re-compute aggregation for the merged session.

# Merging is an expensive operation — minimize late arrivals to avoid frequent merges.



# ⚠️ Pitfalls & Best Practices

# Too short SESSION_GAP → splits logical user visits into multiple sessions (bad UX metrics).
# Choose gap based on domain (web: 30m common; shopping quick sessions might be 30s-2min).

# Too long SESSION_GAP → merges unrelated visits, increases state time & memory.

# Insufficient watermark → too small watermark will cause legitimate late events to be dropped; 
# too large watermark 
# increases state retention.

# High throughput + many keys → state explosion.

# Mitigate with: more partitions, increase executors/memory, use watermark aggressively, filter noise
# early.

# Checkpoints are essential — session windows use state; always configure checkpointing for 
# exactly-once and state recovery.



# 🛠 Checkpointing & Fault Tolerance (Practical)

# Why: session state is stored in memory/disk; on failure you must restore it. Checkpointing is 
# mandatory for stateful streaming for recovery.

# How:
query = df.writeStream \
    .format("parquet") \
    .option("path", "/tmp/out") \
    .option("checkpointLocation", "/tmp/checkpoint/session1") \
    .start()
    

# For memory or console sinks we can still set checkpointLocation, but memory/console not 
# durable — for production use file-based/wal sinks with checkpointing.

# Note: Even if we only use console for testing, practice providing a checkpointLocation in our
# pipeline’s writeStream in scripts intended for real runs.



# 🔧 Tuning Guidelines

# State size: monitor via Spark UI — Streaming tab → State. If big:

# Increase watermark, compact sessions (longer gaps), filter low-value events earlier.

# Parallelism: increase spark.sql.shuffle.partitions or spark.default.parallelism.

# Executor memory: allocate enough memory to hold active session state.

# Partition key: partition by customer_id (or hash) to spread sessions evenly.


# ✅ Minimal Working Example (Session Window) — Safe snippet (no file writes)
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType, StringType

spark = SparkSession.builder.master("local[*]").appName("SessionWindowExample").getOrCreate()

# Example static DataFrame as if it were a small batch of events (demo only)
schema = StructType([
    StructField("event_time", TimestampType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("region", StringType(), True)
])

# 3. Read stream from Kafka (replace with your broker/topic)
raw_stream = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", "events_topic")
         .load()
)

# 4. Parse Kafka value (JSON → structured columns)
events = (
    raw_stream
    .selectExpr("CAST(value AS STRING)")
    .select(F.from_json(F.col("value"), schema).alias("data"))
    .select("data.*")
)

# 5. Session agregation with watermark
session_gap = "30 seconds"
session_agg = (
    events
    .withWatermark("event_time", "1 minute")
    .groupBy(
        F.session_window("event_time", session_gap),
        F.col("customer_id"),
        F.col("region")
    )
    .agg(
        F.count("*").alias("event_count"),
        F.sum("amount").alias("session_revenue")
    )
)


# 6. Write results to console sink with checkpointing
query = (
    session_agg
    .writeStream
    .format("console")
    .option("checkpointLocation", "/tmp/checkpoint/session1")
    .outputMode("update")   # or "complete"
    .start()
)

query.awaitTermination()


# Defined events properly by parsing Kafka JSON.

# Added writeStream sink with checkpointLocation so the query actually runs.

# Included .awaitTermination() to keep the stream alive

# 👉 This is the full working version you can use. Just adjust:

#     kafka.bootstrap.servers to your Kafka broker address.

#     subscribe to your topic name.

#     checkpointLocation to a valid folder path on your system.



# 🧪 Practice Exercises (with answers)

# Exercise 1: Choose a session gap and compute average session duration per region.

# Hint: use session_window(...).start and .end to compute duration 
# unix_timestamp(end) - unix_timestamp(start); then average per region.


# sess = sessionized_df.select(
#     F.col("session").start.alias("start"),
#     F.col("session").end.alias("end"),
#     "region"
# ).withColumn("duration_s", F.unix_timestamp("end") - F.unix_timestamp("start"))
# avg_duration = sess.groupBy("region").agg(F.avg("duration_s").alias("avg_session_s"))



# Exercise 2: Late events arrive causing two sessions to merge — how will Spark handle it?
# Answer: Spark will merge session state for overlapping sessions when a late event bridges them
# (if still inside watermark). Aggregations for merged session are recomputed and a new output 
# reflecting the merged results is emitted. This can be expensive.

# Exercise 3: How to make session windows resilient to node failure?
# Answer: Use checkpointLocation in writeStream (persisted storage), ensure the cluster filesystem
# is durable (HDFS/S3), and enable recovery; test by killing the driver/executor and verifying state restores.

# 🧾 Interview Questions (session window focused)

# Q1. What’s the difference between session windows and tumbling windows?
# A1. Tumbling windows are fixed, non-overlapping time buckets; session windows are dynamic per key 
# and defined by inactivity gaps between events.


# Q2. Why do session windows require stateful processing?
# A2. Because Spark must keep partial session aggregates (open sessions) until they are finalized
# (no more events within gap), which involves storing state across micro-batches.

# Q3. How do watermarks interact with session windows?
# A3. Watermarks limit how long state is retained for sessions; Spark finalizes session when watermark 
# surpasses session end and can drop state.

# Q4. What causes session merging and why is it expensive?
# A4. Late events that land between two previously separate sessions (within gap) cause Spark to merge 
# them into one session and recompute aggregates; it’s expensive due to state merging and reprocessing.



# ✅ Quick Checklist to Implement Session Windows in Production

# Use event time — ensure events have timestamp column (not processing time).

# Set a realistic session gap per domain.

# Always use withWatermark(...).

# Configure checkpointLocation for durability.

# Monitor Spark UI (state/numRows/shuffle) and tune partitions.

# Test with late events to ensure correctness and understand merge behavior.