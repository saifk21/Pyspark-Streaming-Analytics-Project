# This project will be different from the batch-processing projects
# Instead of reading a static dataset, we’ll process live incoming data 
# (e.g., simulated sensor or sales events).

# We’ll see how Spark processes data in micro-batches and updates results continuously.

# 🎯 Objective:
# Build a streaming pipeline that:

# Reads incoming sales events in real-time (simulated with Faker).

# Processes and aggregates sales by product and region every few seconds.

# Outputs live metrics to the console (or a dashboard).

# 🛠 Tools & Tech:

# PySpark Structured Streaming (core framework)

# Faker (to simulate real-time events)

# Parquet (optional checkpointing & storage)

# Console Sink for live viewing


# 📂 Step-by-Step Methodology:

# 📦 Create a Fake Live Sales Data Generator

# Will output JSON lines to a folder (acts as our “stream source”).

# New files appear every few seconds.

# 📥 Read Streaming Data with Spark

# Use spark.readStream.schema(...).json(path)

# Infer schema beforehand for better performance.

# 🧮 Transform & Aggregate in Real-Time

# Group by product & region.

# Calculate total sales per time window.

# Use watermarking to handle late data.

# 📤 Output Results to Console

# writeStream.outputMode("complete") for aggregates.

# Update every 5 seconds.

# 🗄 Optional: Store Processed Stream in Parquet

# For historical queries after the stream ends.

# 🛑 Stop Stream Gracefully

# Ensure no partial writes remain.



# 💡 Key Learning Points Covered:

# Spark Structured Streaming fundamentals.

# Schema-on-read for streams.

# Micro-batch aggregation & watermarking.

# Writing streaming results to sinks (console, files).


# We’ll not create any files and we’ll not write to disk. We’ll use Spark’s in‑memory rate source to
# simulate a live stream and join it with small static dimensions generated with Faker. Output goes 
# to the console and memory sink only.


# Architecture :

# Streaming Source: rate (rows/second tick stream) → turned into sales events.

# Static Dims: small in‑memory customers & products with Faker (broadcast joins).

# Enrichment: quantity, unit_price, total_amount.

# Aggregation: 10‑second tumbling windows, watermark 30s, grouped by region + category.

# Sinks: Console (live view), Memory (ad‑hoc SQL). No files written.

# Visual: imagine a ticker producing IDs every second → we attach labels from tiny lookup tables 
# (customers/products) → bucket by 10‑second windows → live totals show up on the console.


# 1) Spark Session & Config
# 🔍 📘 Why?

# Create the engine that runs streaming queries. Limit shuffle partitions for  laptop and keep 
# everything local.

# 🧱 👨🏫 What I Did

# Started a local Spark with fewer shuffle partitions (8).

# Imported all functions I’ll need.


# --- 1) Spark session & imports ---
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from faker import Faker
import random
import sys
import os


# =========================================================
# FORCE SPARK_HOME to point ot the the correct installation path
# This is the definitive fix for the 'JavaPackage' error
# =========================================================

# The path should be up to the 'pyspark' folder within site-packages
os.environ['SPARK_HOME'] = r'D:\Uncodemy\Pyspark\pyspark_env\Lib\site-packages\pyspark'

# Set the Python interpreter for PySpark
os.environ['PYSPARK_PYTHON'] = r'D:\Uncodemy\Pyspark\pyspark_env\Scripts\python.exe'

# Ensure the Spark Libraries are on the path
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python/lib/py4j-0.10.9.5-src.zip'))

spark = (
    SparkSession.builder
    .appName("Project3-StructuredStreaming")
    .master("local[*]")                         # use all local cores
    .config("spark.sql.shuffle.partitions", "8")# fewer partitions for laptop
    .getOrCreate()
)


# 🔁 Syntax Breakdown

# SparkSession.builder...getOrCreate() → starts (or returns existing) Spark session.

# .master("local[*]") → run locally with all cores.

# .config("spark.sql.shuffle.partitions","8") → reduce default 200 shuffle partitions.


# 🪞 Visual

# A control panel: big button “Start Spark”, checkbox “use all cores”, slider “partitions = 8”.

# 2) Build Tiny Static Dimensions (in memory, no files)
# 🔍 📘 Why?

# I need regions, categories, and prices to make the fake stream look real. I’ll broadcast these 
# tiny tables to avoid shuffles.


# 🧱 👨🏫 What I Did

# Created 5k fake customers with region + gender.

# Created 200 products with category, subcategory, price.

# Turned Python lists into Spark DataFrames.



# --- 2) Static dimensions (small, broadcast-friendly) ---
fake = Faker()
random.seed(42); Faker.seed(42)

NUM_CUSTOMERS = 5000
NUM_PRODUCTS = 200

regions  = ["North", "South", "East", "West"]
genders  = ["Male", "Female", "Other"]
cats     = ["Electronics", "Clothing", "Home", "Sports", "Toys"]
subcats  = {
    "Electronics": ["Phone","Laptop","Headphones","Camera","Monitor"],
    "Clothing":    ["Shirt","Jeans","Jacket","Shoes","Hoodie"],
    "Home":        ["Blender","Cookware","Lamp","Desk","Chair"],
    "Sports":      ["Ball","Gloves","Shoes","Watch","Mat"],
    "Toys":        ["Doll","Blocks","Car","Puzzle","BoardGame"]
}

# Customers: (customer_id, customer_name, region, gender)
customers_py = [
    (i, fake.name(), regions[i % len(regions)], genders[i % len(genders)])
    for i in range(1, NUM_CUSTOMERS + 1)
]

dim_customers = spark.createDataFrame(
    customers_py,
    schema="customer_id INT, customer_name STRING, region STRING, gender STRING"
)



# Products: (product_id, category, subcategory, product_name, base_price)
products_py = []
for pid in range(1, NUM_PRODUCTS + 1):
    cat = random.choice(cats)
    sub = random.choice(subcats[cat])
    name = f"{cat}-{sub}-{pid:04d}"
    price = round(random.uniform(5 ,1500), 2)
    products_py.append((pid, cat, sub, name, float(price)))
    
    
dim_products = spark.createDataFrame(
    products_py,
    schema="product_id INT, category STRING, subcategory STRING, product_name STRING, base_price DOUBLE"
) 

# Quick peek (safe, not required)
dim_customers.show(3, truncate=False)
dim_products.show(3, truncate=False) 


# Streaming Source (no files, pure in‑memory)
# 🔍 📘 Why?

# We simulate an infinite stream without writing any files using Spark’s rate source.

# 🧱 👨🏫 What I Did

# Created a stream that produces rowsPerSecond ticks with columns timestamp and value.

# Turned each tick into a sale event with customer, product, quantity.  

# --- 3) Live stream via rate source (no files created) ---
ROWS_PER_SECOND = 8  # adjust for faster/slower demo

# Base stream with columns: timestamp, value (0,1,2,...)
ticks = (
    spark.readStream
         .format("rate")
         .option("rowsPerSecond", ROWS_PER_SECOND)
         .load()
)

# Convert ticks to sales-like events
events = (
    ticks
    .withColumn("event_time", F.col("timestamp"))                        # rename for clarity
    .withColumn("customer_id", (F.col("value") % NUM_CUSTOMERS) + 1)     # map tick to customer
    .withColumn("product_id",  (F.col("value") % NUM_PRODUCTS) + 1)      # map tick to product
    .withColumn("quantity",    (F.floor(F.rand()*5) + 1).cast("int"))    # 1..5 random qty
    .select("event_time","customer_id","product_id","quantity")
)



# 🔁 Syntax Breakdown

# readStream.format("rate").load() → infinite generator.

# rand() → Spark SQL random per row.

# floor(rand()*5)+1 → integers 1..5.


# 🧠 Analogy

# A metronome ticks 8 times a second; each tick becomes one purchase.

# 🪞 Visual

# A conveyor belt dropping identical tokens (value), which we paint into "orders" by attaching IDs
# and quantities.


# Enrich Stream with Dims (broadcast joins)
# 🔍 📘 Why?

# Add region, category, price to the raw stream so you can compute revenue.

# 🧱 👨🏫 What You Did

# Broadcast tiny dims → avoids shuffles.

# Computed unit_price and total_amount


# --- 4) Enrichment: add region/category/price via broadcast joins ---
stream_enriched = (
    events
    .join(F.broadcast(dim_customers.select("customer_id", "region", "gender")),
          on="customer_id", how="left")
    .join(F.broadcast(dim_products.select("product_id","category","subcategory","product_name","base_price")),
          on="product_id", how="left")
    .withColumn("unit_price", F.coalesce(F.col("base_price"), F.lit(0.0)))   # safe default
    .withColumn("total_amount", F.col("unit_price") * F.col("quantity"))              # revenue
    .select("event_time","customer_id","product_id","region","category",
            "product_name","quantity","unit_price","total_amount")
)


# 🔁 Syntax Breakdown

# broadcast(df) → send small DF to all executors to avoid shuffle join.

# coalesce(a,b) → if a is NULL use b.

# 🧠 Analogy

# Broadcast = laminating the small reference sheets and handing a copy to each worker so nobody has to 
# walk to the warehouse (shuffle).

# 🪞 Visual

# Each incoming order gets instantly stamped with region and category from the small lookup cards.



# Windowed Aggregations + Watermark
# 🔍 📘 Why?

# Compute rolling KPIs in 10‑second windows and drop late data > 30 seconds.

# 🧱 👨🏫 What You Did

# Applied a watermark on event_time (tells Spark how long to wait for late data).

# Grouped by window + region + category.

# Summed revenue and units.



# --- 5) Windowed aggregations with watermark ---
agg_by_region_category = (
    stream_enriched
    .withWatermark("event_time", "30 seconds")  # handle out-of-order events up to 30s late
    .groupBy(
        F.window("event_time", "10 seconds"),   # tumbling window of 10s
        F.col("region"),
        F.col("category")
    )
    .agg(
        F.sum("total_amount").alias("revenue"),
        F.sum("quantity").alias("units")
    )
    .orderBy(F.col("window").start, "region", "category")
)


# 🔁 Syntax Breakdown

# withWatermark(timeCol, "30 seconds") → set lateness threshold.

# window(col, "10 seconds") → bucket events into 10‑second intervals.

# 🧠 Analogy

# Watermark = grace period; we wait a short time for stragglers, then finalize the window.

# 🪞 Visual

# Buckets on a timeline every 10s; late marbles can still fall into a bucket until the 30s gate shuts.



# Sinks: Console (live) + Memory (ad‑hoc SQL) — no files
# 🔍 📘 Why?

# See live metrics in your terminal and optionally run SQL queries against the in‑memory result.

# 🧱 👨🏫 What You Did

# Console sink prints updated aggregates every 5 seconds.

# Memory sink registers a temporary table you can query.



# --- 6) Start the streaming queries (no files) ---
# --- 6) Start the streaming queries (no files) ---
# Console sink: prints live aggregates
q_console = (
    agg_by_region_category.writeStream
    .format("console")
    .outputMode("complete")                 # reprint full table each trigger
    .option("truncate", "false")
    .option("numRows", "40")
    .trigger(processingTime="5 seconds")    # micro-batch every 5s
    .start()
)

# Memory sink: enables SQL queries like spark.sql("SELECT ... FROM agg_live")
q_memory = (
    agg_by_region_category.writeStream
    .format("memory")
    .queryName("agg_live")                  # table name inside Spark
    .outputMode("complete")
    .trigger(processingTime="5 seconds")
    .start()
)

# Block the driver so the streams keep running (stop with Ctrl+C)
q_console.awaitTermination()


# 🔁 Syntax Breakdown

# writeStream.format("console") → print to terminal.

# outputMode("complete") → show whole result table each trigger (good for aggregates).

# trigger(processingTime="5 seconds") → micro‑batch interval.

# 🧠 Analogy

# Think scoreboard that updates every 5 seconds, and a behind‑the‑scenes table you can query anytime.

# 🪞 Visual

# Terminals: left shows live numbers; right lets you run ad‑hoc SELECT * FROM agg_live.



# 🧾 Summary Table
# Piece	                                   What it does	                         Why it matters
# rate source	                        Generates stream ticks	             No files, easy local demo
# Broadcast joins	                    Add region/category/price	         Avoids shuffles, faster
# Watermark (30s)	                    Late data handling	                 Correctness in streams
# 10s tumbling window	                Time bucketing	                     Real‑time KPIs
# Console sink	                        Live monitoring	                     Instant feedback
# Memory sink	                        Ad‑hoc SQL	                         Debug & quick insights
# 🧠 Practice Questions (with Answers)

# Q1. Why use the rate source instead of writing Faker events to files?
# A1. It avoids disk I/O and respects your constraint of no file creation. It’s built‑in, fast, and perfect for local demos.

# Q2. What’s the difference between withWatermark("event_time", "30 seconds") and the window("event_time","10 seconds")?
# A2. window defines the bucket size; watermark defines lateness tolerance before Spark finalizes a bucket.

# Q3. Why broadcast the small dimension tables?
# A3. To avoid shuffling the streaming fact rows when joining. Broadcast copies the dim to all tasks → map‑side join.

# Q4. When would you use append vs complete output mode?
# A4. append for append‑only results (e.g., per‑event logs). complete for aggregations where the whole table changes each trigger.

# Q5. How can you scale this to real sources?
# A5. Replace rate with Kafka/Socket/File sources and keep the same enrichment → watermark → window → sinks pattern.




