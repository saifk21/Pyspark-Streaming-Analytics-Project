# Kafka Version: level‑up Project 3 with stateful, real‑time Top‑N analytics using Structured Streaming.

# 🔥 Goal 

# Compute Top‑N products per region continuously, in a time window, from the live stream I already have.

# Window: 1‑minute tumbling 

# Watermark: 30s (tolerate late events).

# Rank: dense_rank() per (window, region).

# Sink: console (live table) + memory (SQL-able).

# Still no disk I/O

# --- Project 3: Real-time Top-N per Region (no files, console + memory only) ---
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from faker import Faker
import random
import os
import sys

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


# --- Project 3: Real-time Top-N per Region (no files, console + memory only) ---

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from faker import Faker
import random

# ---------------- 1) Spark Session ----------------
spark = (
    SparkSession.builder
    .appName("Project3-TopN-Streaming")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

# ---------------- 2) Tiny Static Dimensions ----------------
fake = Faker()
random.seed(42); Faker.seed(42)

NUM_CUSTOMERS = 5000
NUM_PRODUCTS  = 200
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

customers_py = [
    (i, fake.name(), regions[i % len(regions)], genders[i % len(genders)])
    for i in range(1, NUM_CUSTOMERS + 1)
]
dim_customers = spark.createDataFrame(
    customers_py,
    schema="customer_id INT, customer_name STRING, region STRING, gender STRING"
)

products_py = []
for pid in range(1, NUM_PRODUCTS + 1):
    cat = random.choice(cats)
    sub = random.choice(subcats[cat])
    name = f"{cat}-{sub}-{pid:04d}"
    price = round(random.uniform(5, 1500), 2)
    products_py.append((pid, cat, sub, name, float(price)))
dim_products = spark.createDataFrame(
    products_py,
    schema="product_id INT, category STRING, subcategory STRING, product_name STRING, base_price DOUBLE"
)

# ---------------- 3) Live Stream via rate source ----------------
ROWS_PER_SECOND = 10  # adjust throughput

ticks = (
    spark.readStream
         .format("rate")
         .option("rowsPerSecond", ROWS_PER_SECOND)
         .load()
)

events = (
    ticks
    .withColumn("event_time", F.col("timestamp"))
    .withColumn("customer_id", (F.col("value") % NUM_CUSTOMERS) + 1)
    .withColumn("product_id",  (F.col("value") % NUM_PRODUCTS) + 1)
    .withColumn("quantity",    (F.floor(F.rand()*5) + 1).cast("int"))  # 1..5
    .select("event_time","customer_id","product_id","quantity")
)

# ---------------- 4) Enrich with dims (broadcast joins) ----------------
stream_enriched = (
    events
    .join(F.broadcast(dim_customers.select("customer_id","region")),
          on="customer_id", how="left")
    .join(F.broadcast(dim_products.select("product_id","category","product_name","base_price")),
          on="product_id", how="left")
    .withColumn("unit_price",   F.coalesce(F.col("base_price"), F.lit(0.0)))
    .withColumn("total_amount", F.col("unit_price") * F.col("quantity"))
    .select("event_time","region","category","product_id","product_name",
            "quantity","unit_price","total_amount")
)

# ---------------- 5) Windowed agg + Top-N per (window, region) ----------------
WINDOW_SIZE = "1 minute"   # tumbling; change to "1 minute", "10 seconds" for sliding
WATERMARK   = "30 seconds"
TOP_N       = 5

# 5.1 Aggregate revenue per product within window & region
prod_agg = (
    stream_enriched
    .withWatermark("event_time", WATERMARK)
    .groupBy(
        F.window("event_time", WINDOW_SIZE),   # tumbling window
        F.col("region"),
        F.col("product_id"),
        F.col("product_name"),
        F.col("category")
    )
    .agg(
        F.sum("total_amount").alias("revenue"),
        F.sum("quantity").alias("units")
    )
)

# 5.2 Rank products per (window, region) by revenue and keep Top-N
rank_win = Window.partitionBy("window", "region").orderBy(F.desc("revenue"), F.desc("units"))
topn = (
    prod_agg
    .withColumn("rank", F.dense_rank().over(rank_win))
    .filter(F.col("rank") <= F.lit(TOP_N))
    .orderBy(F.col("window").start, "region", "rank")
)

# ---------------- 6) Sinks: Console + Memory (no files) ----------------
q_console = (
    topn.writeStream
        .format("console")
        .outputMode("complete")                # full table each trigger (aggregations)
        .option("truncate", "false")
        .option("numRows", "60")
        .trigger(processingTime="5 seconds")   # micro-batch interval
        .start()
)

q_memory = (
    topn.writeStream
        .format("memory")
        .queryName("topn_live")                # in-memory table for ad-hoc SQL
        .outputMode("complete")
        .trigger(processingTime="5 seconds")
        .start()
)

# Block the driver so stream keeps running; stop with Ctrl+C
q_console.awaitTermination()



# 🔍 📘 Why?

# We’re continuously deciding the Top‑5 products per region in 1‑minute windows.

# This mirrors real dashboards: “What are my top sellers right now in each region?”


# 🧱 👨🏫 What I Did

# Took the enriched stream.

# Aggregated by (window, region, product) → revenue, units.

# Ranked within each (window, region) using dense_rank.

# Kept rank <= 5, wrote to console and memory.


# 🔁 Syntax Breakdown (tricky bits)

# withWatermark("event_time","30 seconds"): allows late events up to 30s; after that, window state can be 
# dropped.

# groupBy(window(...), region, product_id, ...): window is a time-bucket column produced by Spark.

# Window.partitionBy("window","region").orderBy(desc("revenue")): ranking scope is per window‑region pair.

# outputMode("complete"): required because the table is aggregated and updates each micro‑batch.

# 🧠 Analogy

# Think of each minute as a podium ceremony in every region. As new orders arrive, the podium order 
# changes; every 5 seconds the announcer reads the updated Top‑5.

# 🪞 Visual Imagination

# Timeline split into 1‑minute boxes. Inside each region’s box, products race upward; the top five stay 
# visible; late runners (≤30s) can still change positions.


# Q1. Why use dense_rank instead of row_number?
# A1. dense_rank keeps ties at the same rank and doesn’t skip ranks (1,2,2,3), which is friendlier 
# for Top‑N displays.

# Q2. Why complete mode and not append?
# A2. Aggregations over windows update as new data arrives; complete reprints the full current 
# result every trigger.

# Q3. What happens after the watermark horizon?
# A3. Spark may drop state for old windows; very late events (beyond 30s) won’t change finalized windows.



# ⚙️ (Optional) Kafka version — scaffold only (no files)

# If/when you have Kafka running locally, swap the source:

sales_raw = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootsrap.servers", "localhost:9092")
         .option("subscribe", "sales_events")
         .option("startingOffsets", "latest")
         .load()
)

# Assuming JSON messages; define schema then:
schema = T.StructType([
    T.StructField("event_time", T.TimestampType(), True),
    T.StructField("customer_id", T.IntegerType(), True),
    T.StructField("product_id", T.IntegerType(), True),
    T.StructField("quantity", T.IntegerType(), True)
])


events = (
    sales_raw
    .select(F.from_json(F.col("value").cast("string"), schema).alias("j"))
    .select("j.*")
)


