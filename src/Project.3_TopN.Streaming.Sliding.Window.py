# let’s add the sliding window version to Project 3, still no files, all console + memory sinks

# 🎯 What we’re adding now

# Replace tumbling window (fixed 1-min buckets) with a sliding window:
# Window size = 1 minute, Slide = 10 seconds
# → every 10s you get updated metrics for the last 60s (smooth, dashboard-like).

# 1) 🔍 📘 Why?

# Sliding windows give finer, rolling insights (e.g., “last 60 seconds, updated every 10 seconds”), 
# which is closer to real monitoring dashboards than discrete minute buckets.

# 2) 🧱 👨🏫 What You Did

# Kept the same live stream + dimension enrichment.

# Changed aggregation to window(event_time, "1 minute", "10 seconds").

# Kept watermark (30s) to control state and late events.

# Computed Top‑N products per region per sliding window.


# # --- Project 3: Sliding Window Top-N (no files, console + memory only) ---
# from pyspark.sql import SparkSession, functions as F 
# from pyspark.sql.window import Window
# from faker import Faker
# import random
# import sys
# import os


# # =========================================================
# # FORCE SPARK_HOME to point ot the correct installation path
# # This is the definitive fix for the 'JavaPackage' error
# # =========================================================


# # The path should be up to the 'pyspark' folder within site-packages
# os.environ['SPARK_HOME'] = r'D:\Uncodemy\Pyspark\pyspark_env\Lib\site-packages\pyspark'

# # Set the Python interpreter for PySpark
# os.environ['PYSPARK_PYTHON'] = r'D:\Uncodemy\Pyspark\pyspark_env\Scripts\python.exe'


# # Ensure the Spark Libraries are on the path
# sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
# sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python/lib/py4j-0.10.9.5-src.zip'))

# # 1) Spark Session
# spark = (
#     SparkSession.builder
#     .appName("Project3-SlidingTopN-Streaming")
#     .master("local[*]")
#     .config("spark.sql.shuffle.partitions", "8")
#     .getOrCreate()
# )

# # 2) Tiny Static Dimensions (in-memory)
# fake = Faker()
# random.seed(42); Faker.seed(42)

# NUM_CUSTOMERS = 5000
# NUM_PRODUCTS  = 200
# regions  = ["North", "South", "East", "West"]
# genders  = ["Male", "Female", "Other"]
# cats     = ["Electronics", "Clothing", "Home", "Sports", "Toys"]
# subcats  = {
#     "Electronics": ["Phone","Laptop","Headphones","Camera","Monitor"],
#     "Clothing":    ["Shirt","Jeans","Jacket","Shoes","Hoodie"],
#     "Home":        ["Blender","Cookware","Lamp","Desk","Chair"],
#     "Sports":      ["Ball","Gloves","Shoes","Watch","Mat"],
#     "Toys":        ["Doll","Blocks","Car","Puzzle","BoardGame"]
# }

# customers_py = [
#     (i, fake.name(), regions[i % len(regions)], genders[i % len(genders)])
#     for i in range(1, NUM_CUSTOMERS + 1)
# ]
# dim_customers = spark.createDataFrame(
#     customers_py,
#     schema="customer_id INT, customer_name STRING, region STRING, gender STRING"
# )

# products_py = []
# for pid in range(1, NUM_PRODUCTS + 1):
#     cat = random.choice(cats)
#     sub = random.choice(subcats[cat])
#     name = f"{cat}-{sub}-{pid:04d}"
#     price = round(random.uniform(5, 1500), 2)
#     products_py.append((pid, cat, sub, name, float(price)))
# dim_products = spark.createDataFrame(
#     products_py,
#     schema="product_id INT, category STRING, subcategory STRING, product_name STRING, base_price DOUBLE"
# )

# # 3) Live Stream via rate source (no disk)
# ROWS_PER_SECOND = 10
# ticks = (
#     spark.readStream
#          .format("rate")
#          .option("rowsPerSecond", ROWS_PER_SECOND)
#          .load()
# )

# events = (
#     ticks
#     .withColumn("event_time", F.col("timestamp"))                         # event time column
#     .withColumn("customer_id", (F.col("value") % NUM_CUSTOMERS) + 1)      # map tick to a customer
#     .withColumn("product_id",  (F.col("value") % NUM_PRODUCTS) + 1)       # map tick to a product
#     .withColumn("quantity",    (F.floor(F.rand()*5) + 1).cast("int"))     # 1..5 units
#     .select("event_time","customer_id","product_id","quantity")
# )

# # 4) Enrich with broadcast dims
# stream_enriched = (
#     events
#     .join(F.broadcast(dim_customers.select("customer_id","region")),
#           on="customer_id", how="left")
#     .join(F.broadcast(dim_products.select("product_id","category","product_name","base_price")),
#           on="product_id", how="left")
#     .withColumn("unit_price",   F.coalesce(F.col("base_price"), F.lit(0.0)))
#     .withColumn("total_amount", F.col("unit_price") * F.col("quantity"))
#     .select("event_time","region","category","product_id","product_name","quantity","unit_price","total_amount")
# )

# # 5) Sliding window aggregation (Streaming Part)
# # NOTE: We ONLY do the Aggregation here. We do NOT do the Ranking here.
# WINDOW_SIZE = "1 minute"     # window length
# SLIDE       = "10 seconds"   # slide interval
# WATERMARK   = "30 seconds"   # late data tolerance

# prod_agg = (
#     stream_enriched
#     .withWatermark("event_time", WATERMARK)                 # state retention / late data control
#     .groupBy(
#         F.window("event_time", WINDOW_SIZE, SLIDE),         # SLIDING window here
#         F.col("region"),
#         F.col("product_id"),
#         F.col("product_name"),
#         F.col("category")
#     )
#     .agg(
#         F.sum("total_amount").alias("revenue"),
#         F.sum("quantity").alias("units")
#     )
# )

# # 6) Sinks: Console + Memory (Using foreachBatch to fix Ranking Error)
# def process_batch_with_ranking(df, epoch_id):
#     """
#     This function runs on every trigger (every 5 seconds).
#     'df' is a STATIC DataFrame containing the results for this moment.
#     Because it is static, we can apply Window Functions (Dense Rank) safely.
#     """
    
#     # Define the Window specification (Static Context)
#     rank_win = Window.partitionBy("window","region").orderBy(F.desc("revenue"), F.desc("units"))
    
#     # Apply Top-N Logic
#     topn_batch = (
#         df
#         .withColumn("rank", F.dense_rank().over(rank_win))
#         .filter(F.col("rank") <= 5)  # Top 5 products
#         .orderBy(F.col("window").start.desc(), "region", "rank")
#     )
    
#     # --- Sink 1: Console ---
#     print(f"--- Batch: {epoch_id} ---")
#     topn_batch.show(80, truncate=False)
    
#     # --- Sink 2: Memory ---
#     # We replace the global view so you can query 'topn_sliding_live' from Spark SQL
#     topn_batch.createOrReplaceGlobalTempView("topn_sliding_live")
    
    
#     # 1) Spark Session
# spark = (
#     SparkSession.builder
#     .appName("Project3-SlidingTopN-Streaming")
#     .master("local[*]")
#     .config("spark.sql.shuffle.partitions", "8")
#     .getOrCreate()
# )

# # 2) Tiny Static Dimensions (in-memory)
# fake = Faker()
# random.seed(42); Faker.seed(42)

# NUM_CUSTOMERS = 5000
# NUM_PRODUCTS  = 200
# regions  = ["North", "South", "East", "West"]
# genders  = ["Male", "Female", "Other"]
# cats     = ["Electronics", "Clothing", "Home", "Sports", "Toys"]
# subcats  = {
#     "Electronics": ["Phone","Laptop","Headphones","Camera","Monitor"],
#     "Clothing":    ["Shirt","Jeans","Jacket","Shoes","Hoodie"],
#     "Home":        ["Blender","Cookware","Lamp","Desk","Chair"],
#     "Sports":      ["Ball","Gloves","Shoes","Watch","Mat"],
#     "Toys":        ["Doll","Blocks","Car","Puzzle","BoardGame"]
# }

# customers_py = [
#     (i, fake.name(), regions[i % len(regions)], genders[i % len(genders)])
#     for i in range(1, NUM_CUSTOMERS + 1)
# ]
# dim_customers = spark.createDataFrame(
#     customers_py,
#     schema="customer_id INT, customer_name STRING, region STRING, gender STRING"
# )

# products_py = []
# for pid in range(1, NUM_PRODUCTS + 1):
#     cat = random.choice(cats)
#     sub = random.choice(subcats[cat])
#     name = f"{cat}-{sub}-{pid:04d}"
#     price = round(random.uniform(5, 1500), 2)
#     products_py.append((pid, cat, sub, name, float(price)))
# dim_products = spark.createDataFrame(
#     products_py,
#     schema="product_id INT, category STRING, subcategory STRING, product_name STRING, base_price DOUBLE"
# )

# # 3) Live Stream via rate source (no disk)
# ROWS_PER_SECOND = 10
# ticks = (
#     spark.readStream
#          .format("rate")
#          .option("rowsPerSecond", ROWS_PER_SECOND)
#          .load()
# )

# events = (
#     ticks
#     .withColumn("event_time", F.col("timestamp"))                         # event time column
#     .withColumn("customer_id", (F.col("value") % NUM_CUSTOMERS) + 1)      # map tick to a customer
#     .withColumn("product_id",  (F.col("value") % NUM_PRODUCTS) + 1)       # map tick to a product
#     .withColumn("quantity",    (F.floor(F.rand()*5) + 1).cast("int"))     # 1..5 units
#     .select("event_time","customer_id","product_id","quantity")
# )

# # 4) Enrich with broadcast dims
# stream_enriched = (
#     events
#     .join(F.broadcast(dim_customers.select("customer_id","region")),
#           on="customer_id", how="left")
#     .join(F.broadcast(dim_products.select("product_id","category","product_name","base_price")),
#           on="product_id", how="left")
#     .withColumn("unit_price",   F.coalesce(F.col("base_price"), F.lit(0.0)))
#     .withColumn("total_amount", F.col("unit_price") * F.col("quantity"))
#     .select("event_time","region","category","product_id","product_name","quantity","unit_price","total_amount")
# )

# # 5) Sliding window aggregation + Top-N per (window, region)
# WINDOW_SIZE = "1 minute"     # window length
# SLIDE       = "10 seconds"   # slide interval
# WATERMARK   = "30 seconds"   # late data tolerance
# TOP_N       = 5

# prod_agg = (
#     stream_enriched
#     .withWatermark("event_time", WATERMARK)                 # state retention / late data control
#     .groupBy(
#         F.window("event_time", WINDOW_SIZE, SLIDE),         # SLIDING window here
#         F.col("region"),
#         F.col("product_id"),
#         F.col("product_name"),
#         F.col("category")
#     )
#     .agg(
#         F.sum("total_amount").alias("revenue"),
#         F.sum("quantity").alias("units")
#     )
# )

# rank_win = Window.partitionBy("window","region").orderBy(F.desc("revenue"), F.desc("units"))
# topn_sliding = (
#     prod_agg
#     .withColumn("rank", F.dense_rank().over(rank_win))
#     .filter(F.col("rank") <= F.lit(TOP_N))
#     .orderBy(F.col("window").start.desc(), "region", "rank")
# )

# # 6) Sinks: Console + Memory (no files)
# q_console = (
#     topn_sliding.writeStream
#         .format("console")
#         .outputMode("complete")                 # full table each trigger
#         .option("truncate", "false")
#         .option("numRows", "80")
#         .trigger(processingTime="5 seconds")
#         .start()
# )

# q_memory = (
#     topn_sliding.writeStream
#         .format("memory")
#         .queryName("topn_sliding_live")         # queryable in Spark SQL
#         .outputMode("complete")
#         .trigger(processingTime="5 seconds")
#         .start()
# )

# # Keep running; stop with Ctrl+C
# q_console.awaitTermination()




# --- Project 3: Sliding Window Top-N (Fixed for Window Function Error) ---
from pyspark.sql import SparkSession, functions as F 
from pyspark.sql.window import Window
from faker import Faker
import random
import sys
import os

# =========================================================
# FORCE SPARK_HOME to point ot the correct installation path
# =========================================================
os.environ['SPARK_HOME'] = r'D:\Uncodemy\Pyspark\pyspark_env\Lib\site-packages\pyspark'
os.environ['PYSPARK_PYTHON'] = r'D:\Uncodemy\Pyspark\pyspark_env\Scripts\python.exe'

sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python/lib/py4j-0.10.9.5-src.zip'))

# 1) Spark Session
spark = (
    SparkSession.builder
    .appName("Project3-SlidingTopN-Streaming")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR") # Reduce noise in console

# 2) Tiny Static Dimensions (in-memory)
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

# 3) Live Stream via rate source (no disk)
ROWS_PER_SECOND = 10
ticks = (
    spark.readStream
         .format("rate")
         .option("rowsPerSecond", ROWS_PER_SECOND)
         .load()
)

events = (
    ticks
    .withColumn("event_time", F.col("timestamp"))                         # event time column
    .withColumn("customer_id", (F.col("value") % NUM_CUSTOMERS) + 1)      # map tick to a customer
    .withColumn("product_id",  (F.col("value") % NUM_PRODUCTS) + 1)       # map tick to a product
    .withColumn("quantity",    (F.floor(F.rand()*5) + 1).cast("int"))     # 1..5 units
    .select("event_time","customer_id","product_id","quantity")
)

# 4) Enrich with broadcast dims
stream_enriched = (
    events
    .join(F.broadcast(dim_customers.select("customer_id","region")),
          on="customer_id", how="left")
    .join(F.broadcast(dim_products.select("product_id","category","product_name","base_price")),
          on="product_id", how="left")
    .withColumn("unit_price",   F.coalesce(F.col("base_price"), F.lit(0.0)))
    .withColumn("total_amount", F.col("unit_price") * F.col("quantity"))
    .select("event_time","region","category","product_id","product_name","quantity","unit_price","total_amount")
)

# 5) Sliding window aggregation (Streaming Part)
# NOTE: We ONLY do the Aggregation here. We do NOT do the Ranking here.
WINDOW_SIZE = "1 minute"     # window length
SLIDE       = "10 seconds"   # slide interval
WATERMARK   = "30 seconds"   # late data tolerance

prod_agg = (
    stream_enriched
    .withWatermark("event_time", WATERMARK)                 # state retention / late data control
    .groupBy(
        F.window("event_time", WINDOW_SIZE, SLIDE),         # SLIDING window here
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

# 6) Sinks: Console + Memory (Using foreachBatch to fix Ranking Error)

def process_batch_with_ranking(df, epoch_id):
    """
    This function runs on every trigger (every 5 seconds).
    'df' is a STATIC DataFrame containing the results for this moment.
    Because it is static, we can apply Window Functions (Dense Rank) safely.
    """
    print(f"--- Processing Batch: {epoch_id} ---")
    
    # IMPORTANT: We must CACHE or Persist here because we use 'df' twice (print + memory)
    # otherwise Spark re-computes the stream for every action, which can cause issues.
    df.persist()
    
    # Define the Window specification (Static Context)
    # This is allowed here because 'df' is a static Micro-Batch, not a Stream
    rank_win = Window.partitionBy("window","region").orderBy(F.desc("revenue"), F.desc("units"))
    
    # Apply Top-N Logic
    topn_batch = (
        df
        .withColumn("rank", F.dense_rank().over(rank_win))
        .filter(F.col("rank") <= 5) # TOP_N = 5
        .orderBy(F.col("window").start.desc(), "region", "rank")
    )
    
    # --- Sink 1: Console ---
    # Using show() inside the function acts as the Console sink
    topn_batch.show(80, truncate=False)
    
    # --- Sink 2: Memory ---
    # We replace the global view so you can query 'topn_sliding_live' from Spark SQL
    # Note: GlobalTempView is safer for cross-session access, but TempView works here too.
    topn_batch.createOrReplaceGlobalTempView("topn_sliding_live")
    
    # Unpersist to free memory
    df.unpersist()

# Start the Stream using foreachBatch
# NOTICE: We are using 'prod_agg' here (the aggregated stream), NOT 'topn_sliding'
query = (
    prod_agg.writeStream
        .outputMode("complete")         # Complete mode gives us the full updated table every trigger
        .foreachBatch(process_batch_with_ranking) # Apply the ranking function to each micro-batch
        .trigger(processingTime="5 seconds")
        .start()
)

# Keep running; stop with Ctrl+C
query.awaitTermination()