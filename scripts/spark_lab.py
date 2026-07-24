from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.sql.functions import broadcast, udf
from pyspark import StorageLevel

# ==========================================================
# SPARK SESSION
# ==========================================================

spark = (
    SparkSession.builder
    .appName("PySpark Complete Laboratory")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ==========================================================
# LOAD DATA
# ==========================================================

print("\n" + "="*50)
print("READ JSON")
print("="*50)

df = spark.read.json(
    "data/raw/transactions.json"
)

# ==========================================================
# EXPLORATION
# ==========================================================

print("\nSCHEMA")
df.printSchema()

print("\nFIRST ROWS")
df.show(5)

print("\nDESCRIBE")
df.describe().show()

# ==========================================================
# SELECT
# ==========================================================

print("\nSELECT")

(
    df.select(
        "transaction_id",
        "user_id",
        "amount"
    )
    .show(5)
)

# ==========================================================
# FILTER
# ==========================================================

print("\nFILTER")

(
    df.filter(
        col("amount") > 3000
    )
    .show(5)
)

# ==========================================================
# WITH COLUMN
# ==========================================================

print("\nWITH COLUMN")

df = df.withColumn(
    "amount_with_tax",
    col("amount") * 1.10
)

# ==========================================================
# WHEN / OTHERWISE
# ==========================================================

print("\nRISK LEVEL")

df = df.withColumn(
    "risk_level",
    when(col("amount") > 3000, "HIGH")
    .when(col("amount") > 1000, "MEDIUM")
    .otherwise("LOW")
)

# ==========================================================
# CAST
# ==========================================================

df = df.withColumn(
    "amount_int",
    col("amount").cast("integer")
)

# ==========================================================
# DISTINCT
# ==========================================================

print("\nDISTINCT CATEGORIES")

(
    df.select("category")
      .distinct()
      .show()
)

# ==========================================================
# ORDER BY
# ==========================================================

print("\nTOP VALUES")

(
    df.orderBy(
        col("amount").desc()
    )
    .show(10)
)

# ==========================================================
# GROUP BY
# ==========================================================

print("\nGROUP BY CATEGORY")

(
    df.groupBy("category")
      .count()
      .show()
)

# ==========================================================
# AGGREGATIONS
# ==========================================================

print("\nAGGREGATIONS")

(
    df.groupBy(
        "category",
        "status"
    )
    .agg(
        count("*").alias("count"),
        avg("amount").alias("avg"),
        max("amount").alias("max"),
        min("amount").alias("min"),
        sum("amount").alias("sum")
    )
    .show()
)

# ==========================================================
# PIVOT
# ==========================================================

print("\nPIVOT")

(
    df.groupBy("category")
      .pivot("status")
      .count()
      .show()
)

# ==========================================================
# DROP DUPLICATES
# ==========================================================

print("\nDROP DUPLICATES")

df = df.dropDuplicates(
    ["transaction_id"]
)

# ==========================================================
# REGEXP REPLACE
# ==========================================================

df = df.withColumn(
    "category_clean",
    regexp_replace(
        col("category"),
        "_",
        ""
    )
)

# ==========================================================
# TEMP VIEW
# ==========================================================

print("\nSPARK SQL")

df.createOrReplaceTempView(
    "transactions"
)

spark.sql("""
SELECT
    category,
    COUNT(*) total,
    AVG(amount) avg_amount
FROM transactions
GROUP BY category
""").show()

# ==========================================================
# JOINS
# ==========================================================

print("\nJOINS")

users = spark.createDataFrame(
    [
        (1, "Joao"),
        (2, "Maria"),
        (3, "Pedro"),
        (4, "Ana")
    ],
    ["user_id", "name"]
)

inner_join = (
    df.join(
        users,
        "user_id",
        "inner"
    )
)

inner_join.show()

left_join = (
    df.join(
        users,
        "user_id",
        "left"
    )
)

left_join.show()

anti_join = (
    df.join(
        users,
        "user_id",
        "left_anti"
    )
)

anti_join.show()

# ==========================================================
# BROADCAST JOIN
# ==========================================================

print("\nBROADCAST JOIN")

(
    df.join(
        broadcast(users),
        "user_id"
    )
    .show()
)

# ==========================================================
# WINDOW FUNCTIONS
# ==========================================================

print("\nWINDOW FUNCTIONS")

window_spec = (
    Window
    .partitionBy("user_id")
    .orderBy(
        col("amount").desc()
    )
)

window_df = (
    df
    .withColumn(
        "row_number",
        row_number().over(window_spec)
    )
    .withColumn(
        "rank",
        rank().over(window_spec)
    )
    .withColumn(
        "dense_rank",
        dense_rank().over(window_spec)
    )
    .withColumn(
        "lag_amount",
        lag("amount").over(window_spec)
    )
    .withColumn(
        "lead_amount",
        lead("amount").over(window_spec)
    )
)

window_df.show(20)

# ==========================================================
# WINDOW AGGREGATION
# ==========================================================

print("\nWINDOW SUM")

window_running = (
    Window
    .partitionBy("user_id")
    .orderBy("amount")
)

(
    df.withColumn(
        "running_total",
        sum("amount").over(window_running)
    )
    .show()
)

# ==========================================================
# UDF
# ==========================================================

print("\nUDF")

def classify(amount):

    if amount > 3000:
        return "HIGH"

    elif amount > 1000:
        return "MEDIUM"

    return "LOW"

risk_udf = udf(
    classify,
    StringType()
)

(
    df.withColumn(
        "risk_udf",
        risk_udf("amount")
    )
    .show()
)

# ==========================================================
# CACHE
# ==========================================================

print("\nCACHE")

df.cache()

df.count()

# ==========================================================
# PERSIST
# ==========================================================

print("\nPERSIST")

df.persist(
    StorageLevel.MEMORY_AND_DISK
)

df.count()

# ==========================================================
# PARTITIONS
# ==========================================================

print("\nPARTITIONS")

print(
    "Current:",
    df.rdd.getNumPartitions()
)

repartitioned = df.repartition(
    8,
    "category"
)

print(
    "After repartition:",
    repartitioned.rdd.getNumPartitions()
)

coalesced = repartitioned.coalesce(4)

print(
    "After coalesce:",
    coalesced.rdd.getNumPartitions()
)

# ==========================================================
# WRITE PARQUET
# ==========================================================

print("\nWRITE PARQUET")

(
    df.write
    .mode("overwrite")
    .partitionBy("category")
    .parquet("data/curated")
)

# ==========================================================
# EXPLAIN PLAN
# ==========================================================

print("\nEXPLAIN")

(
    df.filter(
        col("amount") > 1000
    )
    .groupBy("category")
    .agg(avg("amount"))
    .explain(True)
)

# ==========================================================
# SAMPLE
# ==========================================================

print("\nSAMPLE")

(
    df.sample(
        fraction=0.10,
        seed=42
    )
    .show()
)

# ==========================================================
# LIMIT
# ==========================================================

print("\nLIMIT")

df.limit(5).show()

# ==========================================================
# COLLECT
# ==========================================================

print("\nCOLLECT")

rows = df.limit(3).collect()

for row in rows:
    print(row)

# ==========================================================
# CLEANUP
# ==========================================================

df.unpersist()

spark.stop()