# %% [markdown]
# # Deep-Dive: Diagnosing and Fixing Data Skew in PySpark
# 
# **Problem**: A single merchant generates 60% of all order events in an ecommerce streaming lakehouse.
# When joining orders with dimension tables, the default `SortMergeJoin` partitions by key, causing:
# - 1 executor handling 600K records while 199 executors handle <2K records
# - Disk spills (memory buffer exhaustion)
# - Executor OutOfMemory (OOM) crashes
#
# Let's walk through detection and the 4-step engineering playbook!

# %%
import sys
import os
sys.path.insert(0, os.path.abspath(".."))

from pyspark.sql import functions as F
from src.config.spark_config import SparkConfig
from src.optimizations.skew_handler import SkewHandler
from src.utils.metrics import MetricsTracker
from src.ingestion.kafka_producer import generate_events_batch, generate_merchant_dimension

# Initialize Spark with AQE enabled
spark = SparkConfig(app_name="data-skew-notebook", mode="local").create_session()
print("Spark Session Initialized:", spark.version)

# %% [markdown]
# ## 1. Generate Skewed Dataset
# We generate 100,000 order records where `MERCHANT_001` accounts for ~60% of volume.

# %%
events = list(generate_events_batch("orders", count=100000))
orders_df = spark.read.json(
    spark.sparkContext.parallelize([
        str(e).replace("'", '"').replace("None", "null").replace("True", "true").replace("False", "false")
        for e in events
    ])
)
merchant_dim = spark.createDataFrame(generate_merchant_dimension())

orders_df.printSchema()

# %% [markdown]
# ## 2. Quantify Data Skew
# Let's inspect the frequency distribution of `merchant_id`.

# %%
tracker = MetricsTracker(spark)
skew_report = tracker.analyze_skew(orders_df, key_column="merchant_id", threshold=5.0)
tracker.print_skew_report(skew_report)

# %% [markdown]
# ## 3. The 4-Step Skew Fix Playbook
#
# ### Step 1: Null / Default Key Isolation
# Keys with `None`, `""`, `"UNKNOWN"`, or `"DEFAULT"` cause extreme skew and can never match dimension records.
# We isolate them before the join and union downstream.

# %%
handler = SkewHandler(spark)
normal_df, null_df = handler.isolate_null_keys(orders_df, "merchant_id")
print(f"Normal records: {normal_df.count()}, Null/Default records isolated: {null_df.count()}")

# %% [markdown]
# ### Step 2: Broadcast Hash Join
# For dimension tables under 100MB, broadcast the table to eliminate the shuffle entirely!

# %%
broadcast_joined = normal_df.join(
    F.broadcast(merchant_dim),
    on="merchant_id",
    how="left"
)
broadcast_joined.explain(mode="simple")

# %% [markdown]
# ### Step 3: Adaptive Query Execution (AQE) Skew Join
# In batch execution and micro-batch foreachBatch stages, Spark 3+ AQE splits skewed partitions dynamically.
# Note: Spark's streaming engine disables AQE for continuous streaming writes (see ResolveWriteToStream),
# which is why streaming pipelines rely on broadcast joins, deterministic salting, and write-time partitioning.

# %%
handler.configure_aqe_skew(skew_factor=5)
print("AQE Skew Join Enabled:", spark.conf.get("spark.sql.adaptive.skewJoin.enabled"))

# %% [markdown]
# ### Step 4: Deterministic Key Salting
# When performing high-volume aggregations on skewed keys, deterministic salting
# (using `pmod(xxhash64(order_id), N)`) spreads the dominant key across N buckets safely
# without non-deterministic `rand()` hazards across task retries.

# %%
salted_agg = handler.salted_aggregation(
    orders_df,
    group_cols=["merchant_id", "event_date"],
    agg_exprs={"total_amount": "sum", "order_id": "count"},
    num_salts=10,
    id_col="order_id",
)
salted_agg.show(10)

# %%
spark.stop()
