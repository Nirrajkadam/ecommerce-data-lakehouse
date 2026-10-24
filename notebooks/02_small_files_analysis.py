# %% [markdown]
# # Deep-Dive: The Small Files Problem in Streaming Lakehouses
#
# **Problem**: Structured Streaming micro-batches (e.g. 30s triggers) writing to Delta/Parquet sinks
# generate 10,000s of tiny (<1MB) files.
#
# Downstream query engines (Presto, Trino, BigQuery, Spark) spend 80% of runtime on metadata listing
# and file open operations rather than data scanning.

# %%
import sys
import os
sys.path.insert(0, os.path.abspath(".."))

from src.config.spark_config import SparkConfig
from src.optimizations.compaction import CompactionEngine
from src.ingestion.kafka_producer import generate_events_batch

spark = SparkConfig(app_name="small-files-notebook", mode="local").create_session()
engine = CompactionEngine(spark)

# %% [markdown]
# ## 1. Write-Time Fix: Intelligent Repartitioning
# Calculate optimal file partition count dynamically based on payload volume:
# `num_partitions = max(1, total_size_mb // target_file_size_mb)`

# %%
events = list(generate_events_batch("orders", count=50000))
df = spark.read.json(
    spark.sparkContext.parallelize([
        str(e).replace("'", '"').replace("None", "null").replace("True", "true").replace("False", "false")
        for e in events
    ])
)

optimized_df = engine.optimize_write_partitioning(df, partition_col="event_date", target_file_size_mb=256)
print("Partitions after write optimization:", optimized_df.rdd.getNumPartitions())

# %% [markdown]
# ## 2. Lakehouse Compaction Fix: OPTIMIZE & ZORDER
# Delta Lake provides async file compaction into 128MB-256MB blocks, plus Z-Ordering along
# high-cardinality query predicate columns (`merchant_id`, `event_timestamp`) for data skipping.

# %%
# Example OPTIMIZE command:
# spark.sql("OPTIMIZE delta.`/path/to/table` ZORDER BY (merchant_id, event_timestamp)")

# %%
spark.stop()
