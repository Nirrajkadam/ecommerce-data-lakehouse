#!/usr/bin/env python3
"""
Benchmark Runner and Artifact Generator
=======================================
Executes repeated, controlled performance benchmarks measuring the impact of
skew handling and compaction optimizations on PySpark 3.5.1 / Delta Lake 3.1.0.

Outputs committed benchmark results in JSON and Markdown formats:
  - docs/benchmarks/benchmark_results.json
  - docs/benchmarks/BENCHMARKS.md
"""

import os
import sys
import time
import json
import platform
from datetime import datetime, timezone

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from src.config.spark_config import SparkConfig
from src.ingestion.kafka_producer import generate_events_batch, generate_merchant_dimension
from src.optimizations.skew_handler import SkewHandler
from src.optimizations.compaction import CompactionEngine


def collect_system_metadata(spark):
    """Collect hardware and execution environment details."""
    sc = spark.sparkContext
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "python_version": platform.python_version(),
        "spark_version": spark.version,
        "os": platform.system(),
        "os_release": platform.release(),
        "cpu_count": os.cpu_count(),
        "default_parallelism": sc.defaultParallelism,
        "master": sc.master,
    }


def benchmark_join(spark, orders_df, merchant_df, iterations=3):
    """Benchmark SortMergeJoin vs. Broadcast Hash Join under 60% key skew."""
    handler = SkewHandler(spark)

    # 1. Baseline: Force SortMergeJoin by disabling broadcast and AQE
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    baseline_times = []
    for _ in range(iterations):
        t0 = time.time()
        c = orders_df.join(merchant_df, on="merchant_id", how="left").count()
        baseline_times.append(time.time() - t0)

    # 2. Optimized: Broadcast Hash Join with Null Key Isolation
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    optimized_times = []
    for _ in range(iterations):
        t0 = time.time()
        c_opt = handler.fix_skewed_join(
            fact_df=orders_df,
            dim_df=merchant_df,
            key_col="merchant_id",
            dim_size_mb=1.0,
        ).count()
        optimized_times.append(time.time() - t0)

    return {
        "baseline_sort_merge_join_seconds": [round(t, 3) for t in baseline_times],
        "baseline_mean_seconds": round(sum(baseline_times) / len(baseline_times), 3),
        "optimized_broadcast_join_seconds": [round(t, 3) for t in optimized_times],
        "optimized_mean_seconds": round(sum(optimized_times) / len(optimized_times), 3),
        "speedup_factor": round((sum(baseline_times) / len(baseline_times)) / max(sum(optimized_times) / len(optimized_times), 0.001), 2),
    }


def benchmark_aggregation(spark, orders_df, iterations=3):
    """Benchmark Naive GroupBy vs. Deterministic Two-Stage Salted Aggregation."""
    handler = SkewHandler(spark)

    # 1. Naive Aggregation on skewed merchant_id
    naive_times = []
    for _ in range(iterations):
        t0 = time.time()
        c = orders_df.groupBy("merchant_id", "event_date").agg(
            F.sum("total_amount").alias("revenue"),
            F.count("order_id").alias("orders")
        ).count()
        naive_times.append(time.time() - t0)

    # 2. Two-Stage Salted Aggregation
    salted_times = []
    for _ in range(iterations):
        t0 = time.time()
        c_salted = handler.salted_aggregation(
            df=orders_df,
            group_cols=["merchant_id", "event_date"],
            agg_exprs={"total_amount": "sum", "order_id": "count"},
            num_salts=10,
            id_col="order_id",
        ).count()
        salted_times.append(time.time() - t0)

    return {
        "naive_groupby_seconds": [round(t, 3) for t in naive_times],
        "naive_mean_seconds": round(sum(naive_times) / len(naive_times), 3),
        "salted_aggregation_seconds": [round(t, 3) for t in salted_times],
        "salted_mean_seconds": round(sum(salted_times) / len(salted_times), 3),
    }


def benchmark_compaction(spark, orders_df, base_path="./data/benchmark_compaction"):
    """Benchmark query scan duration before and after file compaction."""
    import shutil
    shutil.rmtree(base_path, ignore_errors=True)
    os.makedirs(base_path, exist_ok=True)

    problem_path = f"{base_path}/fragmented"
    fixed_path = f"{base_path}/compacted"
    engine = CompactionEngine(spark)

    # Write 15 small micro-batches
    total_records = orders_df.count()
    rows = orders_df.collect()
    batch_size = max(100, len(rows) // 15)

    for i in range(15):
        batch = rows[i * batch_size : (i + 1) * batch_size]
        if batch:
            b_df = spark.createDataFrame(batch, orders_df.schema)
            b_df.repartition(4).write.mode("append").parquet(problem_path)

    stats_before = engine.monitor_file_sizes(problem_path)

    # Scan benchmark on fragmented data
    t0 = time.time()
    c1 = spark.read.parquet(problem_path).filter(F.col("total_amount") > 50).count()
    fragmented_scan_time = time.time() - t0

    # Compact files
    df_raw = spark.read.parquet(problem_path)
    opt_df = engine.optimize_write_partitioning(df_raw, "event_date", target_file_size_mb=256)
    opt_df.write.mode("overwrite").parquet(fixed_path)

    stats_after = engine.monitor_file_sizes(fixed_path)

    # Scan benchmark on compacted data
    t1 = time.time()
    c2 = spark.read.parquet(fixed_path).filter(F.col("total_amount") > 50).count()
    compacted_scan_time = time.time() - t1

    shutil.rmtree(base_path, ignore_errors=True)

    return {
        "files_before_compaction": stats_before["file_count"],
        "files_after_compaction": stats_after["file_count"],
        "file_reduction_pct": round((1 - stats_after["file_count"] / max(stats_before["file_count"], 1)) * 100, 1),
        "fragmented_scan_seconds": round(fragmented_scan_time, 3),
        "compacted_scan_seconds": round(compacted_scan_time, 3),
        "scan_speedup_factor": round(fragmented_scan_time / max(compacted_scan_time, 0.001), 2),
    }


def main():
    print("=" * 70)
    print("  RUNNING LAKEHOUSE OPTIMIZATION BENCHMARK SUITE")
    print("=" * 70)

    spark = SparkConfig(app_name="benchmark-runner", mode="local").create_session()
    metadata = collect_system_metadata(spark)

    print(f"System: {metadata['os']} | CPU Cores: {metadata['cpu_count']} | Spark: {metadata['spark_version']}")
    print("Synthesizing 50,000 order events with 60% skew on MERCHANT_001...")

    events = list(generate_events_batch("orders", count=50000))
    orders_df = spark.read.json(
        spark.sparkContext.parallelize([
            str(e).replace("'", '"').replace("None", "null").replace("True", "true").replace("False", "false")
            for e in events
        ])
    )
    merchants = generate_merchant_dimension()
    merchant_df = spark.read.json(
        spark.sparkContext.parallelize([str(m).replace("'", '"') for m in merchants])
    )

    print("\n[1/3] Benchmarking Join Skew (SortMergeJoin vs. Broadcast Hash Join)...")
    join_results = benchmark_join(spark, orders_df, merchant_df, iterations=3)

    print("\n[2/3] Benchmarking Aggregation Skew (Naive vs. Deterministic Salting)...")
    agg_results = benchmark_aggregation(spark, orders_df, iterations=3)

    print("\n[3/3] Benchmarking Small Files Compaction (Fragmented vs. Compacted)...")
    compaction_results = benchmark_compaction(spark, orders_df)

    benchmark_payload = {
        "metadata": metadata,
        "dataset": {
            "record_count": 50000,
            "skew_pattern": "60% volume allocated to MERCHANT_001",
        },
        "results": {
            "join_skew": join_results,
            "aggregation_skew": agg_results,
            "storage_compaction": compaction_results,
        }
    }

    # Write output artifacts
    out_dir = "./docs/benchmarks"
    os.makedirs(out_dir, exist_ok=True)
    json_path = os.path.join(out_dir, "benchmark_results.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(benchmark_payload, f, indent=2)

    md_path = os.path.join(out_dir, "BENCHMARKS.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"""# Lakehouse Optimization Benchmark Report

**Generated:** {metadata['timestamp_utc']}  
**Hardware & Runtime:** {metadata['os']} | {metadata['cpu_count']} CPU cores | Spark {metadata['spark_version']} | Python {metadata['python_version']}  
**Dataset Scale:** 50,000 order records (60% volume skewed to `MERCHANT_001`)

---

## 1. Join Skew Benchmark: SortMergeJoin vs. Broadcast Hash Join

| Metric | SortMergeJoin (Unoptimized) | Broadcast Hash Join (Optimized) |
| :--- | :--- | :--- |
| **Execution Times (3 runs)** | {join_results['baseline_sort_merge_join_seconds']} s | {join_results['optimized_broadcast_join_seconds']} s |
| **Mean Duration** | {join_results['baseline_mean_seconds']} s | {join_results['optimized_mean_seconds']} s |
| **Speedup Factor** | Baseline | **{join_results['speedup_factor']}x** |

---

## 2. Storage Compaction Benchmark: Fragmented vs. Compacted Parquet Scans

| Metric | Fragmented Layout (15 micro-batches) | Compacted Layout (Write-Time Repartitioning) |
| :--- | :--- | :--- |
| **File Count** | {compaction_results['files_before_compaction']} files | {compaction_results['files_after_compaction']} files ({compaction_results['file_reduction_pct']}% reduction) |
| **Filtered Scan Duration** | {compaction_results['fragmented_scan_seconds']} s | {compaction_results['compacted_scan_seconds']} s |
| **Scan Speedup** | Baseline | **{compaction_results['scan_speedup_factor']}x** |

---

## Reproduction Instructions

To reproduce these measurements locally:
```bash
python scripts/run_benchmarks.py
```
""")

    print(f"\nBenchmark artifacts generated successfully:")
    print(f"  JSON: {json_path}")
    print(f"  Report: {md_path}")
    spark.stop()


if __name__ == "__main__":
    main()
