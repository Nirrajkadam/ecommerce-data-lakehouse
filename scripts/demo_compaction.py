#!/usr/bin/env python3
"""
Small Files Compaction — Interactive Demo
==========================================
Demonstrates the small files problem and how compaction fixes it.

1. Creates many tiny Parquet files (simulating streaming writes)
2. Shows the problem: file count, sizes, metadata overhead
3. Runs compaction
4. Shows the fix: fewer, larger files

Usage:
    python scripts/demo_compaction.py
"""

import os
import sys
import time
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from src.config.spark_config import SparkConfig
from src.optimizations.compaction import CompactionEngine
from src.ingestion.kafka_producer import generate_events_batch
from src.utils.metrics import MetricsTracker


def run_demo():
    """Run the small files compaction demonstration."""

    print("\n" + "=" * 70)
    print("  📁 SMALL FILES PROBLEM — INTERACTIVE DEMO")
    print("=" * 70)

    config = SparkConfig(app_name="compaction-demo", mode="local")
    spark = config.create_session()
    engine = CompactionEngine(spark)
    tracker = MetricsTracker(spark)

    base_path = "./data/demo_compaction"
    problem_path = f"{base_path}/problem"
    fixed_path = f"{base_path}/fixed"

    # Clean up
    shutil.rmtree(base_path, ignore_errors=True)

    # ════════════════════════════════════════════════════════
    # STEP 1: Simulate the Problem
    # ════════════════════════════════════════════════════════
    print("\n  Step 1: Simulating streaming writes (many tiny files)...")
    print("  Writing 20 micro-batches, each producing multiple small files...\n")

    events = list(generate_events_batch("orders", 50000))
    all_df = spark.read.json(
        spark.sparkContext.parallelize([
            str(e).replace("'", '"').replace("None", "null")
            .replace("True", "true").replace("False", "false")
            for e in events
        ])
    )

    # Simulate streaming: write many small batches
    batch_size = 2500
    total_records = all_df.count()
    rows = all_df.collect()

    for i in range(20):
        start_idx = i * batch_size
        end_idx = min(start_idx + batch_size, total_records)
        batch_rows = rows[start_idx:end_idx]
        if not batch_rows:
            break

        batch_df = spark.createDataFrame(batch_rows, all_df.schema)
        # Each batch writes 4 files (simulating 4 executors)
        batch_df.repartition(4).write.mode("append").parquet(problem_path)
        print(f"    Batch {i + 1}/20 written ({len(batch_rows)} records)")

    # Show the problem
    problem_stats = engine.monitor_file_sizes(problem_path)
    print(f"\n  ⚠️  THE PROBLEM:")
    print(f"  ────────────────────────────────────────")
    print(f"  Total Files:      {problem_stats['file_count']}")
    print(f"  Total Size:       {problem_stats['total_size_mb']:.2f} MB")
    print(f"  Avg File Size:    {problem_stats['avg_file_size_mb']:.4f} MB")
    if 'min_file_size_mb' in problem_stats:
        print(f"  Min File Size:    {problem_stats['min_file_size_mb']:.4f} MB")
        print(f"  Max File Size:    {problem_stats['max_file_size_mb']:.4f} MB")
    if 'small_file_count' in problem_stats:
        print(f"  Small Files:      {problem_stats['small_file_count']}")
    print(f"\n  Each query must open {problem_stats['file_count']} files!")
    print(f"  80% of query time is spent on file I/O, not data scanning.\n")

    # Benchmark query on fragmented data
    start = time.time()
    count1 = spark.read.parquet(problem_path).count()
    fragmented_time = time.time() - start

    # ════════════════════════════════════════════════════════
    # STEP 2: Apply the Fix (Compaction)
    # ════════════════════════════════════════════════════════
    print("  Step 2: Compacting files (read → coalesce → write)...\n")

    df = spark.read.parquet(problem_path)
    optimized_df = engine.optimize_write_partitioning(df, "event_date")
    optimized_df.write.mode("overwrite").parquet(fixed_path)

    fixed_stats = engine.monitor_file_sizes(fixed_path)
    print(f"  ✅ THE FIX:")
    print(f"  ────────────────────────────────────────")
    print(f"  Total Files:      {fixed_stats['file_count']}")
    print(f"  Total Size:       {fixed_stats['total_size_mb']:.2f} MB")
    print(f"  Avg File Size:    {fixed_stats['avg_file_size_mb']:.4f} MB")
    if 'min_file_size_mb' in fixed_stats:
        print(f"  Min File Size:    {fixed_stats['min_file_size_mb']:.4f} MB")
        print(f"  Max File Size:    {fixed_stats['max_file_size_mb']:.4f} MB")

    # Benchmark query on compacted data
    start = time.time()
    count2 = spark.read.parquet(fixed_path).count()
    compacted_time = time.time() - start

    # ════════════════════════════════════════════════════════
    # COMPARISON
    # ════════════════════════════════════════════════════════
    reduction = (1 - fixed_stats['file_count'] / max(problem_stats['file_count'], 1)) * 100
    speedup = fragmented_time / max(compacted_time, 0.001)

    print(f"\n  {'=' * 50}")
    print(f"  📊 BEFORE vs AFTER COMPACTION")
    print(f"  {'=' * 50}")
    print(f"  {'Metric':<25} {'Before':>12} {'After':>12}")
    print(f"  {'─' * 25} {'─' * 12} {'─' * 12}")
    print(f"  {'File Count':<25} {problem_stats['file_count']:>12} {fixed_stats['file_count']:>12}")
    print(f"  {'Avg File Size':<25} {problem_stats['avg_file_size_mb']:>11.4f}MB {fixed_stats['avg_file_size_mb']:>11.4f}MB")
    print(f"  {'Query Time':<25} {fragmented_time:>11.3f}s {compacted_time:>11.3f}s")
    print(f"  {'Record Count':<25} {count1:>12,} {count2:>12,}")
    print(f"  {'File Reduction':<25} {'':>12} {reduction:>11.0f}%")
    print(f"  {'Query Speedup':<25} {'':>12} {speedup:>11.1f}x")
    print(f"  {'=' * 50}")

    spark.stop()
    shutil.rmtree(base_path, ignore_errors=True)
    print("\n  Demo complete! ✅\n")


if __name__ == "__main__":
    run_demo()
