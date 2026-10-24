#!/usr/bin/env python3
"""
Data Skew Fix — Interactive Demo
=================================
Run this script to see a side-by-side comparison of:
  1. Naive join on skewed data (slow, OOM-prone)
  2. Optimized join with skew handling (fast, stable)

Shows execution plans, task distribution, and timing for both approaches.

Usage:
    python scripts/demo_skew_fix.py
    python scripts/demo_skew_fix.py --records 1000000 --salts 20
"""

import time
import argparse
import sys
import os

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F

from src.config.spark_config import SparkConfig
from src.ingestion.kafka_producer import (
    generate_events_batch, generate_merchant_dimension,
)
from src.optimizations.skew_handler import SkewHandler
from src.utils.metrics import MetricsTracker
from src.schemas.ecommerce_schemas import ORDERS_SCHEMA, MERCHANT_DIM_SCHEMA


def run_demo(num_records: int = 100000, num_salts: int = 10):
    """Run the data skew fix demonstration."""

    print("\n" + "=" * 70)
    print("  🔧 DATA SKEW FIX — INTERACTIVE DEMO")
    print("=" * 70)
    print(f"\n  Generating {num_records:,} order records with skewed merchant_id...")
    print("  (MERCHANT_001 gets ~60% of ALL records)\n")

    # ── Create SparkSession ──
    config = SparkConfig(app_name="skew-demo", mode="local")
    spark = config.create_session()

    # ── Generate skewed data ──
    events = list(generate_events_batch("orders", num_records))
    merchants = generate_merchant_dimension()

    orders_df = spark.read.json(
        spark.sparkContext.parallelize([
            str(e).replace("'", '"').replace("None", "null").replace("True", "true").replace("False", "false")
            for e in events
        ])
    )
    merchant_df = spark.read.json(
        spark.sparkContext.parallelize([
            str(m).replace("'", '"')
            for m in merchants
        ])
    )

    # ── Analyze the skew ──
    tracker = MetricsTracker(spark)
    skew_report = tracker.analyze_skew(orders_df, "merchant_id")
    tracker.print_skew_report(skew_report)

    handler = SkewHandler(spark)

    # ════════════════════════════════════════════════════════
    # APPROACH 1: Naive Join (THE PROBLEM)
    # ════════════════════════════════════════════════════════
    print("\n" + "─" * 70)
    print("  ❌ APPROACH 1: Naive SortMergeJoin (THE PROBLEM)")
    print("─" * 70)
    print("  orders_df.join(merchant_df, 'merchant_id')")
    print("  → 60% of data routed to 1 executor → OOM risk!")
    print()

    # Disable AQE to show the raw problem
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    start_time = time.time()
    naive_result = orders_df.join(merchant_df, on="merchant_id", how="left")
    naive_count = naive_result.count()
    naive_duration = time.time() - start_time

    print(f"  Result: {naive_count:,} records in {naive_duration:.2f}s")
    print(f"  ⚠️  In production with billions of records, this causes OOM!")
    print("\n  Execution Plan (notice SortMergeJoin):")
    naive_result.explain(mode="simple")

    # ════════════════════════════════════════════════════════
    # APPROACH 2: Optimized Join (THE FIX)
    # ════════════════════════════════════════════════════════
    print("\n" + "─" * 70)
    print("  ✅ APPROACH 2: Optimized Join (THE FIX)")
    print("─" * 70)
    print("  Step 1: Isolate null/default keys")
    print("  Step 2: Broadcast join (dim table < 100MB)")
    print("  Step 3: AQE enabled as safety net")
    print("  Step 4: Union null records back")
    print()

    # Re-enable AQE
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    start_time = time.time()
    optimized_result = handler.fix_skewed_join(
        fact_df=orders_df,
        dim_df=merchant_df,
        key_col="merchant_id",
        dim_size_mb=1.0,
    )
    optimized_count = optimized_result.count()
    optimized_duration = time.time() - start_time

    print(f"  Result: {optimized_count:,} records in {optimized_duration:.2f}s")
    print("\n  Execution Plan (notice BroadcastHashJoin — no shuffle!):")
    optimized_result.explain(mode="simple")

    # ════════════════════════════════════════════════════════
    # COMPARISON
    # ════════════════════════════════════════════════════════
    speedup = naive_duration / max(optimized_duration, 0.001)
    print("\n" + "=" * 70)
    print("  📊 COMPARISON")
    print("=" * 70)
    print(f"  {'Metric':<30} {'Naive':>15} {'Optimized':>15}")
    print(f"  {'─' * 30} {'─' * 15} {'─' * 15}")
    print(f"  {'Duration':<30} {naive_duration:>14.2f}s {optimized_duration:>14.2f}s")
    print(f"  {'Record Count':<30} {naive_count:>15,} {optimized_count:>15,}")
    print(f"  {'Join Strategy':<30} {'SortMerge':>15} {'Broadcast':>15}")
    print(f"  {'Null Key Handling':<30} {'Mixed in':>15} {'Isolated':>15}")
    print(f"  {'AQE Skew Protection':<30} {'Disabled':>15} {'Enabled':>15}")
    print(f"  {'Speedup':<30} {'':>15} {speedup:>14.1f}x")
    print(f"  {'OOM Risk':<30} {'⚠️  HIGH':>15} {'✅ NONE':>15}")
    print("=" * 70)

    spark.stop()
    print("\n  Demo complete! ✅\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Skew Fix Demo")
    parser.add_argument("--records", type=int, default=100000,
                        help="Number of order records to generate")
    parser.add_argument("--salts", type=int, default=10,
                        help="Number of salt buckets for salted join")
    args = parser.parse_args()
    run_demo(args.records, args.salts)
