#!/usr/bin/env python3
"""
Standalone Compaction Runner
============================
Executes OPTIMIZE and ZORDER operations on Delta tables in the Lakehouse
to mitigate the Small Files Problem.

Usage:
    python scripts/run_compaction.py --table orders --layer bronze
    python scripts/run_compaction.py --all
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.spark_config import SparkConfig
from src.config.pipeline_config import PipelineConfig
from src.optimizations.compaction import CompactionEngine
from src.utils.logger import PipelineLogger

logger = PipelineLogger("compaction-runner")


def compact_table(engine, config, layer: str, table: str):
    table_path = config.paths.get_table_path(layer, table)
    zorder_cols = config.compaction.zorder_columns.get(table, [])

    logger.info(f"Checking table {table} at {table_path}...")
    stats_before = engine.monitor_file_sizes(table_path)
    logger.info(f"Before: {stats_before.get('file_count', 0)} files, avg size {stats_before.get('avg_file_size_mb', 0)} MB")

    logger.info(f"Running compaction with ZORDER columns: {zorder_cols}")
    result = engine.run_compaction(table_path, zorder_cols=zorder_cols if zorder_cols else None)

    logger.info(
        f"Compaction finished: {result.get('files_before')} -> {result.get('files_after')} files "
        f"({result.get('files_removed')} files cleaned)"
    )


def main():
    parser = argparse.ArgumentParser(description="Delta Lake Compaction Engine")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold"], default="bronze")
    parser.add_argument("--table", type=str, default="orders")
    parser.add_argument("--all", action="store_true", help="Compact all known tables")
    args = parser.parse_args()

    config = PipelineConfig()
    spark = SparkConfig(app_name="lakehouse-compactor", mode="local").create_session()
    engine = CompactionEngine(spark)

    try:
        if args.all:
            tables = [
                ("bronze", "orders"),
                ("bronze", "clickstream"),
                ("silver", "orders"),
                ("silver", "clickstream"),
                ("gold", "merchant_daily_summary"),
            ]
            for layer, table in tables:
                compact_table(engine, config, layer, table)
        else:
            compact_table(engine, config, args.layer, args.table)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
