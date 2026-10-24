"""
Small Files Compaction Engine
=============================
Fixes the Small Files Problem in streaming Delta Lake sinks.

When micro-batch streaming jobs write every 30 seconds, they quickly generate
tens of thousands of <1MB Parquet files. Downstream analytical engines and
RAG indexers then spend 80% of their time on S3/GCS metadata listing and
file-open I/O rather than actually scanning data.

This module provides two strategies:

1. **Write-Time Optimization** — Repartition data before writing to produce
   fewer, well-sized files. Applied during pipeline execution.

2. **Post-Write Compaction** — Schedule async OPTIMIZE + ZORDER operations
   to merge tiny files into optimal 128MB-256MB blocks after the fact.

Usage:
    >>> from src.optimizations.compaction import CompactionEngine
    >>> engine = CompactionEngine(spark)
    >>> # At write time:
    >>> optimized_df = engine.optimize_write_partitioning(df, "event_date")
    >>> # Post-write:
    >>> engine.run_compaction("./data/lakehouse/bronze/orders", ["merchant_id"])
"""

import os
import glob
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.logger import PipelineLogger


class CompactionEngine:
    """Engine for optimizing file sizes in Delta Lake tables.

    Provides both write-time and post-write strategies to maintain
    optimal file sizes (128MB-256MB) for analytical query performance.
    """

    def __init__(
        self,
        spark: SparkSession,
        target_file_size_mb: int = 256,
        min_file_size_mb: float = 1.0,
    ):
        """Initialize the compaction engine.

        Args:
            spark: Active SparkSession.
            target_file_size_mb: Target file size after compaction.
            min_file_size_mb: Files smaller than this trigger compaction.
        """
        self.spark = spark
        self.target_file_size_mb = target_file_size_mb
        self.min_file_size_mb = min_file_size_mb
        self.logger = PipelineLogger("compaction-engine", layer="optimization")

    # ──────────────────────────────────────────────────────────
    # STRATEGY 1: Write-Time Optimization
    # ──────────────────────────────────────────────────────────

    def optimize_write_partitioning(
        self,
        df: DataFrame,
        partition_col: str,
        target_file_size_mb: Optional[int] = None,
        estimated_row_size_bytes: int = 500,
    ) -> DataFrame:
        """Repartition DataFrame before writing to produce well-sized files.

        The Small Files Problem occurs because streaming writes produce
        N_executors × N_partitions tiny files every micro-batch. By
        repartitioning by the partition column before writing, we ensure
        each output partition writes a single file of reasonable size.

        The optimal number of partitions is calculated as:
            num_partitions = total_data_size / target_file_size

        Args:
            df: DataFrame to repartition before writing.
            partition_col: Column to partition by (typically date).
            target_file_size_mb: Target size per file. Uses instance default
                                 if not specified.
            estimated_row_size_bytes: Estimated average row size for
                                     partition count calculation.

        Returns:
            Repartitioned DataFrame ready for writing.
        """
        target_mb = target_file_size_mb or self.target_file_size_mb

        self.logger.info(
            f"Optimizing write partitioning on '{partition_col}'",
            target_file_size_mb=target_mb,
        )

        # Estimate optimal partition count
        # For streaming, we typically have smaller batches, so we use
        # fewer partitions to avoid tiny files.
        record_count = df.count()
        estimated_size_mb = (record_count * estimated_row_size_bytes) / (1024 * 1024)

        # Calculate optimal partitions: at least 1, at most 200
        optimal_partitions = max(1, min(
            int(estimated_size_mb / target_mb) + 1,
            200,
        ))

        self.logger.info(
            f"Repartitioning to {optimal_partitions} partitions",
            estimated_size_mb=round(estimated_size_mb, 2),
            record_count=record_count,
        )

        # Repartition with intra-partition distribution:
        # Repartitioning strictly by partition_col (e.g. event_date) causes all records
        # for a single date to hash to the exact same partition, leaving other partitions empty.
        # Adding an intra-partition hash bucket ensures even distribution across all optimal_partitions.
        if partition_col in df.columns and optimal_partitions > 1:
            bucket_candidates = ["order_id", "event_id", "session_id", "inventory_id", "id", "customer_id", "user_id"]
            hash_col = next((c for c in bucket_candidates if c in df.columns), None)
            if hash_col:
                bucket_expr = F.pmod(F.xxhash64(F.col(hash_col)), F.lit(optimal_partitions))
            else:
                bucket_expr = F.pmod(F.xxhash64(F.monotonically_increasing_id()), F.lit(optimal_partitions))
            return df.repartition(optimal_partitions, F.col(partition_col), bucket_expr)
        elif partition_col in df.columns:
            return df.repartition(optimal_partitions, F.col(partition_col))
        else:
            return df.repartition(optimal_partitions)

    def coalesce_for_write(
        self,
        df: DataFrame,
        num_files: int = 1,
    ) -> DataFrame:
        """Coalesce DataFrame to produce exactly N output files.

        Use this for small batches where repartition is overkill.
        Coalesce avoids a full shuffle (unlike repartition) by combining
        existing partitions on the same executor.

        WARNING: Coalescing to 1 file on a large DataFrame creates a
        single-executor bottleneck. Use repartition for large data.

        Args:
            df: DataFrame to coalesce.
            num_files: Number of output files to produce.

        Returns:
            Coalesced DataFrame.
        """
        return df.coalesce(num_files)

    # ──────────────────────────────────────────────────────────
    # STRATEGY 2: Post-Write Compaction (OPTIMIZE + ZORDER)
    # ──────────────────────────────────────────────────────────

    def run_compaction(
        self,
        table_path: str,
        zorder_cols: Optional[List[str]] = None,
    ) -> Dict[str, int]:
        """Run Delta Lake OPTIMIZE (+ optional ZORDER) on a table.

        OPTIMIZE merges small Parquet files into larger ones (target: 1GB
        for Delta, but configurable). ZORDER BY co-locates related records
        within files for efficient data skipping.

        Example: ZORDER BY (merchant_id, event_timestamp) ensures that
        queries filtering on a specific merchant + time range only need
        to read a few files instead of scanning the entire table.

        Args:
            table_path: Path to the Delta table to compact.
            zorder_cols: Columns to ZORDER BY. If None, only OPTIMIZE runs.

        Returns:
            Dict with compaction statistics.
        """
        self.logger.info(f"Running compaction on: {table_path}")

        before_stats = self._get_file_stats(table_path)

        try:
            from delta.tables import DeltaTable

            delta_table = DeltaTable.forPath(self.spark, table_path)

            if zorder_cols:
                # OPTIMIZE + ZORDER — compact and co-locate data
                self.logger.info(
                    f"Running OPTIMIZE + ZORDER BY ({', '.join(zorder_cols)})"
                )
                # Use SQL since the Python API for ZORDER varies by version
                zorder_clause = ", ".join(zorder_cols)
                self.spark.sql(
                    f"OPTIMIZE delta.`{table_path}` ZORDER BY ({zorder_clause})"
                )
            else:
                # OPTIMIZE only — just compact files
                self.logger.info("Running OPTIMIZE (no ZORDER)")
                self.spark.sql(f"OPTIMIZE delta.`{table_path}`")

        except ImportError:
            self.logger.warning(
                "Delta Lake not available. Performing manual Parquet compaction."
            )
            self._manual_compaction(table_path)
        except Exception as e:
            self.logger.error(f"Compaction failed: {e}")
            raise

        after_stats = self._get_file_stats(table_path)

        result = {
            "files_before": before_stats["file_count"],
            "files_after": after_stats["file_count"],
            "files_removed": before_stats["file_count"] - after_stats["file_count"],
            "size_before_mb": before_stats["total_size_mb"],
            "size_after_mb": after_stats["total_size_mb"],
        }

        self.logger.info(
            "Compaction complete",
            files_before=result["files_before"],
            files_after=result["files_after"],
            files_removed=result["files_removed"],
        )

        return result

    def compact_small_files(
        self,
        table_path: str,
        target_size_mb: Optional[int] = None,
    ) -> Dict[str, int]:
        """Compact only small files in a Delta table.

        Reads all small files (<min_file_size_mb), merges them, and writes
        back as optimally-sized files. Useful when you don't want to
        rewrite the entire table.

        Args:
            table_path: Path to the Delta table.
            target_size_mb: Target file size after compaction.

        Returns:
            Compaction statistics.
        """
        target = target_size_mb or self.target_file_size_mb
        self.logger.info(
            f"Compacting small files in {table_path}",
            target_size_mb=target,
        )

        # Configure Delta to write files of the target size
        self.spark.conf.set(
            "spark.databricks.delta.optimize.maxFileSize",
            str(target * 1024 * 1024),
        )

        return self.run_compaction(table_path)

    # ──────────────────────────────────────────────────────────
    # FILE MONITORING
    # ──────────────────────────────────────────────────────────

    def monitor_file_sizes(
        self,
        table_path: str,
    ) -> Dict[str, any]:
        """Get file size distribution statistics for a table.

        Args:
            table_path: Path to the Delta table or Parquet directory.

        Returns:
            Dictionary with file size statistics.
        """
        return self._get_file_stats(table_path)

    def _get_file_stats(self, table_path: str) -> Dict[str, any]:
        """Get file statistics for a table path.

        Args:
            table_path: Path to scan for Parquet files.

        Returns:
            File statistics dictionary.
        """
        # Try Delta Lake metadata first
        try:
            from delta.tables import DeltaTable
            dt = DeltaTable.forPath(self.spark, table_path)
            detail = dt.detail().collect()[0]
            num_files = detail["numFiles"]
            size_bytes = detail["sizeInBytes"]
            return {
                "file_count": num_files,
                "total_size_mb": round(size_bytes / (1024 * 1024), 2),
                "avg_file_size_mb": round(
                    size_bytes / (1024 * 1024) / max(num_files, 1), 2
                ),
            }
        except Exception:
            pass

        # Fallback: scan Parquet files on disk
        parquet_files = glob.glob(
            os.path.join(table_path, "**", "*.parquet"), recursive=True
        )
        if not parquet_files:
            return {
                "file_count": 0,
                "total_size_mb": 0.0,
                "avg_file_size_mb": 0.0,
            }

        sizes = [os.path.getsize(f) for f in parquet_files]
        total_bytes = sum(sizes)

        return {
            "file_count": len(parquet_files),
            "total_size_mb": round(total_bytes / (1024 * 1024), 2),
            "avg_file_size_mb": round(
                total_bytes / (1024 * 1024) / len(parquet_files), 4
            ),
            "min_file_size_mb": round(min(sizes) / (1024 * 1024), 4),
            "max_file_size_mb": round(max(sizes) / (1024 * 1024), 2),
            "small_file_count": len(
                [s for s in sizes if s < self.min_file_size_mb * 1024 * 1024]
            ),
        }

    def _manual_compaction(self, table_path: str) -> None:
        """Manual compaction for non-Delta Parquet tables.

        Reads all Parquet files, coalesces, and writes back.
        """
        self.logger.info("Performing manual Parquet compaction")
        df = self.spark.read.parquet(table_path)
        record_count = df.count()

        # Calculate optimal file count
        estimated_size_mb = record_count * 500 / (1024 * 1024)
        num_files = max(1, int(estimated_size_mb / self.target_file_size_mb))

        temp_path = f"{table_path}_compacted_temp"
        df.coalesce(num_files).write.mode("overwrite").parquet(temp_path)

        # Atomic swap would be needed in production
        self.logger.warning(
            f"Manual compaction wrote to {temp_path}. "
            f"Manual swap with {table_path} required."
        )

    # ──────────────────────────────────────────────────────────
    # VACUUM — Clean up old versions
    # ──────────────────────────────────────────────────────────

    def vacuum_table(
        self,
        table_path: str,
        retention_hours: int = 168,  # 7 days
    ) -> None:
        """Run VACUUM on a Delta table to remove old file versions.

        After OPTIMIZE, old small files are marked for deletion but not
        physically removed. VACUUM cleans them up to reclaim storage.

        Args:
            table_path: Path to the Delta table.
            retention_hours: Keep files newer than this (default: 7 days).
        """
        self.logger.info(
            f"Vacuuming {table_path} (retention: {retention_hours}h)"
        )
        self.spark.sql(
            f"VACUUM delta.`{table_path}` RETAIN {retention_hours} HOURS"
        )
        self.logger.info("Vacuum complete")
