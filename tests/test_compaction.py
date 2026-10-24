"""
Tests for the Small Files Compaction Engine.
"""

import os
import pytest
from pyspark.sql import functions as F
from src.optimizations.compaction import CompactionEngine


class TestWriteOptimization:
    """Tests for write-time file optimization."""

    def test_optimize_write_partitioning_returns_dataframe(
        self, spark, skewed_df
    ):
        """Verify repartitioning returns a valid DataFrame."""
        engine = CompactionEngine(spark)

        # Add a partition column
        df = skewed_df.withColumn("event_date", F.lit("2024-01-15"))

        result = engine.optimize_write_partitioning(df, "event_date")

        assert result.count() == skewed_df.count()

    def test_coalesce_reduces_partitions(self, spark, skewed_df):
        """Verify coalesce produces fewer partitions."""
        engine = CompactionEngine(spark)

        # Force many partitions
        df = skewed_df.repartition(20)
        assert df.rdd.getNumPartitions() == 20

        result = engine.coalesce_for_write(df, num_files=2)
        assert result.rdd.getNumPartitions() == 2
        assert result.count() == skewed_df.count()


class TestFileMonitoring:
    """Tests for file size monitoring."""

    def test_monitor_file_sizes_reports_correctly(
        self, spark, skewed_df, temp_delta_path
    ):
        """Verify file monitoring returns correct statistics."""
        # Write some data
        skewed_df.write.mode("overwrite").parquet(temp_delta_path)

        engine = CompactionEngine(spark)
        stats = engine.monitor_file_sizes(temp_delta_path)

        assert stats["file_count"] > 0
        assert stats["total_size_mb"] > 0

    def test_monitor_empty_path(self, spark, tmp_path):
        """Verify monitoring handles empty paths gracefully."""
        engine = CompactionEngine(spark)
        stats = engine.monitor_file_sizes(str(tmp_path / "nonexistent"))

        assert stats["file_count"] == 0


class TestCompaction:
    """Tests for file compaction operations."""

    def test_compact_small_files_preserves_data(
        self, spark, skewed_df, temp_delta_path
    ):
        """Verify compaction preserves all records."""
        # Write many small files
        skewed_df.repartition(20).write.mode("overwrite").parquet(temp_delta_path)

        engine = CompactionEngine(spark)
        before_count = spark.read.parquet(temp_delta_path).count()

        # Manual read-and-rewrite compaction
        df = spark.read.parquet(temp_delta_path)
        compacted_path = temp_delta_path + "_compacted"
        df.coalesce(2).write.mode("overwrite").parquet(compacted_path)

        after_count = spark.read.parquet(compacted_path).count()
        assert after_count == before_count
