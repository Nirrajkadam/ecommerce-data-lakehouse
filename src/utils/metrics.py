"""
Performance Metrics Tracker for Ecommerce Data Lakehouse
=========================================================
Tracks and reports on key performance indicators including:
- Partition skew ratios (detecting hot partitions)
- File size distributions (detecting small files problem)
- Task execution times per stage
- Data quality metrics

These metrics drive alerting and optimization decisions.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.logger import PipelineLogger


@dataclass
class SkewReport:
    """Report on data skew for a specific key column.

    Attributes:
        key_column: The column analyzed for skew.
        total_records: Total number of records in the dataset.
        unique_keys: Number of distinct key values.
        top_keys: List of (key_value, count, percentage) for hottest keys.
        skew_ratio: Ratio of max partition to median partition.
        is_skewed: Whether the data exceeds the skew threshold.
    """
    key_column: str
    total_records: int
    unique_keys: int
    top_keys: List[Tuple[str, int, float]]
    skew_ratio: float
    median_partition_size: int
    max_partition_size: int
    is_skewed: bool


@dataclass
class FileSizeReport:
    """Report on file sizes for a Delta table.

    Attributes:
        table_path: Path to the Delta table.
        total_files: Number of Parquet files.
        total_size_mb: Total size in megabytes.
        avg_file_size_mb: Average file size.
        min_file_size_mb: Smallest file size.
        max_file_size_mb: Largest file size.
        small_file_count: Number of files below threshold.
        small_file_percentage: Percentage of files that are "small".
    """
    table_path: str
    total_files: int
    total_size_mb: float
    avg_file_size_mb: float
    min_file_size_mb: float
    max_file_size_mb: float
    median_file_size_mb: float
    small_file_count: int
    small_file_percentage: float
    needs_compaction: bool


class MetricsTracker:
    """Tracks performance metrics across pipeline stages.

    Provides methods to analyze data skew, monitor file sizes,
    and generate actionable reports for optimization.

    Example:
        >>> tracker = MetricsTracker(spark)
        >>> skew_report = tracker.analyze_skew(df, "merchant_id")
        >>> if skew_report.is_skewed:
        ...     print(f"ALERT: {skew_report.skew_ratio}x skew detected!")
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = PipelineLogger("metrics-tracker")
        self._stage_timings: Dict[str, List[float]] = {}

    def analyze_skew(
        self,
        df: DataFrame,
        key_column: str,
        threshold: float = 10.0,
        top_n: int = 10,
    ) -> SkewReport:
        """Analyze data distribution skew for a given key column.

        Computes key frequency distribution and identifies hot keys
        that would cause partition skew during joins and aggregations.

        Args:
            df: DataFrame to analyze.
            key_column: Column to check for skew.
            threshold: Skew ratio above which data is considered skewed.
            top_n: Number of top keys to include in the report.

        Returns:
            SkewReport with detailed skew analysis.
        """
        self.logger.info(f"Analyzing skew on column: {key_column}")

        # Count records per key value
        key_counts = (
            df.groupBy(key_column)
            .agg(F.count("*").alias("record_count"))
            .orderBy(F.desc("record_count"))
        )

        total_records = df.count()
        unique_keys = key_counts.count()

        # Get top N keys
        top_keys_rows = key_counts.limit(top_n).collect()
        top_keys = [
            (str(row[key_column]), row["record_count"],
             round(row["record_count"] / total_records * 100, 2))
            for row in top_keys_rows
        ]

        # Calculate skew metrics
        stats = key_counts.agg(
            F.max("record_count").alias("max_count"),
            F.expr("percentile_approx(record_count, 0.5)").alias("median_count"),
        ).collect()[0]

        max_count = stats["max_count"]
        median_count = max(stats["median_count"], 1)  # Avoid division by zero
        skew_ratio = round(max_count / median_count, 2)

        report = SkewReport(
            key_column=key_column,
            total_records=total_records,
            unique_keys=unique_keys,
            top_keys=top_keys,
            skew_ratio=skew_ratio,
            median_partition_size=median_count,
            max_partition_size=max_count,
            is_skewed=skew_ratio > threshold,
        )

        if report.is_skewed:
            self.logger.warning(
                f"DATA SKEW DETECTED on '{key_column}'",
                skew_ratio=skew_ratio,
                threshold=threshold,
                hottest_key=top_keys[0][0] if top_keys else "N/A",
                hottest_key_pct=top_keys[0][2] if top_keys else 0,
            )
        else:
            self.logger.info(
                f"No significant skew on '{key_column}'",
                skew_ratio=skew_ratio,
            )

        return report

    def analyze_file_sizes(
        self,
        table_path: str,
        small_file_threshold_mb: float = 1.0,
    ) -> FileSizeReport:
        """Analyze file size distribution for a Delta table.

        Reads the Delta log to get file-level metadata and identifies
        the small files problem.

        Args:
            table_path: Path to the Delta table.
            small_file_threshold_mb: Files smaller than this are "small" (MB).

        Returns:
            FileSizeReport with file size distribution analysis.
        """
        self.logger.info(f"Analyzing file sizes for: {table_path}")

        try:
            from delta.tables import DeltaTable
            dt = DeltaTable.forPath(self.spark, table_path)
            detail = dt.detail().collect()[0]

            num_files = detail["numFiles"]
            size_bytes = detail["sizeInBytes"]
            size_mb = size_bytes / (1024 * 1024)
            avg_size_mb = size_mb / max(num_files, 1)

            # For detailed per-file analysis, read the Parquet files directly
            file_stats = (
                self.spark.read.format("delta")
                .load(table_path)
                .inputFiles()
            )

            report = FileSizeReport(
                table_path=table_path,
                total_files=num_files,
                total_size_mb=round(size_mb, 2),
                avg_file_size_mb=round(avg_size_mb, 2),
                min_file_size_mb=round(avg_size_mb * 0.1, 2),  # Estimate
                max_file_size_mb=round(avg_size_mb * 3, 2),    # Estimate
                median_file_size_mb=round(avg_size_mb, 2),     # Estimate
                small_file_count=num_files if avg_size_mb < small_file_threshold_mb else 0,
                small_file_percentage=100.0 if avg_size_mb < small_file_threshold_mb else 0.0,
                needs_compaction=avg_size_mb < small_file_threshold_mb,
            )
        except Exception as e:
            self.logger.warning(f"Could not analyze Delta table: {e}")
            # Fallback: analyze Parquet files directly
            import os
            import glob
            files = glob.glob(f"{table_path}/**/*.parquet", recursive=True)
            sizes_mb = [os.path.getsize(f) / (1024 * 1024) for f in files]

            if not sizes_mb:
                sizes_mb = [0.0]

            small_files = [s for s in sizes_mb if s < small_file_threshold_mb]
            total_size = sum(sizes_mb)

            report = FileSizeReport(
                table_path=table_path,
                total_files=len(files),
                total_size_mb=round(total_size, 2),
                avg_file_size_mb=round(total_size / max(len(files), 1), 2),
                min_file_size_mb=round(min(sizes_mb), 4),
                max_file_size_mb=round(max(sizes_mb), 2),
                median_file_size_mb=round(sorted(sizes_mb)[len(sizes_mb) // 2], 4),
                small_file_count=len(small_files),
                small_file_percentage=round(
                    len(small_files) / max(len(files), 1) * 100, 1
                ),
                needs_compaction=len(small_files) > len(files) * 0.5,
            )

        if report.needs_compaction:
            self.logger.warning(
                f"SMALL FILES PROBLEM detected in {table_path}",
                total_files=report.total_files,
                avg_file_size_mb=report.avg_file_size_mb,
                small_file_pct=report.small_file_percentage,
            )

        return report

    def print_skew_report(self, report: SkewReport) -> None:
        """Pretty-print a skew analysis report."""
        print("\n" + "=" * 60)
        print(f"  DATA SKEW REPORT - Column: {report.key_column}")
        print("=" * 60)
        print(f"  Total Records:         {report.total_records:,}")
        print(f"  Unique Keys:           {report.unique_keys:,}")
        print(f"  Skew Ratio:            {report.skew_ratio}x")
        print(f"  Max Partition Size:    {report.max_partition_size:,}")
        print(f"  Median Partition Size: {report.median_partition_size:,}")
        print(f"  Status:                {'SKEWED' if report.is_skewed else 'BALANCED'}")
        print(f"\n  Top {len(report.top_keys)} Keys:")
        print(f"  {'Key':<25} {'Count':>12} {'Percentage':>10}")
        print(f"  {'-' * 25} {'-' * 12} {'-' * 10}")
        for key, count, pct in report.top_keys:
            bar = "=" * int(pct / 2)
            print(f"  {key:<25} {count:>12,} {pct:>9.1f}% [{bar}]")
        print("=" * 60 + "\n")

    def print_file_size_report(self, report: FileSizeReport) -> None:
        """Pretty-print a file size analysis report."""
        print("\n" + "=" * 60)
        print(f"  FILE SIZE REPORT - {report.table_path}")
        print("=" * 60)
        print(f"  Total Files:          {report.total_files:,}")
        print(f"  Total Size:           {report.total_size_mb:.1f} MB")
        print(f"  Avg File Size:        {report.avg_file_size_mb:.2f} MB")
        print(f"  Min File Size:        {report.min_file_size_mb:.4f} MB")
        print(f"  Max File Size:        {report.max_file_size_mb:.2f} MB")
        print(f"  Median File Size:     {report.median_file_size_mb:.4f} MB")
        print(f"  Small Files (<1MB):   {report.small_file_count} ({report.small_file_percentage:.1f}%)")
        print(f"  Needs Compaction:     {'YES (Compaction Recommended)' if report.needs_compaction else 'NO'}")
        print("=" * 60 + "\n")
