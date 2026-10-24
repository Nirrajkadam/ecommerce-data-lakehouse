"""
Write-Time Optimization Module
===============================
Smart repartitioning and file layout optimization applied during pipeline
writes to prevent the small files problem at its source.
"""

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.logger import PipelineLogger


class WriteOptimizer:
    """Optimizes DataFrame writes for minimal file fragmentation.

    Applies intelligent repartitioning based on data volume, partition
    columns, and target file sizes to produce well-sized output files.
    """

    def __init__(self, spark: SparkSession, target_file_size_mb: int = 256):
        self.spark = spark
        self.target_file_size_mb = target_file_size_mb
        self.logger = PipelineLogger("write-optimizer", layer="optimization")

    def optimize_for_write(
        self,
        df: DataFrame,
        partition_col: Optional[str] = None,
        estimated_row_bytes: int = 500,
    ) -> DataFrame:
        """Auto-tune the number of output files based on data volume.

        Calculates the optimal number of output partitions to produce
        files close to the target size, then repartitions accordingly.

        Args:
            df: DataFrame to optimize.
            partition_col: Column to partition by (e.g., "event_date").
            estimated_row_bytes: Average row size in bytes for estimation.

        Returns:
            Repartitioned DataFrame.
        """
        record_count = df.count()
        estimated_size_mb = (record_count * estimated_row_bytes) / (1024 * 1024)

        num_partitions = max(1, int(estimated_size_mb / self.target_file_size_mb) + 1)

        self.logger.info(
            f"Auto-tuned to {num_partitions} partitions",
            record_count=record_count,
            estimated_size_mb=round(estimated_size_mb, 2),
            target_file_size_mb=self.target_file_size_mb,
        )

        if partition_col:
            return df.repartition(num_partitions, partition_col)
        else:
            return df.repartition(num_partitions)

    def write_optimized(
        self,
        df: DataFrame,
        path: str,
        partition_col: Optional[str] = None,
        format: str = "delta",
        mode: str = "append",
    ) -> None:
        """Write DataFrame with automatic file size optimization.

        Combines repartitioning with write in a single call.

        Args:
            df: DataFrame to write.
            path: Output path.
            partition_col: Column to partition by.
            format: Output format ("delta", "parquet").
            mode: Write mode ("append", "overwrite").
        """
        optimized_df = self.optimize_for_write(df, partition_col)

        writer = optimized_df.write.format(format).mode(mode)

        if partition_col:
            writer = writer.partitionBy(partition_col)

        writer.save(path)

        self.logger.info(f"Optimized write complete to {path}", format=format, mode=mode)
