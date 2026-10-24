"""
Data Skew Handler — Production-Grade Utilities
===============================================
Provides detection and resolution strategies for data skew in PySpark jobs.

When joining high-volume event streams on skewed keys (e.g., a single
merchant_id generating 60% of traffic), a standard SortMergeJoin routes
millions of records to a single executor — triggering disk spills,
GC pressure, and OOM crashes. The remaining 199 tasks finish in seconds
while 1 task runs for hours.

This module implements four techniques to fix this:

1. **Key Distribution Analysis** — Detect skew before it causes problems
2. **Null/Default Key Isolation** — Separate problematic keys before joins
3. **Broadcast Hash Joins** — Eliminate shuffle entirely for small tables
4. **Deterministic Key Salting** — Split hot partitions across executors

Usage:
    >>> from src.optimizations.skew_handler import SkewHandler
    >>> handler = SkewHandler(spark)
    >>> report = handler.detect_skew(df, "merchant_id")
    >>> if report.is_skewed:
    ...     result = handler.salted_join(orders_df, merchant_dim_df, "merchant_id")
"""

from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.utils.logger import PipelineLogger


class SkewHandler:
    """Production-grade data skew detection and resolution.

    Attributes:
        spark: Active SparkSession.
        logger: Pipeline logger for tracking operations.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = PipelineLogger("skew-handler", layer="optimization")

    # ──────────────────────────────────────────────────────────
    # 1. DETECTION — Know your enemy before fighting
    # ──────────────────────────────────────────────────────────

    def detect_skew(
        self,
        df: DataFrame,
        key_col: str,
        threshold: float = 10.0,
    ) -> Dict[str, Any]:
        """Analyze key distribution to detect data skew.

        Computes per-key record counts and compares the largest partition
        to the median. A skew_ratio > threshold indicates problematic skew.

        Args:
            df: DataFrame to analyze.
            key_col: Column name to check for skew.
            threshold: Max-to-median ratio above which data is "skewed".

        Returns:
            Dictionary with skew analysis results:
                - is_skewed: bool
                - skew_ratio: float
                - total_records: int
                - unique_keys: int
                - top_keys: List of (key, count, percentage)
                - recommendation: str (suggested fix strategy)
        """
        self.logger.info(f"Detecting skew on column: {key_col}")

        # Count records per key value
        key_counts = (
            df.groupBy(key_col)
            .agg(F.count("*").alias("cnt"))
            .orderBy(F.desc("cnt"))
            .cache()  # Cache because we read this multiple times
        )

        total = df.count()
        unique = key_counts.count()

        # Get distribution statistics
        stats = key_counts.agg(
            F.max("cnt").alias("max_cnt"),
            F.min("cnt").alias("min_cnt"),
            F.avg("cnt").alias("avg_cnt"),
            F.expr("percentile_approx(cnt, 0.5)").alias("median_cnt"),
        ).collect()[0]

        max_cnt = stats["max_cnt"]
        median_cnt = max(stats["median_cnt"], 1)
        skew_ratio = round(max_cnt / median_cnt, 2)

        # Get top 10 hottest keys
        top_rows = key_counts.limit(10).collect()
        top_keys = [
            (str(row[key_col]), row["cnt"], round(row["cnt"] / total * 100, 2))
            for row in top_rows
        ]

        # Determine recommendation based on severity
        is_skewed = skew_ratio > threshold
        if not is_skewed:
            recommendation = "No action needed — distribution is balanced."
        elif top_keys and top_keys[0][2] > 50:
            recommendation = (
                "SEVERE SKEW: Use null key isolation + broadcast join + key salting. "
                "The hottest key has >50% of records."
            )
        elif skew_ratio > 50:
            recommendation = (
                "HIGH SKEW: Enable AQE skew join + consider key salting for aggregations."
            )
        else:
            recommendation = (
                "MODERATE SKEW: AQE should handle this. Enable broadcast joins for dim tables."
            )

        key_counts.unpersist()

        result = {
            "is_skewed": is_skewed,
            "skew_ratio": skew_ratio,
            "total_records": total,
            "unique_keys": unique,
            "max_partition_size": max_cnt,
            "median_partition_size": median_cnt,
            "top_keys": top_keys,
            "recommendation": recommendation,
        }

        self.logger.info(
            f"Skew analysis complete",
            skew_ratio=skew_ratio,
            is_skewed=is_skewed,
        )
        return result

    # ──────────────────────────────────────────────────────────
    # 2. NULL KEY ISOLATION — Step 1 of the fix
    # ──────────────────────────────────────────────────────────

    def isolate_null_keys(
        self,
        df: DataFrame,
        key_col: str,
        null_values: Optional[List[str]] = None,
    ) -> Tuple[DataFrame, DataFrame]:
        """Separate null/default key records from normal records.

        Null or default keys (e.g., "", "NULL", "UNKNOWN") cannot join
        meaningfully and generate massive partitions. By isolating them:
        - The join operates only on valid keys (balanced distribution)
        - Null records are processed separately (or assigned defaults)
        - They're unioned back after the join

        Args:
            df: Input DataFrame.
            key_col: Column to check for null/default values.
            null_values: List of values to treat as null. Defaults to
                         [None, "", "NULL", "UNKNOWN", "DEFAULT", "N/A"].

        Returns:
            Tuple of (normal_df, null_df) where:
                - normal_df: Records with valid join keys
                - null_df: Records with null/default keys
        """
        if null_values is None:
            null_values = ["", "NULL", "UNKNOWN", "DEFAULT", "N/A"]

        self.logger.info(f"Isolating null/default keys from column: {key_col}")

        # Build the null condition: column IS NULL OR column IN (null_values)
        null_condition = F.col(key_col).isNull()
        if null_values:
            null_condition = null_condition | F.col(key_col).isin(null_values)

        # Split into two DataFrames
        normal_df = df.filter(~null_condition)
        null_df = df.filter(null_condition)

        normal_count = normal_df.count()
        null_count = null_df.count()
        total = normal_count + null_count

        self.logger.info(
            f"Key isolation complete",
            normal_records=normal_count,
            null_records=null_count,
            null_percentage=round(null_count / max(total, 1) * 100, 2),
        )

        return normal_df, null_df

    # ──────────────────────────────────────────────────────────
    # 3. SALTED JOIN — The heavy artillery for extreme skew
    # ──────────────────────────────────────────────────────────

    def salted_join(
        self,
        left_df: DataFrame,
        right_df: DataFrame,
        key_col: str,
        num_salts: int = 10,
        join_type: str = "inner",
    ) -> DataFrame:
        """Perform a salted join to distribute skewed keys across partitions.

        Instead of all records for "MERCHANT_001" going to a single executor,
        we append a random salt (0-N) to the key on the left side and
        explode the right side with all salt values. This spreads the
        hot key across N partitions.

        Before salting:
            Left:  (MERCHANT_001, data) → ALL go to partition 42
            Right: (MERCHANT_001, dim)  → Also partition 42
            Result: Partition 42 has 60% of all data = OOM

        After salting (N=10):
            Left:  (MERCHANT_001_3, data) → partition 12
                   (MERCHANT_001_7, data) → partition 28
                   (MERCHANT_001_1, data) → partition 5
            Right: (MERCHANT_001_0, dim), (MERCHANT_001_1, dim), ...
            Result: Hot key spread across 10 partitions = balanced

        Args:
            left_df: Large (fact) DataFrame with the skewed key.
            right_df: Smaller (dimension) DataFrame to join against.
            key_col: Join key column name (present in both DataFrames).
            num_salts: Number of salt buckets (higher = more parallelism).
            join_type: Join type ("inner", "left", "left_outer", etc.).

        Returns:
            Joined DataFrame with salt columns removed.
        """
        self.logger.info(
            f"Performing salted join on '{key_col}' with {num_salts} buckets"
        )

        # Step 1: Add random salt to left (large) table
        # Uses deterministic floor(rand() * N) — same record always gets
        # the same salt within a single execution, ensuring correctness.
        salted_left = left_df.withColumn(
            "_salt",
            F.floor(F.rand() * num_salts).cast("int"),
        ).withColumn(
            "_salted_key",
            F.concat(F.col(key_col).cast(StringType()), F.lit("_"), F.col("_salt")),
        )

        # Step 2: Explode right (small) table with all salt values
        # Each right-side row gets replicated N times, once per salt bucket.
        # This is acceptable because the right table is small (dimension table).
        salt_range = self.spark.range(num_salts).withColumnRenamed("id", "_salt")

        salted_right = right_df.crossJoin(salt_range).withColumn(
            "_salted_key",
            F.concat(
                F.col(key_col).cast(StringType()), F.lit("_"), F.col("_salt")
            ),
        )

        # Step 3: Join on the salted key instead of the original key
        result = salted_left.join(
            salted_right,
            on="_salted_key",
            how=join_type,
        )

        # Step 4: Clean up — remove salt columns, resolve ambiguous key column
        columns_to_drop = ["_salt", "_salted_key"]

        # Handle duplicate key columns from the join
        left_cols = [c for c in left_df.columns if c != key_col]
        right_cols = [c for c in right_df.columns if c != key_col]

        # Select left's key column + all non-key columns from both sides
        select_cols = [salted_left[key_col]]
        select_cols += [salted_left[c] for c in left_cols]
        select_cols += [salted_right[c] for c in right_cols]

        result = result.select(*select_cols)

        self.logger.info("Salted join complete")
        return result

    # ──────────────────────────────────────────────────────────
    # 4. SALTED AGGREGATION — Two-stage partial-then-final
    # ──────────────────────────────────────────────────────────

    def salted_aggregation(
        self,
        df: DataFrame,
        key_col: str,
        agg_exprs: Dict[str, str],
        num_salts: int = 10,
    ) -> DataFrame:
        """Perform a two-stage salted aggregation for skewed keys.

        Standard groupBy on a skewed key sends 60% of data to one reducer.
        Salted aggregation works in two stages:

        Stage 1 (Partial): Group by (key + salt) — spreads hot keys across
                           N reducers, each processing 1/N of the hot key's data.

        Stage 2 (Final):   Group by (key) — combines the N partial aggregates.
                           Now each reducer only combines N small partial results
                           instead of processing millions of raw records.

        Args:
            df: DataFrame to aggregate.
            key_col: Column to group by (the skewed key).
            agg_exprs: Dict mapping column_name → agg_function.
                       Supported: "sum", "count", "avg", "min", "max".
                       Example: {"total_amount": "sum", "order_id": "count"}
            num_salts: Number of salt buckets for stage 1.

        Returns:
            Aggregated DataFrame with final results.
        """
        self.logger.info(
            f"Starting two-stage salted aggregation on '{key_col}'"
        )

        # ── Stage 1: Partial aggregation with salted keys ──
        salted_df = df.withColumn(
            "_salted_key",
            F.concat(
                F.col(key_col).cast(StringType()),
                F.lit("_"),
                F.floor(F.rand() * num_salts).cast("string"),
            ),
        )

        # Build stage-1 aggregation expressions
        stage1_aggs = []
        for col_name, agg_func in agg_exprs.items():
            if agg_func == "sum":
                stage1_aggs.append(F.sum(col_name).alias(f"_partial_{col_name}"))
            elif agg_func == "count":
                stage1_aggs.append(F.count(col_name).alias(f"_partial_{col_name}"))
            elif agg_func == "min":
                stage1_aggs.append(F.min(col_name).alias(f"_partial_{col_name}"))
            elif agg_func == "max":
                stage1_aggs.append(F.max(col_name).alias(f"_partial_{col_name}"))
            elif agg_func == "avg":
                # For avg, we need sum + count to combine in stage 2
                stage1_aggs.append(F.sum(col_name).alias(f"_partial_sum_{col_name}"))
                stage1_aggs.append(F.count(col_name).alias(f"_partial_count_{col_name}"))

        partial_df = salted_df.groupBy("_salted_key", key_col).agg(*stage1_aggs)

        # ── Stage 2: Final aggregation on original key ──
        stage2_aggs = []
        for col_name, agg_func in agg_exprs.items():
            if agg_func in ("sum", "count"):
                # Sum of partial sums/counts = total sum/count
                stage2_aggs.append(
                    F.sum(f"_partial_{col_name}").alias(col_name)
                )
            elif agg_func == "min":
                stage2_aggs.append(
                    F.min(f"_partial_{col_name}").alias(col_name)
                )
            elif agg_func == "max":
                stage2_aggs.append(
                    F.max(f"_partial_{col_name}").alias(col_name)
                )
            elif agg_func == "avg":
                # avg = total_sum / total_count
                stage2_aggs.append(
                    (F.sum(f"_partial_sum_{col_name}") /
                     F.sum(f"_partial_count_{col_name}")).alias(col_name)
                )

        result = partial_df.groupBy(key_col).agg(*stage2_aggs)

        self.logger.info("Two-stage salted aggregation complete")
        return result

    # ──────────────────────────────────────────────────────────
    # 5. AQE CONFIGURATION — Let Spark handle it automatically
    # ──────────────────────────────────────────────────────────

    def configure_aqe_skew(self, skew_factor: int = 5) -> None:
        """Configure Spark's Adaptive Query Execution for skew handling.

        AQE dynamically detects skewed partitions during SortMergeJoin and
        splits them into smaller sub-partitions at runtime. This is the
        easiest fix — just enable it and Spark does the work.

        However, AQE alone may not be sufficient for extreme skew (>50x).
        Combine with broadcast joins and key salting for best results.

        Args:
            skew_factor: Partition is skewed if > N× the median size.
                         Lower values are more aggressive at splitting.
        """
        self.logger.info(f"Configuring AQE with skew factor: {skew_factor}")

        configs = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor": str(skew_factor),
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "268435456",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "134217728",
        }

        for key, value in configs.items():
            self.spark.conf.set(key, value)
            self.logger.info(f"  Set {key} = {value}")

    # ──────────────────────────────────────────────────────────
    # COMBINED FIX — The full playbook
    # ──────────────────────────────────────────────────────────

    def fix_skewed_join(
        self,
        fact_df: DataFrame,
        dim_df: DataFrame,
        key_col: str,
        dim_size_mb: float = 50.0,
        null_values: Optional[List[str]] = None,
    ) -> DataFrame:
        """Apply the complete skew-fix playbook for a join operation.

        This is the recommended entry point. It applies all four techniques
        in the correct order:

        1. Enable AQE (safety net for moderate skew)
        2. Isolate null/default keys (remove noise)
        3. If dim table is small → broadcast join (eliminates shuffle)
        4. If dim table is large → salted join (distributes hot keys)
        5. Union null records back

        Args:
            fact_df: Large fact table (e.g., orders, clickstream).
            dim_df: Dimension table (e.g., merchant dimensions).
            key_col: Join key column name.
            dim_size_mb: Estimated size of dim table in MB.
            null_values: Values to treat as null/default.

        Returns:
            Joined DataFrame with skew handled.
        """
        self.logger.info(f"Applying full skew-fix playbook for join on '{key_col}'")

        # Step 1: Enable AQE as a safety net
        self.configure_aqe_skew()

        # Step 2: Isolate null/default keys
        normal_df, null_df = self.isolate_null_keys(fact_df, key_col, null_values)

        # Step 3: Choose join strategy based on dim table size
        if dim_size_mb < 100:
            # Broadcast join — no shuffle at all!
            # The entire dim table is sent to every executor.
            self.logger.info(
                f"Using BROADCAST join (dim table ~{dim_size_mb}MB < 100MB threshold)"
            )
            joined_df = normal_df.join(
                F.broadcast(dim_df),
                on=key_col,
                how="left",
            )
        else:
            # Salted join for large dimension tables
            self.logger.info(
                f"Using SALTED join (dim table ~{dim_size_mb}MB >= 100MB threshold)"
            )
            joined_df = self.salted_join(
                normal_df, dim_df, key_col, num_salts=10, join_type="left"
            )

        # Step 4: Process null records separately
        # Add dim columns as nulls for null-key records
        dim_cols = [c for c in dim_df.columns if c != key_col]
        for col_name in dim_cols:
            null_df = null_df.withColumn(col_name, F.lit(None))

        # Step 5: Union back
        result = joined_df.unionByName(null_df, allowMissingColumns=True)

        self.logger.info("Skew-fix playbook complete")
        return result
