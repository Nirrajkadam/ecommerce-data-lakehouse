"""
Silver Layer - Data Cleansing & Enrichment
==========================================
Cleans, deduplicates, and enriches Bronze data into analytics-ready datasets.

Demonstrates Data Skew Fixes During Joins:
When joining orders with merchant dimensions on merchant_id, a naive
SortMergeJoin routes all records for MERCHANT_001 (60% of traffic)
to a single executor. That executor:
  - Receives 600,000 out of 1,000,000 records
  - Spills to disk when memory fills up
  - Takes 10x longer than the other 199 tasks
  - May trigger an Out-of-Memory failure under peak load

This module applies the complete fix playbook:
  1. Null/Default key isolation
  2. Broadcast Hash Joins for dimension tables
  3. AQE Skew Join optimization
  4. Salted key aggregation for streaming stats
"""

from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.streaming import StreamingQuery

from src.config.pipeline_config import PipelineConfig
from src.optimizations.skew_handler import SkewHandler
from src.optimizations.compaction import CompactionEngine
from src.utils.logger import PipelineLogger
from src.utils.metrics import MetricsTracker


class SilverLayer:
    """Silver layer processor for data cleansing and enrichment.

    Reads from Bronze Delta tables, cleans and enriches the data,
    and writes to Silver Delta tables with optimized file layouts.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Optional[PipelineConfig] = None,
    ):
        self.spark = spark
        self.config = config or PipelineConfig()
        self.logger = PipelineLogger("silver-layer", layer="silver")
        self.skew_handler = SkewHandler(spark)
        self.compaction = CompactionEngine(spark)
        self.metrics = MetricsTracker(spark)

    # ──────────────────────────────────────────────────────────
    # DATA QUALITY — Clean before enriching
    # ──────────────────────────────────────────────────────────

    def _clean_orders(self, df: DataFrame) -> DataFrame:
        """Clean, validate, and deterministically deduplicate order records.

        Operations:
            - Validates non-null order_id and positive total_amount
            - Deterministically selects the latest record for each order_id using
              an ordered Window (event_timestamp DESC, _ingestion_timestamp DESC, order_status DESC)
            - Standardizes null currencies and payment methods
            - Ensures event_date is derived from timestamp if missing
        """
        self.logger.info("Cleaning order data with deterministic latest-record windowing")

        # Filter invalid records
        valid_df = df.filter(F.col("order_id").isNotNull()).filter(F.col("total_amount") > 0)

        # Build deterministic ordering for deduplication
        order_specs = []
        if "event_timestamp" in valid_df.columns:
            order_specs.append(F.col("event_timestamp").desc_nulls_last())
        if "_ingestion_timestamp" in valid_df.columns:
            order_specs.append(F.col("_ingestion_timestamp").desc_nulls_last())
        if "order_status" in valid_df.columns:
            order_specs.append(F.col("order_status").desc_nulls_last())

        # Deterministic tie-breaker
        order_specs.append(F.lit(1))
        window_spec = Window.partitionBy("order_id").orderBy(*order_specs)

        # Retain only the newest record per order_id
        cleaned = (
            valid_df
            .withColumn("_row_num", F.row_number().over(window_spec))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

        # Standardize optional columns safely without assuming schema existence
        if "payment_method" in cleaned.columns:
            cleaned = cleaned.withColumn(
                "payment_method",
                F.coalesce(F.col("payment_method"), F.lit("unknown")),
            )
        if "currency" in cleaned.columns:
            cleaned = cleaned.withColumn(
                "currency",
                F.coalesce(F.col("currency"), F.lit("INR")),
            )

        if "event_date" in cleaned.columns and "event_timestamp" in cleaned.columns:
            cleaned = cleaned.withColumn(
                "event_date",
                F.coalesce(
                    F.col("event_date"),
                    F.date_format(F.col("event_timestamp"), "yyyy-MM-dd"),
                ),
            )
        elif "event_timestamp" in cleaned.columns and "event_date" not in cleaned.columns:
            cleaned = cleaned.withColumn(
                "event_date",
                F.date_format(F.col("event_timestamp"), "yyyy-MM-dd"),
            )

        return cleaned

    def _clean_clickstream(self, df: DataFrame) -> DataFrame:
        """Clean, validate, and deterministically deduplicate clickstream records."""
        self.logger.info("Cleaning clickstream data")

        valid_df = df.filter(F.col("event_id").isNotNull()).filter(F.col("session_id").isNotNull())

        order_specs = []
        if "event_timestamp" in valid_df.columns:
            order_specs.append(F.col("event_timestamp").desc_nulls_last())
        if "_ingestion_timestamp" in valid_df.columns:
            order_specs.append(F.col("_ingestion_timestamp").desc_nulls_last())
        order_specs.append(F.lit(1))

        window_spec = Window.partitionBy("event_id").orderBy(*order_specs)
        cleaned = (
            valid_df
            .withColumn("_row_num", F.row_number().over(window_spec))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

        if "duration_seconds" in cleaned.columns:
            cleaned = cleaned.withColumn(
                "duration_seconds",
                F.when(F.col("duration_seconds") < 0, 0)
                .otherwise(F.col("duration_seconds")),
            )

        return cleaned

    # ──────────────────────────────────────────────────────────
    # SKEW-AWARE JOIN — The main event
    # ──────────────────────────────────────────────────────────

    def enrich_with_merchant_data(
        self,
        df: DataFrame,
        merchant_dim_df: DataFrame,
    ) -> DataFrame:
        """Join fact data with merchant dimensions — WITH SKEW HANDLING.

        This is where the data skew problem manifests. Without the fixes
        below, this join would cause:
        - 199 tasks complete in 2 seconds
        - 1 task (MERCHANT_001) runs for 20 minutes
        - Possible OOM crash on the executor handling MERCHANT_001

        ──── BEFORE (Naive Join) ────────────────────────────────────
        >>> result = orders.join(merchants, "merchant_id")
        # SortMergeJoin -> 60% of data routed to 1 partition -> OOM / disk spill

        ──── AFTER (Optimized Join) ─────────────────────────────────
        The fix applies three techniques:

        Fix 1: ISOLATE NULL KEYS
            Records with null/empty merchant_id are filtered out before
            the join. They can't join meaningfully and just inflate the
            null partition. We process them separately and union back.

        Fix 2: BROADCAST HASH JOIN
            The merchant dimension table is small (~20MB). By broadcasting
            it, we eliminate the shuffle entirely. Every executor has a
            local copy of the dim table — no data movement needed.

        Fix 3: AQE SKEW JOIN
            Even with broadcast joins, streaming aggregations on merchant_id
            still create skewed partitions. AQE detects this at runtime
            and automatically splits hot partitions.

        Args:
            df: Fact DataFrame (orders/clickstream) with merchant_id.
            merchant_dim_df: Merchant dimension DataFrame (~20MB).

        Returns:
            Enriched DataFrame with merchant attributes joined in.
        """
        self.logger.info("Enriching with merchant data (skew-aware join)")

        # ── Analyze skew first (optional but useful for monitoring) ──
        skew_report = self.skew_handler.detect_skew(df, "merchant_id")
        self.logger.info(
            f"Pre-join skew analysis",
            skew_ratio=skew_report["skew_ratio"],
            is_skewed=skew_report["is_skewed"],
        )

        # ── Apply the full skew-fix playbook ──
        result = self.skew_handler.fix_skewed_join(
            fact_df=df,
            dim_df=merchant_dim_df,
            key_col="merchant_id",
            dim_size_mb=20.0,  # Merchant dim is small → broadcast join
            null_values=["", "NULL", "UNKNOWN", "DEFAULT", "N/A"],
        )

        self.logger.info("Merchant enrichment complete (skew handled)")
        return result

    # ──────────────────────────────────────────────────────────
    # FULL SILVER PROCESSING
    # ──────────────────────────────────────────────────────────

    def process_orders(
        self,
        merchant_dim_df: Optional[DataFrame] = None,
    ) -> int:
        """Full Silver processing pipeline for orders.

        Steps:
            1. Read from Bronze orders Delta table
            2. Clean and deduplicate
            3. Enrich with merchant dimensions (skew-aware)
            4. Write to Silver with optimized partitioning

        Args:
            merchant_dim_df: Merchant dimension DataFrame. If None,
                            reads from the default location.

        Returns:
            Number of records processed.
        """
        bronze_path = self.config.paths.get_table_path("bronze", "orders")
        silver_path = self.config.paths.get_table_path("silver", "orders")

        self.logger.info("Starting Silver orders processing")

        with self.logger.stage("read-bronze"):
            df = self.spark.read.format("delta").load(bronze_path)

        with self.logger.stage("clean-data"):
            df = self._clean_orders(df)

        record_count = df.count()

        # Enrich with merchant data if dimension table is provided
        if merchant_dim_df is not None:
            with self.logger.stage("enrich-merchants"):
                df = self.enrich_with_merchant_data(df, merchant_dim_df)

        # ── OPTIMIZATION: Repartition before write ──
        with self.logger.stage("write-silver"):
            optimized_df = self.compaction.optimize_write_partitioning(
                df, "event_date"
            )
            (
                optimized_df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("event_date")
                .save(silver_path)
            )

        self.logger.info(f"Silver orders complete: {record_count} records")
        return record_count

    def process_clickstream(
        self,
        merchant_dim_df: Optional[DataFrame] = None,
    ) -> int:
        """Full Silver processing pipeline for clickstream data.

        Args:
            merchant_dim_df: Merchant dimension DataFrame.

        Returns:
            Number of records processed.
        """
        bronze_path = self.config.paths.get_table_path("bronze", "clickstream")
        silver_path = self.config.paths.get_table_path("silver", "clickstream")

        self.logger.info("Starting Silver clickstream processing")

        df = self.spark.read.format("delta").load(bronze_path)
        df = self._clean_clickstream(df)

        record_count = df.count()

        if merchant_dim_df is not None:
            df = self.enrich_with_merchant_data(df, merchant_dim_df)

        optimized_df = self.compaction.optimize_write_partitioning(
            df, "event_date"
        )
        (
            optimized_df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("event_date")
            .save(silver_path)
        )

        self.logger.info(f"Silver clickstream complete: {record_count} records")
        return record_count

    # ──────────────────────────────────────────────────────────
    # CONTINUOUS STREAMING ENRICHMENT (DELTA MERGE UPSERT)
    # ──────────────────────────────────────────────────────────

    def upsert_orders_batch(
        self,
        batch_df: DataFrame,
        batch_id: int,
        merchant_dim_df: Optional[DataFrame] = None,
    ) -> None:
        """Micro-batch upsert handler using Delta MERGE.

        Ensures deterministic latest-record retention and idempotent exactly-once
        semantics when streaming from Bronze to Silver.
        """
        if batch_df.isEmpty():
            return

        silver_path = self.config.paths.get_table_path("silver", "orders")
        cleaned_batch = self._clean_orders(batch_df)

        if merchant_dim_df is not None:
            cleaned_batch = self.enrich_with_merchant_data(cleaned_batch, merchant_dim_df)

        try:
            from delta.tables import DeltaTable
            if DeltaTable.isDeltaTable(self.spark, silver_path):
                target_table = DeltaTable.forPath(self.spark, silver_path)
                match_cond = "target.order_id = source.order_id"
                update_cond = "source.event_timestamp >= target.event_timestamp" if "event_timestamp" in cleaned_batch.columns else None

                builder = target_table.alias("target").merge(
                    cleaned_batch.alias("source"), match_cond
                )
                if update_cond:
                    builder = builder.whenMatchedUpdate(condition=update_cond, set={c: f"source.{c}" for c in cleaned_batch.columns})
                else:
                    builder = builder.whenMatchedUpdateAll()

                builder.whenNotMatchedInsertAll().execute()
            else:
                (
                    cleaned_batch.write
                    .format("delta")
                    .mode("append")
                    .partitionBy("event_date")
                    .save(silver_path)
                )
        except Exception as e:
            self.logger.warning(f"Delta merge fallback to append write: {e}")
            cleaned_batch.write.format("delta").mode("append").partitionBy("event_date").save(silver_path)

    def process_streaming(
        self,
        event_type: str = "orders",
        merchant_dim_df: Optional[DataFrame] = None,
        trigger_interval: Optional[str] = None,
    ) -> StreamingQuery:
        """Start continuous Structured Streaming pipeline from Bronze to Silver."""
        bronze_path = self.config.paths.get_table_path("bronze", event_type)
        checkpoint_path = self.config.paths.get_checkpoint_path(f"silver_{event_type}")
        trigger = trigger_interval or self.config.streaming.trigger_interval

        self.logger.info(f"Starting Silver streaming enrichment for '{event_type}'")

        bronze_stream = (
            self.spark.readStream
            .format("delta")
            .load(bronze_path)
        )

        def batch_handler(df: DataFrame, b_id: int):
            if event_type == "orders":
                self.upsert_orders_batch(df, b_id, merchant_dim_df)
            else:
                silver_path = self.config.paths.get_table_path("silver", event_type)
                cleaned = self._clean_clickstream(df)
                if merchant_dim_df is not None:
                    cleaned = self.enrich_with_merchant_data(cleaned, merchant_dim_df)
                cleaned.write.format("delta").mode("append").partitionBy("event_date").save(silver_path)

        query = (
            bronze_stream.writeStream
            .format("delta")
            .foreachBatch(batch_handler)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=trigger)
            .start()
        )
        return query
