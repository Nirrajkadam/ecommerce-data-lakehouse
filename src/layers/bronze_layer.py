"""
Bronze Layer — Raw Data Ingestion
=================================
Ingests raw events from Kafka (or JSON files) into Delta Lake with minimal
transformation. Adds metadata columns for lineage tracking.

The Bronze layer is intentionally raw — we preserve the original data
and add only audit columns. All cleaning happens in Silver.

DEMONSTRATES: The Small Files Problem
    With micro-batch streaming writes every 30 seconds, each batch creates
    N_executors × N_partitions tiny Parquet files. After 24 hours, a single
    partition can have 2,880 files (2 per minute × 60 × 24).
"""

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from src.config.pipeline_config import PipelineConfig
from src.ingestion.kafka_consumer import KafkaStreamConsumer
from src.optimizations.compaction import CompactionEngine
from src.utils.logger import PipelineLogger


class BronzeLayer:
    """Bronze layer processor for raw data ingestion.

    Reads raw events and writes them to Delta Lake with metadata.
    Supports both streaming (from Kafka) and batch (from files) modes.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Optional[PipelineConfig] = None,
    ):
        self.spark = spark
        self.config = config or PipelineConfig()
        self.logger = PipelineLogger("bronze-layer", layer="bronze")
        self.consumer = KafkaStreamConsumer(spark, self.config)
        self.compaction = CompactionEngine(spark)

    def _add_metadata(
        self,
        df: DataFrame,
        source_topic: str,
        batch_id: Optional[str] = None,
    ) -> DataFrame:
        """Add audit/metadata columns to raw data.

        Args:
            df: Raw event DataFrame.
            source_topic: Name of the source Kafka topic.
            batch_id: Optional batch identifier for lineage tracking.

        Returns:
            DataFrame with metadata columns added.
        """
        enriched = df.withColumns({
            "_ingestion_timestamp": F.current_timestamp(),
            "_source_topic": F.lit(source_topic),
            "_batch_id": F.lit(batch_id),
        })

        # Add _raw_json if not already present (from Kafka consumer)
        if "_raw_json" not in df.columns:
            enriched = enriched.withColumn(
                "_raw_json", F.to_json(F.struct([F.col(c) for c in df.columns]))
            )

        return enriched

    def ingest_streaming(
        self,
        event_type: str,
        trigger_interval: Optional[str] = None,
    ) -> StreamingQuery:
        """Ingest events from Kafka as a streaming pipeline.

        Reads from Kafka topic, adds metadata, and writes to Bronze
        Delta table with micro-batch processing.

        Note: Without write-time optimization, this creates the Small Files
        Problem - each micro-batch (every 30s) creates new files:
            30s * 2 files/batch = 2,880 files/day per partition!

        Args:
            event_type: Type of event ("orders", "clickstream", "inventory").
            trigger_interval: Override trigger interval (default: "30 seconds").

        Returns:
            Running StreamingQuery handle.
        """
        trigger = trigger_interval or self.config.streaming.trigger_interval
        output_path = self.config.paths.get_table_path("bronze", event_type)
        checkpoint_path = self.config.paths.get_checkpoint_path(f"bronze_{event_type}")

        self.logger.info(
            f"Starting Bronze streaming ingestion for '{event_type}'",
            output_path=output_path,
            trigger=trigger,
        )

        # Read from Kafka
        stream_df = self.consumer.read_stream(event_type)

        # Add metadata columns
        stream_df = self._add_metadata(stream_df, f"ecommerce.{event_type}")

        # Apply watermark for late-arriving data handling
        if "event_timestamp" in stream_df.columns:
            stream_df = stream_df.withWatermark(
                "event_timestamp",
                self.config.streaming.watermark_delay,
            )

        # Write to Delta Lake
        # NOTE: Without repartitioning, each micro-batch creates
        # num_executors × num_partitions tiny files. This is the
        # Small Files Problem in action!
        query = (
            stream_df.writeStream
            .format("delta")
            .outputMode(self.config.streaming.output_mode)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=trigger)
            .partitionBy("event_date")
            .start(output_path)
        )

        self.logger.info(f"Bronze stream started: {query.name}")
        return query

    def ingest_batch(
        self,
        event_type: str,
        input_path: str,
        optimize_files: bool = True,
    ) -> int:
        """Ingest events from JSON/Parquet files (batch mode).

        For local development or backfill operations without Kafka.

        Args:
            event_type: Type of event.
            input_path: Path to input files.
            optimize_files: Whether to repartition before writing
                           (prevents small files).

        Returns:
            Number of records ingested.
        """
        output_path = self.config.paths.get_table_path("bronze", event_type)

        self.logger.info(
            f"Starting Bronze batch ingestion for '{event_type}'",
            input_path=input_path,
            output_path=output_path,
        )

        # Read from files
        df = self.consumer.read_from_json_files(event_type, input_path)

        # Add metadata
        df = self._add_metadata(df, f"file://{input_path}")

        record_count = df.count()
        self.logger.info(f"Read {record_count} records from {input_path}")

        # ── OPTIMIZATION: Repartition before write ──
        # This is the FIX for the small files problem at write time.
        # By repartitioning by event_date, we ensure each partition
        # gets a single, well-sized file instead of many tiny ones.
        if optimize_files:
            df = self.compaction.optimize_write_partitioning(
                df, "event_date"
            )

        # Write to Delta
        (
            df.write
            .format("delta")
            .mode("append")
            .partitionBy("event_date")
            .save(output_path)
        )

        self.logger.info(
            f"Bronze batch ingestion complete",
            records=record_count,
            output_path=output_path,
        )

        return record_count
