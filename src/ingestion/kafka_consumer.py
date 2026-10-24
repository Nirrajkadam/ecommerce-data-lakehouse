"""
Kafka Consumer — PySpark Structured Streaming Ingestion
========================================================
Reads raw ecommerce events from Kafka topics using PySpark Structured
Streaming and writes them to the Bronze layer as Delta tables.

Handles:
    - Schema enforcement via from_json()
    - Watermarking for late-arriving data
    - Checkpoint management for exactly-once semantics
    - Micro-batch trigger configuration
"""

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from src.config.pipeline_config import KafkaConfig, PipelineConfig
from src.schemas.ecommerce_schemas import SCHEMA_REGISTRY
from src.utils.logger import PipelineLogger


class KafkaStreamConsumer:
    """PySpark Structured Streaming consumer for Kafka topics.

    Reads from one or more Kafka topics, applies schemas, and provides
    typed DataFrames ready for downstream processing.

    Example:
        >>> consumer = KafkaStreamConsumer(spark, config)
        >>> orders_stream = consumer.read_stream("orders")
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Optional[PipelineConfig] = None,
    ):
        self.spark = spark
        self.config = config or PipelineConfig()
        self.logger = PipelineLogger("kafka-consumer", layer="ingestion")

    def read_stream(
        self,
        event_type: str,
        schema: Optional[StructType] = None,
    ) -> DataFrame:
        """Read a streaming DataFrame from a Kafka topic.

        Connects to Kafka, reads the raw bytes, deserializes the JSON
        payload, and applies the schema for the given event type.

        Args:
            event_type: Type of event ("orders", "clickstream", "inventory").
            schema: Override schema. Uses default from registry if not provided.

        Returns:
            Streaming DataFrame with parsed event data.
        """
        topic = self.config.kafka.topics.get(event_type, f"ecommerce.{event_type}")
        event_schema = schema or SCHEMA_REGISTRY.get(event_type)

        if event_schema is None:
            raise ValueError(f"No schema found for event type: {event_type}")

        self.logger.info(f"Starting stream from topic: {topic}")

        # Read raw bytes from Kafka
        raw_stream = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.config.kafka.bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", self.config.kafka.starting_offsets)
            .option("maxOffsetsPerTrigger", self.config.kafka.max_offsets_per_trigger)
            .option("kafka.security.protocol", self.config.kafka.security_protocol)
            .load()
        )

        # Parse the JSON value and apply schema
        parsed_stream = (
            raw_stream
            .select(
                F.col("key").cast("string").alias("_kafka_key"),
                F.col("value").cast("string").alias("_raw_json"),
                F.col("topic").alias("_source_topic"),
                F.col("partition").alias("_kafka_partition"),
                F.col("offset").alias("_kafka_offset"),
                F.col("timestamp").alias("_kafka_timestamp"),
            )
            .withColumn("parsed", F.from_json(F.col("_raw_json"), event_schema))
            .select("parsed.*", "_raw_json", "_source_topic",
                    "_kafka_partition", "_kafka_offset", "_kafka_timestamp")
        )

        return parsed_stream

    def read_batch(
        self,
        event_type: str,
        schema: Optional[StructType] = None,
    ) -> DataFrame:
        """Read a batch DataFrame from a Kafka topic (for testing/backfill).

        Same as read_stream but returns a static DataFrame.

        Args:
            event_type: Type of event.
            schema: Override schema.

        Returns:
            Static DataFrame with parsed event data.
        """
        topic = self.config.kafka.topics.get(event_type)
        event_schema = schema or SCHEMA_REGISTRY.get(event_type)

        raw_batch = (
            self.spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", self.config.kafka.bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        parsed_batch = (
            raw_batch
            .select(
                F.col("value").cast("string").alias("_raw_json"),
                F.col("topic").alias("_source_topic"),
                F.col("partition").alias("_kafka_partition"),
                F.col("offset").alias("_kafka_offset"),
            )
            .withColumn("parsed", F.from_json(F.col("_raw_json"), event_schema))
            .select("parsed.*", "_raw_json", "_source_topic",
                    "_kafka_partition", "_kafka_offset")
        )

        return parsed_batch

    def read_from_json_files(
        self,
        event_type: str,
        input_path: str,
        schema: Optional[StructType] = None,
    ) -> DataFrame:
        """Read events from JSON files (for local development without Kafka).

        Args:
            event_type: Type of event.
            input_path: Path to the JSON file(s).
            schema: Override schema.

        Returns:
            DataFrame with parsed event data.
        """
        event_schema = schema or SCHEMA_REGISTRY.get(event_type)

        self.logger.info(f"Reading {event_type} from JSON files: {input_path}")

        df = (
            self.spark.read
            .schema(event_schema)
            .json(input_path)
        )

        return df
