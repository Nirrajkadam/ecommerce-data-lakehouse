"""
Pipeline Configuration for Ecommerce Data Lakehouse
====================================================
Centralized configuration for all pipeline components including Kafka topics,
Medallion layer paths, checkpoint locations, compaction schedules, and
file size optimization targets.

All paths and settings can be overridden via environment variables for
seamless local → staging → production promotion.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class KafkaConfig:
    """Kafka connection and topic configuration.

    Attributes:
        bootstrap_servers: Comma-separated list of Kafka broker addresses.
        topics: Mapping of logical topic names to actual Kafka topic names.
        consumer_group: Consumer group ID for offset tracking.
        starting_offsets: Where to start reading ("earliest" or "latest").
        max_offsets_per_trigger: Max records per micro-batch (backpressure).
    """
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topics: Dict[str, str] = field(default_factory=lambda: {
        "orders": "ecommerce.orders",
        "clickstream": "ecommerce.clickstream",
        "inventory": "ecommerce.inventory",
    })
    consumer_group: str = "ecommerce-lakehouse-consumer"
    starting_offsets: str = "earliest"
    max_offsets_per_trigger: int = 100000  # Backpressure control
    security_protocol: str = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")


@dataclass
class LakehousePathConfig:
    """Medallion architecture path configuration.

    Directory structure:
        base_path/
        ├── bronze/          # Raw ingested data from Kafka
        │   ├── orders/
        │   ├── clickstream/
        │   └── inventory/
        ├── silver/          # Cleaned, deduplicated, enriched data
        │   ├── orders/
        │   ├── clickstream/
        │   └── inventory/
        ├── gold/            # Business-level aggregations
        │   ├── merchant_daily_summary/
        │   ├── customer_360/
        │   └── product_performance/
        └── checkpoints/     # Streaming checkpoint locations
    """
    base_path: str = os.getenv("LAKEHOUSE_BASE_PATH", "./data/lakehouse")

    @property
    def bronze_path(self) -> str:
        return f"{self.base_path}/bronze"

    @property
    def silver_path(self) -> str:
        return f"{self.base_path}/silver"

    @property
    def gold_path(self) -> str:
        return f"{self.base_path}/gold"

    @property
    def checkpoint_path(self) -> str:
        return f"{self.base_path}/checkpoints"

    def get_table_path(self, layer: str, table: str) -> str:
        """Get the full path for a specific table in a layer.

        Args:
            layer: Medallion layer ("bronze", "silver", "gold").
            table: Table name (e.g., "orders", "merchant_daily_summary").

        Returns:
            Full path to the Delta table directory.
        """
        return f"{self.base_path}/{layer}/{table}"

    def get_checkpoint_path(self, stream_name: str) -> str:
        """Get checkpoint path for a specific streaming query.

        Args:
            stream_name: Unique name for the streaming query.

        Returns:
            Checkpoint directory path for the stream.
        """
        return f"{self.checkpoint_path}/{stream_name}"


@dataclass
class CompactionConfig:
    """Configuration for Delta Lake table compaction.

    Controls the OPTIMIZE + ZORDER operations that fix the small files
    problem by merging tiny Parquet files into optimal-sized blocks.

    Attributes:
        target_file_size_mb: Desired output file size after compaction (128-256MB).
        min_file_size_mb: Files smaller than this trigger compaction.
        compaction_interval_hours: How often to run async compaction.
        zorder_columns: Columns to ZORDER BY for data skipping optimization.
    """
    target_file_size_mb: int = 256
    min_file_size_mb: int = 1
    compaction_interval_hours: int = 6
    max_files_per_compaction: int = 1000

    # ZORDER columns per table — co-locates related records for
    # faster predicate pushdown and data skipping
    zorder_columns: Dict[str, List[str]] = field(default_factory=lambda: {
        "orders": ["merchant_id", "event_timestamp"],
        "clickstream": ["user_id", "event_timestamp"],
        "inventory": ["product_id", "merchant_id"],
        "merchant_daily_summary": ["merchant_id", "event_date"],
        "customer_360": ["customer_id"],
    })


@dataclass
class StreamingConfig:
    """Streaming pipeline trigger and watermark configuration.

    Attributes:
        trigger_interval: Micro-batch processing interval.
        watermark_delay: How long to wait for late-arriving data.
        output_mode: Streaming output mode ("append", "update", "complete").
    """
    trigger_interval: str = "30 seconds"
    watermark_delay: str = "10 minutes"
    output_mode: str = "append"

    # Write-time optimization: number of output partitions per write.
    # Fewer partitions = fewer (larger) files, fighting small files.
    # Set based on daily data volume / target_file_size.
    output_partitions: int = 8


@dataclass
class SkewConfig:
    """Configuration for data skew detection and handling.

    Attributes:
        skew_threshold_ratio: Partition is skewed if > N× median size.
        salt_buckets: Number of salt partitions for key salting strategy.
        hot_keys: Known hot keys to handle specially (e.g., high-volume merchants).
        null_key_defaults: Values treated as null/default for isolation.
    """
    skew_threshold_ratio: float = 10.0
    salt_buckets: int = 10

    # Known hot keys — these merchants generate 60%+ of traffic.
    # Pre-identifying them avoids runtime detection overhead.
    hot_keys: Dict[str, List[str]] = field(default_factory=lambda: {
        "merchant_id": ["MERCHANT_001"],
    })

    # Values treated as null/default that should be isolated before joins
    null_key_defaults: List[str] = field(default_factory=lambda: [
        None, "", "NULL", "UNKNOWN", "DEFAULT", "N/A",
    ])


@dataclass
class PipelineConfig:
    """Master configuration aggregating all pipeline settings.

    Usage:
        >>> config = PipelineConfig()
        >>> config.kafka.bootstrap_servers
        'localhost:9092'
        >>> config.paths.get_table_path("bronze", "orders")
        './data/lakehouse/bronze/orders'
    """
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    paths: LakehousePathConfig = field(default_factory=LakehousePathConfig)
    compaction: CompactionConfig = field(default_factory=CompactionConfig)
    streaming: StreamingConfig = field(default_factory=StreamingConfig)
    skew: SkewConfig = field(default_factory=SkewConfig)

    @classmethod
    def from_env(cls, env: str = "local") -> "PipelineConfig":
        """Create configuration from environment preset.

        Args:
            env: Environment name ("local", "staging", "production").

        Returns:
            PipelineConfig with environment-appropriate settings.
        """
        if env == "production":
            return cls(
                kafka=KafkaConfig(
                    bootstrap_servers=os.getenv(
                        "KAFKA_BOOTSTRAP_SERVERS",
                        "kafka-1:9092,kafka-2:9092,kafka-3:9092",
                    ),
                    max_offsets_per_trigger=500000,
                ),
                paths=LakehousePathConfig(
                    base_path=os.getenv("LAKEHOUSE_BASE_PATH", "s3a://ecommerce-lakehouse"),
                ),
                compaction=CompactionConfig(
                    target_file_size_mb=256,
                    compaction_interval_hours=4,
                ),
                streaming=StreamingConfig(
                    trigger_interval="30 seconds",
                    output_partitions=32,
                ),
            )
        elif env == "staging":
            return cls(
                paths=LakehousePathConfig(
                    base_path=os.getenv("LAKEHOUSE_BASE_PATH", "s3a://ecommerce-lakehouse-staging"),
                ),
                streaming=StreamingConfig(output_partitions=16),
            )
        else:
            # Local development defaults
            return cls()
