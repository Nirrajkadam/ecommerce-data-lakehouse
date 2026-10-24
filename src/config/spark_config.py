"""
Spark Configuration Module for Ecommerce Data Lakehouse
=======================================================
Provides optimized SparkSession configurations for streaming and batch workloads.
Includes AQE (Adaptive Query Execution) settings for handling data skew,
memory tuning, and Delta Lake integration.

Key Optimizations:
    - Adaptive Query Execution (AQE) with skew join support
    - Dynamic partition pruning for faster reads
    - Delta Lake extensions for ACID transactions
    - Broadcast join threshold tuning
    - Memory management for streaming workloads
"""

import os
from typing import Optional, Dict, Any, List
from pyspark.sql import SparkSession

# Try importing Delta Lake helper utilities
try:
    from delta import configure_spark_with_delta_pip
    HAS_DELTA_PIP = True
except ImportError:
    HAS_DELTA_PIP = False


class SparkConfig:
    """Factory for creating optimized SparkSession instances.

    Provides pre-configured SparkSession builders for different deployment
    modes (local development vs. cluster) with all performance optimizations
    for handling data skew and small files problems.

    Example:
        >>> config = SparkConfig(app_name="ecommerce-lakehouse", mode="local")
        >>> spark = config.create_session()
    """

    # ──────────────────────────────────────────────────────────────
    # Default configuration values
    # ──────────────────────────────────────────────────────────────
    DEFAULT_CONFIGS: Dict[str, Dict[str, Any]] = {
        "local": {
            "spark.master": "local[*]",
            "spark.driver.memory": "4g",
            "spark.executor.memory": "4g",
            "spark.sql.warehouse.dir": "./spark-warehouse",
        },
        "cluster": {
            "spark.master": "spark://spark-master:7077",
            "spark.driver.memory": "8g",
            "spark.executor.memory": "16g",
            "spark.executor.cores": "4",
            "spark.executor.instances": "4",
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.minExecutors": "2",
            "spark.dynamicAllocation.maxExecutors": "10",
        },
    }

    # ──────────────────────────────────────────────────────────────
    # AQE (Adaptive Query Execution) settings — critical for skew
    # ──────────────────────────────────────────────────────────────
    AQE_CONFIGS: Dict[str, str] = {
        # Enable AQE — the single most impactful setting for skew
        "spark.sql.adaptive.enabled": "true",

        # Enable automatic skewed join optimization
        # When Spark detects a partition much larger than others during
        # a SortMergeJoin, it splits the skewed partition into smaller
        # sub-partitions and replicates the matching partition from the
        # other side. This prevents the "1 task stuck at 99%" problem.
        "spark.sql.adaptive.skewJoin.enabled": "true",

        # A partition is considered skewed if its size is more than
        # 5x the median partition size (default). Lower this if you
        # have moderate skew that AQE isn't catching.
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",

        # Minimum size threshold for a partition to be considered skewed.
        # Partitions smaller than this won't be split even if they're
        # 5x the median — avoids unnecessary splitting of small data.
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "268435456",  # 256MB

        # Allow AQE to coalesce small shuffle partitions into larger ones.
        # This directly fights the small files problem when partitions
        # produce too many tiny output files after a shuffle.
        "spark.sql.adaptive.coalescePartitions.enabled": "true",

        # Target size for coalesced partitions — aim for ~128MB files
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "134217728",  # 128MB

        # Minimum number of partitions after coalescing — prevents
        # over-coalescing that would reduce parallelism
        "spark.sql.adaptive.coalescePartitions.minPartitionSize": "1048576",  # 1MB
    }

    # ──────────────────────────────────────────────────────────────
    # Broadcast join settings — eliminates shuffle for small tables
    # ──────────────────────────────────────────────────────────────
    BROADCAST_CONFIGS: Dict[str, str] = {
        # Tables smaller than this threshold are broadcast to all executors.
        # Set to 100MB — our merchant dimension table is ~20MB, so this
        # ensures it's always broadcast, eliminating the shuffle entirely.
        "spark.sql.autoBroadcastJoinThreshold": "104857600",  # 100MB
    }

    # ──────────────────────────────────────────────────────────────
    # Delta Lake settings
    # ──────────────────────────────────────────────────────────────
    DELTA_CONFIGS: Dict[str, str] = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",

        # Enable Delta Lake's auto-compaction (post-write optimization)
        # This automatically compacts small files after each write,
        # helping mitigate the small files problem from streaming.
        "spark.databricks.delta.autoCompact.enabled": "true",

        # Enable optimized writes — Delta rearranges data before writing
        # to produce fewer, more evenly-sized files.
        "spark.databricks.delta.optimizeWrite.enabled": "true",

        # Dynamic partition pruning — Spark pushes partition filters
        # into scan operators, skipping irrelevant files entirely.
        "spark.sql.optimizer.dynamicPartitionPruning.enabled": "true",
    }

    # ──────────────────────────────────────────────────────────────
    # Shuffle and memory settings
    # ──────────────────────────────────────────────────────────────
    PERFORMANCE_CONFIGS: Dict[str, str] = {
        # Default shuffle partitions — 200 is Spark's default but often
        # too high for moderate data volumes, too low for large ones.
        # AQE will auto-tune this, but we set a reasonable starting point.
        "spark.sql.shuffle.partitions": "200",

        # Fraction of executor memory used for shuffle/join spill buffers.
        # Higher values reduce disk spills during skewed joins.
        "spark.memory.fraction": "0.8",

        # Within the memory fraction, how much is reserved for storage
        # (caching) vs. execution (shuffles, joins, aggregations).
        "spark.memory.storageFraction": "0.3",

        # Serializer — Kryo is faster and more compact than Java serialization
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",

        # Enable Arrow-based columnar data transfer between JVM and Python.
        # Dramatically speeds up toPandas() and pandas UDF operations.
        "spark.sql.execution.arrow.pyspark.enabled": "true",
    }

    def __init__(
        self,
        app_name: str = "ecommerce-data-lakehouse",
        mode: str = "local",
        extra_configs: Optional[Dict[str, str]] = None,
    ):
        """Initialize Spark configuration.

        Args:
            app_name: Name of the Spark application.
            mode: Deployment mode - "local" for development, "cluster" for production.
            extra_configs: Additional Spark configurations to override defaults.
        """
        self.app_name = app_name
        self.mode = mode
        self.extra_configs = extra_configs or {}

    def _merge_configs(self) -> Dict[str, str]:
        """Merge all configuration layers in priority order.

        Returns:
            Merged configuration dictionary. Later layers override earlier ones.
        """
        merged = {}

        # Layer 1: Mode-specific defaults (local vs cluster)
        mode_configs = self.DEFAULT_CONFIGS.get(self.mode, self.DEFAULT_CONFIGS["local"])
        merged.update({k: str(v) for k, v in mode_configs.items()})

        # Layer 2: AQE settings (critical for data skew handling)
        merged.update(self.AQE_CONFIGS)

        # Layer 3: Broadcast join settings
        merged.update(self.BROADCAST_CONFIGS)

        # Layer 4: Delta Lake settings
        merged.update(self.DELTA_CONFIGS)

        # Layer 5: Performance tuning
        merged.update(self.PERFORMANCE_CONFIGS)

        # Layer 6: User overrides (highest priority)
        merged.update(self.extra_configs)

        return merged

    # Required Maven packages for Delta Lake 3.1.0 and Spark Kafka connector on Spark 3.5.x
    DEFAULT_PACKAGES: List[str] = [
        "io.delta:delta-spark_2.12:3.1.0",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    ]

    def create_session(self) -> SparkSession:
        """Create and return an optimized SparkSession.

        The session is configured with all performance optimizations for:
        - Data skew handling via AQE
        - Small files mitigation via Delta Lake auto-compaction
        - Memory management for streaming workloads
        - Broadcast join optimization for dimension tables
        - JVM packages for Delta Lake and Spark Kafka integration

        Returns:
            Configured SparkSession instance.
        """
        builder = SparkSession.builder.appName(self.app_name)

        configs = self._merge_configs()
        for key, value in configs.items():
            builder = builder.config(key, value)

        # Ensure Delta Lake and Kafka JVM packages are loaded
        kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        if HAS_DELTA_PIP:
            # configure_spark_with_delta_pip resolves local delta jar and adds extra packages
            builder = configure_spark_with_delta_pip(
                builder,
                extra_packages=[kafka_package],
            )
        else:
            # Fallback: explicitly declare Maven coordinates
            current_packages = configs.get("spark.jars.packages", "")
            packages_to_add = [p for p in self.DEFAULT_PACKAGES if p not in current_packages]
            if packages_to_add:
                combined = f"{current_packages},{','.join(packages_to_add)}" if current_packages else ",".join(packages_to_add)
                builder = builder.config("spark.jars.packages", combined)

        spark = builder.getOrCreate()

        # Set log level to reduce noise in development
        spark.sparkContext.setLogLevel("WARN")

        return spark

    @staticmethod
    def get_streaming_session(
        app_name: str = "ecommerce-streaming",
        checkpoint_dir: str = "./checkpoints",
    ) -> SparkSession:
        """Create a SparkSession optimized for Structured Streaming.

        Adds streaming-specific configurations on top of the base
        optimizations (AQE, Delta Lake, broadcast joins).

        Args:
            app_name: Name of the streaming application.
            checkpoint_dir: Base directory for streaming checkpoints.

        Returns:
            SparkSession configured for streaming workloads.
        """
        streaming_configs = {
            # Streaming-specific: process micro-batches efficiently
            "spark.sql.streaming.checkpointLocation": checkpoint_dir,

            # Allow multiple streaming queries in one session
            "spark.sql.streaming.forceDeleteTempCheckpointLocation": "true",

            # Kafka-specific settings
            "spark.sql.streaming.kafka.useDeprecatedOffsetFetching": "false",
        }

        config = SparkConfig(
            app_name=app_name,
            mode="local",
            extra_configs=streaming_configs,
        )
        return config.create_session()


# ──────────────────────────────────────────────────────────────
# Convenience function for quick session creation
# ──────────────────────────────────────────────────────────────
def get_spark(
    app_name: str = "ecommerce-data-lakehouse",
    mode: str = "local",
) -> SparkSession:
    """Quick helper to create a configured SparkSession.

    Args:
        app_name: Application name.
        mode: "local" or "cluster".

    Returns:
        Optimized SparkSession.
    """
    return SparkConfig(app_name=app_name, mode=mode).create_session()
