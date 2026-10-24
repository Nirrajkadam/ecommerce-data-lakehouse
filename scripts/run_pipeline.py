#!/usr/bin/env python3
"""
Pipeline Runner — Main Entry Point
====================================
Orchestrates the full Bronze → Silver → Gold pipeline execution.

Usage:
    python scripts/run_pipeline.py --layer all --mode batch
    python scripts/run_pipeline.py --layer bronze --mode batch --input ./data/sample
    python scripts/run_pipeline.py --layer silver
    python scripts/run_pipeline.py --layer gold
"""

import argparse
import sys
import os

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.spark_config import SparkConfig
from src.config.pipeline_config import PipelineConfig
from src.layers.bronze_layer import BronzeLayer
from src.layers.silver_layer import SilverLayer
from src.layers.gold_layer import GoldLayer
from src.ingestion.kafka_producer import generate_merchant_dimension
from src.utils.logger import PipelineLogger

logger = PipelineLogger("pipeline-runner")


def run_bronze(spark, config, input_dir):
    """Run Bronze layer ingestion."""
    bronze = BronzeLayer(spark, config)
    for event_type in ["orders", "clickstream", "inventory"]:
        input_path = os.path.join(input_dir, f"{event_type}.json")
        if os.path.exists(input_path):
            logger.info(f"Ingesting {event_type} from {input_path}")
            count = bronze.ingest_batch(event_type, input_path)
            logger.info(f"Bronze {event_type}: {count} records ingested")
        else:
            logger.warning(f"Input file not found: {input_path}")


def run_silver(spark, config):
    """Run Silver layer processing."""
    silver = SilverLayer(spark, config)

    # Create merchant dimension DataFrame
    merchants = generate_merchant_dimension()
    merchant_df = spark.read.json(
        spark.sparkContext.parallelize([
            str(m).replace("'", '"') for m in merchants
        ])
    )

    silver.process_orders(merchant_df)
    logger.info("Silver orders processing complete")


def run_gold(spark, config):
    """Run Gold layer aggregations."""
    gold = GoldLayer(spark, config)
    gold.build_all()
    logger.info("Gold layer processing complete")


def run_streaming(spark, config, layer="all", duration_seconds=None):
    """Run continuous Structured Streaming across Bronze, Silver, and Gold layers.

    Demonstrates the live Kafka -> Bronze Delta -> Silver Delta -> Gold Delta pipeline.
    """
    import time

    logger.info("Initializing continuous streaming execution across Medallion layers...")
    bronze = BronzeLayer(spark, config)
    silver = SilverLayer(spark, config)
    gold = GoldLayer(spark, config)

    # Broadcast merchant dimension for stream-static joins
    merchants = generate_merchant_dimension()
    merchant_df = spark.read.json(
        spark.sparkContext.parallelize([
            str(m).replace("'", '"') for m in merchants
        ])
    )

    active_queries = []

    # 1. Bronze Streams (Kafka -> Raw Delta)
    if layer in ("bronze", "all"):
        logger.info("Starting Bronze stream ingestion...")
        for event_type in ["orders", "clickstream"]:
            try:
                q = bronze.ingest_streaming(event_type)
                active_queries.append(q)
                logger.info(f"Bronze stream '{event_type}' active: id={q.id}")
            except Exception as e:
                logger.error(f"Failed to start Bronze stream for {event_type}: {e}")

    # 2. Silver Stream (Bronze Delta -> Silver Delta via foreachBatch + MERGE)
    if layer in ("silver", "all"):
        logger.info("Starting Silver stream enrichment...")
        try:
            q_silver = silver.process_streaming(
                event_type="orders", merchant_dim_df=merchant_df
            )
            active_queries.append(q_silver)
            logger.info(f"Silver orders enrichment stream active: id={q_silver.id}")
        except Exception as e:
            logger.error(f"Failed to start Silver stream: {e}")

    # 3. Gold Stream (Silver Delta -> Gold Delta via foreachBatch + MERGE)
    if layer in ("gold", "all"):
        logger.info("Starting Gold stream aggregation...")
        try:
            q_gold = gold.process_streaming()
            active_queries.append(q_gold)
            logger.info(f"Gold summary aggregation stream active: id={q_gold.id}")
        except Exception as e:
            logger.error(f"Failed to start Gold stream: {e}")

    if not active_queries:
        logger.warning("No streaming queries active.")
        return

    logger.info(
        f"Pipeline running: {len(active_queries)} streams active. "
        f"{'Running for ' + str(duration_seconds) + 's' if duration_seconds else 'Press Ctrl+C to stop.'}"
    )

    start_time = time.time()
    try:
        while True:
            time.sleep(2)
            for q in active_queries:
                if not q.isActive and q.exception():
                    logger.error(f"Stream query failed: {q.exception()}")
                    raise q.exception()
            if duration_seconds and (time.time() - start_time) >= duration_seconds:
                logger.info(f"Completed requested run duration ({duration_seconds}s).")
                break
    except KeyboardInterrupt:
        logger.info("Termination signal received. Stopping streaming queries...")
    finally:
        for q in active_queries:
            if q.isActive:
                q.stop()
        logger.info("All streaming queries terminated cleanly.")


def main():
    parser = argparse.ArgumentParser(description="Ecommerce Data Lakehouse Pipeline")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "all"],
                        default="all", help="Which layer to run")
    parser.add_argument("--mode", choices=["batch", "streaming"],
                        default="batch", help="Execution mode (batch or streaming)")
    parser.add_argument("--duration", type=int, default=None,
                        help="Streaming duration in seconds (optional, default runs until stopped)")
    parser.add_argument("--input", default="./data/sample",
                        help="Input data directory (for batch mode)")
    parser.add_argument("--env", choices=["local", "staging", "production"],
                        default="local", help="Environment")
    args = parser.parse_args()

    config = PipelineConfig.from_env(args.env)
    spark_config = SparkConfig(app_name="ecommerce-lakehouse", mode="local")
    spark = spark_config.create_session()

    logger.info(f"Starting pipeline: layer={args.layer}, mode={args.mode}")

    try:
        if args.mode == "streaming":
            run_streaming(spark, config, layer=args.layer, duration_seconds=args.duration)
        else:
            if args.layer in ("bronze", "all"):
                run_bronze(spark, config, args.input)

            if args.layer in ("silver", "all"):
                run_silver(spark, config)

            if args.layer in ("gold", "all"):
                run_gold(spark, config)

        logger.info("Pipeline execution completed successfully.")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
