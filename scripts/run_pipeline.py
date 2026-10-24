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


def main():
    parser = argparse.ArgumentParser(description="Ecommerce Data Lakehouse Pipeline")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "all"],
                        default="all", help="Which layer to run")
    parser.add_argument("--mode", choices=["batch", "streaming"],
                        default="batch", help="Execution mode")
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
        if args.layer in ("bronze", "all"):
            run_bronze(spark, config, args.input)

        if args.layer in ("silver", "all"):
            run_silver(spark, config)

        if args.layer in ("gold", "all"):
            run_gold(spark, config)

        logger.info("Pipeline complete ✅")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
