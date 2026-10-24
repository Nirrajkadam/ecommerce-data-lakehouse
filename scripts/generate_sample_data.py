#!/usr/bin/env python3
"""
Generate Sample Data for Local Development
============================================
Creates sample ecommerce data files (JSON format) with realistic
skewed distribution for testing the pipeline locally.

Usage:
    python scripts/generate_sample_data.py
    python scripts/generate_sample_data.py --orders 500000 --output ./data/sample
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.ingestion.kafka_producer import write_sample_data_to_json


def main():
    parser = argparse.ArgumentParser(description="Generate Sample Ecommerce Data")
    parser.add_argument("--orders", type=int, default=100000,
                        help="Number of order events (default: 100,000)")
    parser.add_argument("--clickstream", type=int, default=500000,
                        help="Number of clickstream events (default: 500,000)")
    parser.add_argument("--inventory", type=int, default=10000,
                        help="Number of inventory events (default: 10,000)")
    parser.add_argument("--output", type=str, default="./data/sample",
                        help="Output directory")
    args = parser.parse_args()

    print(f"\n📊 Generating sample ecommerce data...")
    print(f"   Orders:      {args.orders:>10,}")
    print(f"   Clickstream: {args.clickstream:>10,}")
    print(f"   Inventory:   {args.inventory:>10,}")
    print(f"   Output:      {args.output}\n")

    files = write_sample_data_to_json(
        output_dir=args.output,
        orders_count=args.orders,
        clickstream_count=args.clickstream,
        inventory_count=args.inventory,
    )

    print(f"\n✅ Sample data generated:")
    for event_type, filepath in files.items():
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        print(f"   {event_type:<15} → {filepath} ({size_mb:.1f} MB)")
    print()


if __name__ == "__main__":
    main()
