"""
Kafka Producer — Simulated Ecommerce Event Generator
=====================================================
Generates realistic ecommerce events (orders, clickstream, inventory)
with INTENTIONAL data skew to demonstrate the skew problem.

Key Design:
    - merchant_id "MERCHANT_001" receives ~60% of ALL events
    - This simulates real-world scenarios where a dominant seller
      generates the majority of platform traffic
    - The skewed distribution causes join/aggregation bottlenecks
      that our optimization modules are designed to fix

Usage:
    python -m src.ingestion.kafka_producer --mode batch --count 100000
    python -m src.ingestion.kafka_producer --mode streaming --rate 1000
"""

import json
import random
import uuid
import time
import argparse
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Generator, List, Optional

from src.utils.logger import PipelineLogger

logger = PipelineLogger("kafka-producer", layer="ingestion")

# ──────────────────────────────────────────────────────────────
# Skew Configuration
# ──────────────────────────────────────────────────────────────
# MERCHANT_001 gets 60% of traffic — this is the "hot key"
# that causes the data skew problem in downstream joins.
MERCHANT_WEIGHTS = {
    "MERCHANT_001": 0.60,   # ← THE HOT KEY: 60% of all events
    "MERCHANT_002": 0.08,
    "MERCHANT_003": 0.07,
    "MERCHANT_004": 0.05,
    "MERCHANT_005": 0.04,
    "MERCHANT_006": 0.03,
    "MERCHANT_007": 0.03,
    "MERCHANT_008": 0.03,
    "MERCHANT_009": 0.03,
    "MERCHANT_010": 0.02,
    "MERCHANT_011": 0.01,
    "MERCHANT_012": 0.01,
}

MERCHANT_IDS = list(MERCHANT_WEIGHTS.keys())
MERCHANT_PROBS = list(MERCHANT_WEIGHTS.values())

# ──────────────────────────────────────────────────────────────
# Reference Data
# ──────────────────────────────────────────────────────────────
PRODUCTS = [
    ("PROD_001", "Wireless Earbuds", 49.99),
    ("PROD_002", "Laptop Stand", 29.99),
    ("PROD_003", "USB-C Hub", 39.99),
    ("PROD_004", "Mechanical Keyboard", 89.99),
    ("PROD_005", "Monitor Light Bar", 59.99),
    ("PROD_006", "Webcam HD", 69.99),
    ("PROD_007", "Mouse Pad XL", 19.99),
    ("PROD_008", "Desk Organizer", 24.99),
    ("PROD_009", "Cable Management Kit", 14.99),
    ("PROD_010", "Phone Charger", 12.99),
    ("PROD_011", "Tablet Case", 34.99),
    ("PROD_012", "Smart Watch Band", 22.99),
    ("PROD_013", "Bluetooth Speaker", 44.99),
    ("PROD_014", "Power Bank", 27.99),
    ("PROD_015", "Screen Protector", 9.99),
]

PAYMENT_METHODS = ["credit_card", "debit_card", "upi", "wallet", "cod"]
ORDER_STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
CITIES = [
    ("Mumbai", "Maharashtra"), ("Delhi", "Delhi"), ("Bangalore", "Karnataka"),
    ("Hyderabad", "Telangana"), ("Chennai", "Tamil Nadu"), ("Kolkata", "West Bengal"),
    ("Pune", "Maharashtra"), ("Ahmedabad", "Gujarat"), ("Jaipur", "Rajasthan"),
]
PAGES = ["home", "product", "cart", "checkout", "confirmation", "search", "category"]
ACTIONS = ["view", "click", "add_to_cart", "remove_from_cart", "purchase", "search"]
DEVICES = ["mobile", "desktop", "tablet"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge"]
WAREHOUSES = ["WH_NORTH_01", "WH_SOUTH_01", "WH_EAST_01", "WH_WEST_01", "WH_CENTRAL_01"]

# Customer IDs — 1000 unique customers
CUSTOMER_IDS = [f"CUST_{i:05d}" for i in range(1, 1001)]


def _pick_merchant() -> str:
    """Pick a merchant_id with skewed distribution (60% → MERCHANT_001)."""
    return random.choices(MERCHANT_IDS, weights=MERCHANT_PROBS, k=1)[0]


def _random_timestamp(days_back: int = 30) -> datetime:
    """Generate a random timestamp within the last N days."""
    now = datetime.now(timezone.utc)
    delta = timedelta(
        days=random.randint(0, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    return now - delta


# ──────────────────────────────────────────────────────────────
# Event Generators
# ──────────────────────────────────────────────────────────────

def generate_order_event(timestamp: Optional[datetime] = None) -> Dict[str, Any]:
    """Generate a single order event with skewed merchant distribution."""
    ts = timestamp or _random_timestamp()
    merchant_id = _pick_merchant()  # ← SKEWED distribution
    num_items = random.randint(1, 5)

    items = []
    total = 0.0
    for _ in range(num_items):
        product = random.choice(PRODUCTS)
        qty = random.randint(1, 3)
        discount = round(random.uniform(0, 0.2), 2)
        price = round(product[2] * qty * (1 - discount), 2)
        total += price
        items.append({
            "product_id": product[0],
            "product_name": product[1],
            "quantity": qty,
            "unit_price": product[2],
            "discount": discount,
        })

    city, state = random.choice(CITIES)

    # Inject some null merchant_ids (~5% of records)
    # to demonstrate null key isolation in the skew handler
    if random.random() < 0.05:
        merchant_id = random.choice([None, "", "NULL", "UNKNOWN"])

    return {
        "order_id": f"ORD-{uuid.uuid4().hex[:12].upper()}",
        "merchant_id": merchant_id,
        "customer_id": random.choice(CUSTOMER_IDS),
        "order_status": random.choice(ORDER_STATUSES),
        "total_amount": round(total, 2),
        "currency": "INR",
        "payment_method": random.choice(PAYMENT_METHODS),
        "items": items,
        "shipping_address_city": city,
        "shipping_address_state": state,
        "event_timestamp": ts.isoformat(),
        "event_date": ts.strftime("%Y-%m-%d"),
    }


def generate_clickstream_event(
    timestamp: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Generate a single clickstream event with skewed merchant distribution."""
    ts = timestamp or _random_timestamp()
    merchant_id = _pick_merchant()

    return {
        "event_id": f"EVT-{uuid.uuid4().hex[:12].upper()}",
        "session_id": f"SESS-{uuid.uuid4().hex[:8].upper()}",
        "user_id": random.choice(CUSTOMER_IDS),
        "merchant_id": merchant_id,
        "page": random.choice(PAGES),
        "action": random.choice(ACTIONS),
        "product_id": random.choice(PRODUCTS)[0],
        "referrer": random.choice(["google", "direct", "social", "email", None]),
        "device_type": random.choice(DEVICES),
        "browser": random.choice(BROWSERS),
        "duration_seconds": random.randint(1, 300),
        "event_timestamp": ts.isoformat(),
        "event_date": ts.strftime("%Y-%m-%d"),
    }


def generate_inventory_event(
    timestamp: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Generate a single inventory update event."""
    ts = timestamp or _random_timestamp()
    merchant_id = _pick_merchant()
    product = random.choice(PRODUCTS)

    return {
        "inventory_id": f"INV-{uuid.uuid4().hex[:12].upper()}",
        "product_id": product[0],
        "merchant_id": merchant_id,
        "warehouse_id": random.choice(WAREHOUSES),
        "quantity_available": random.randint(0, 1000),
        "quantity_reserved": random.randint(0, 50),
        "reorder_threshold": random.randint(10, 100),
        "unit_cost": round(product[2] * 0.6, 2),  # 60% of retail
        "last_restock_date": (ts - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d"),
        "event_timestamp": ts.isoformat(),
        "event_date": ts.strftime("%Y-%m-%d"),
    }


def generate_merchant_dimension() -> List[Dict[str, Any]]:
    """Generate the merchant dimension table (static reference data).

    This small table (~20MB in production) is ideal for broadcast joins.
    """
    categories = ["electronics", "fashion", "grocery", "home_garden", "sports"]
    tiers = ["platinum", "gold", "silver", "bronze"]
    regions = ["North", "South", "East", "West", "Central"]

    merchants = []
    for m_id in MERCHANT_IDS:
        merchants.append({
            "merchant_id": m_id,
            "merchant_name": f"Merchant {m_id.split('_')[1]}",
            "category": random.choice(categories),
            "tier": "platinum" if m_id == "MERCHANT_001" else random.choice(tiers),
            "region": random.choice(regions),
            "onboarding_date": f"20{random.randint(18, 24)}-{random.randint(1,12):02d}-01",
            "is_active": "true",
        })
    return merchants


def generate_events_batch(
    event_type: str = "orders",
    count: int = 10000,
) -> Generator[Dict[str, Any], None, None]:
    """Generate a batch of events.

    Args:
        event_type: Type of event ("orders", "clickstream", "inventory").
        count: Number of events to generate.

    Yields:
        Event dictionaries.
    """
    generators = {
        "orders": generate_order_event,
        "clickstream": generate_clickstream_event,
        "inventory": generate_inventory_event,
    }
    gen_func = generators.get(event_type, generate_order_event)

    for i in range(count):
        yield gen_func()
        if (i + 1) % 10000 == 0:
            logger.info(f"Generated {i + 1}/{count} {event_type} events")


def write_sample_data_to_json(
    output_dir: str,
    orders_count: int = 100000,
    clickstream_count: int = 500000,
    inventory_count: int = 10000,
) -> Dict[str, str]:
    """Write sample data to JSON files for local development.

    Args:
        output_dir: Directory to write JSON files.
        orders_count: Number of order events.
        clickstream_count: Number of clickstream events.
        inventory_count: Number of inventory events.

    Returns:
        Dict mapping event type to file path.
    """
    import os
    os.makedirs(output_dir, exist_ok=True)

    files = {}
    for event_type, count in [
        ("orders", orders_count),
        ("clickstream", clickstream_count),
        ("inventory", inventory_count),
    ]:
        filepath = os.path.join(output_dir, f"{event_type}.json")
        logger.info(f"Writing {count} {event_type} events to {filepath}")

        with open(filepath, "w") as f:
            for event in generate_events_batch(event_type, count):
                f.write(json.dumps(event) + "\n")

        files[event_type] = filepath
        logger.info(f"Wrote {filepath}")

    # Write merchant dimension
    merchant_path = os.path.join(output_dir, "merchant_dim.json")
    with open(merchant_path, "w") as f:
        for m in generate_merchant_dimension():
            f.write(json.dumps(m) + "\n")
    files["merchant_dim"] = merchant_path

    return files


# ──────────────────────────────────────────────────────────────
# Kafka Producer (requires kafka-python)
# ──────────────────────────────────────────────────────────────

def produce_to_kafka(
    bootstrap_servers: str = "localhost:9092",
    event_type: str = "orders",
    count: int = 10000,
    rate_per_second: int = 1000,
) -> None:
    """Produce events to a Kafka topic.

    Args:
        bootstrap_servers: Kafka broker addresses.
        event_type: Type of events to produce.
        count: Total number of events.
        rate_per_second: Target production rate.
    """
    try:
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )

        topic = f"ecommerce.{event_type}"
        batch_size = max(1, rate_per_second // 10)

        logger.info(
            f"Producing {count} events to {topic}",
            rate_per_second=rate_per_second,
        )

        for i, event in enumerate(generate_events_batch(event_type, count)):
            key = event.get("merchant_id") or "UNKNOWN"
            producer.send(topic, key=key, value=event)

            # Rate limiting
            if (i + 1) % batch_size == 0:
                producer.flush()
                time.sleep(0.1)

        producer.flush()
        producer.close()
        logger.info(f"Produced {count} events to {topic}")

    except ImportError:
        logger.warning("kafka-python not installed. Writing to JSON files instead.")
        write_sample_data_to_json("./data/sample", **{f"{event_type}_count": count})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ecommerce Event Producer")
    parser.add_argument("--mode", choices=["batch", "streaming", "json"], default="json")
    parser.add_argument("--type", choices=["orders", "clickstream", "inventory"], default="orders")
    parser.add_argument("--count", type=int, default=100000)
    parser.add_argument("--rate", type=int, default=1000)
    parser.add_argument("--output-dir", default="./data/sample")
    parser.add_argument("--kafka-brokers", default="localhost:9092")

    args = parser.parse_args()

    if args.mode == "json":
        write_sample_data_to_json(args.output_dir)
    elif args.mode == "batch":
        produce_to_kafka(args.kafka_brokers, args.type, args.count, args.rate)
    elif args.mode == "streaming":
        # Continuous mode — produce indefinitely
        while True:
            produce_to_kafka(args.kafka_brokers, args.type, args.count, args.rate)
            logger.info("Batch complete. Starting next batch...")
