"""
PySpark Schema Definitions for Ecommerce Data
===============================================
Defines StructType schemas for all ecommerce event types flowing through
the Kafka → Medallion pipeline.

Key Design Decision:
    merchant_id is the primary skew key. In production, ~60% of all events
    belong to a single dominant merchant (MERCHANT_001), creating severe
    data skew during joins and aggregations. The schemas and downstream
    processing are designed to handle this distribution.
"""

from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ──────────────────────────────────────────────────────────────
# Raw Kafka Message Schema
# ──────────────────────────────────────────────────────────────
KAFKA_RAW_SCHEMA = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), False),
    StructField("topic", StringType(), False),
    StructField("partition", IntegerType(), False),
    StructField("offset", LongType(), False),
    StructField("timestamp", TimestampType(), False),
])

# ──────────────────────────────────────────────────────────────
# Orders Schema
# ──────────────────────────────────────────────────────────────
ORDER_ITEM_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("discount", DoubleType(), True),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("merchant_id", StringType(), True),     # ← SKEW KEY (~60% MERCHANT_001)
    StructField("customer_id", StringType(), False),
    StructField("order_status", StringType(), False),    # pending, confirmed, shipped, delivered, cancelled
    StructField("total_amount", DoubleType(), False),
    StructField("currency", StringType(), True),
    StructField("payment_method", StringType(), True),   # credit_card, debit_card, upi, wallet, cod
    StructField("items", ArrayType(ORDER_ITEM_SCHEMA), True),
    StructField("shipping_address_city", StringType(), True),
    StructField("shipping_address_state", StringType(), True),
    StructField("event_timestamp", TimestampType(), False),
    StructField("event_date", StringType(), False),      # Partition column: YYYY-MM-DD
])


# ──────────────────────────────────────────────────────────────
# Clickstream Schema
# ──────────────────────────────────────────────────────────────
CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("merchant_id", StringType(), True),     # ← SKEW KEY
    StructField("page", StringType(), False),            # home, product, cart, checkout, confirmation
    StructField("action", StringType(), False),          # view, click, add_to_cart, remove, purchase
    StructField("product_id", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("device_type", StringType(), True),      # mobile, desktop, tablet
    StructField("browser", StringType(), True),
    StructField("duration_seconds", IntegerType(), True),
    StructField("event_timestamp", TimestampType(), False),
    StructField("event_date", StringType(), False),
])


# ──────────────────────────────────────────────────────────────
# Inventory Schema
# ──────────────────────────────────────────────────────────────
INVENTORY_SCHEMA = StructType([
    StructField("inventory_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("merchant_id", StringType(), True),     # ← SKEW KEY
    StructField("warehouse_id", StringType(), False),
    StructField("quantity_available", IntegerType(), False),
    StructField("quantity_reserved", IntegerType(), True),
    StructField("reorder_threshold", IntegerType(), True),
    StructField("unit_cost", DoubleType(), True),
    StructField("last_restock_date", StringType(), True),
    StructField("event_timestamp", TimestampType(), False),
    StructField("event_date", StringType(), False),
])


# ──────────────────────────────────────────────────────────────
# Dimension Tables (for joins in Silver layer)
# ──────────────────────────────────────────────────────────────
MERCHANT_DIM_SCHEMA = StructType([
    StructField("merchant_id", StringType(), False),
    StructField("merchant_name", StringType(), False),
    StructField("category", StringType(), True),         # electronics, fashion, grocery, etc.
    StructField("tier", StringType(), True),              # platinum, gold, silver, bronze
    StructField("region", StringType(), True),
    StructField("onboarding_date", StringType(), True),
    StructField("is_active", StringType(), True),
])

CUSTOMER_DIM_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("segment", StringType(), True),          # premium, regular, new
    StructField("region", StringType(), True),
    StructField("city", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("lifetime_value", DoubleType(), True),
])


# ──────────────────────────────────────────────────────────────
# Bronze Layer Metadata Schema (added during ingestion)
# ──────────────────────────────────────────────────────────────
BRONZE_METADATA_FIELDS = [
    StructField("_ingestion_timestamp", TimestampType(), False),
    StructField("_source_topic", StringType(), False),
    StructField("_batch_id", StringType(), True),
    StructField("_raw_json", StringType(), True),
    StructField("_kafka_partition", IntegerType(), True),
    StructField("_kafka_offset", LongType(), True),
]


# ──────────────────────────────────────────────────────────────
# Gold Layer Aggregation Schemas
# ──────────────────────────────────────────────────────────────
MERCHANT_DAILY_SUMMARY_SCHEMA = StructType([
    StructField("merchant_id", StringType(), False),
    StructField("merchant_name", StringType(), True),
    StructField("event_date", StringType(), False),
    StructField("total_revenue", DoubleType(), False),
    StructField("total_orders", LongType(), False),
    StructField("avg_order_value", DoubleType(), False),
    StructField("unique_customers", LongType(), False),
    StructField("total_page_views", LongType(), True),
    StructField("conversion_rate", DoubleType(), True),
])

CUSTOMER_360_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("total_lifetime_value", DoubleType(), False),
    StructField("total_orders", LongType(), False),
    StructField("avg_order_value", DoubleType(), False),
    StructField("first_purchase_date", StringType(), True),
    StructField("last_purchase_date", StringType(), True),
    StructField("days_since_last_purchase", IntegerType(), True),
    StructField("favorite_merchant", StringType(), True),
    StructField("preferred_payment_method", StringType(), True),
])

PRODUCT_PERFORMANCE_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("merchant_id", StringType(), True),
    StructField("total_quantity_sold", LongType(), False),
    StructField("total_revenue", DoubleType(), False),
    StructField("avg_selling_price", DoubleType(), False),
    StructField("total_page_views", LongType(), True),
    StructField("view_to_purchase_rate", DoubleType(), True),
    StructField("current_stock", IntegerType(), True),
    StructField("stock_turnover_days", IntegerType(), True),
])


# ──────────────────────────────────────────────────────────────
# Schema Registry (for dynamic schema lookup)
# ──────────────────────────────────────────────────────────────
SCHEMA_REGISTRY = {
    "orders": ORDERS_SCHEMA,
    "clickstream": CLICKSTREAM_SCHEMA,
    "inventory": INVENTORY_SCHEMA,
    "merchant_dim": MERCHANT_DIM_SCHEMA,
    "customer_dim": CUSTOMER_DIM_SCHEMA,
    "merchant_daily_summary": MERCHANT_DAILY_SUMMARY_SCHEMA,
    "customer_360": CUSTOMER_360_SCHEMA,
    "product_performance": PRODUCT_PERFORMANCE_SCHEMA,
}


def get_schema(table_name: str) -> StructType:
    """Look up a schema by table name.

    Args:
        table_name: Name of the table/event type.

    Returns:
        PySpark StructType schema.

    Raises:
        KeyError: If the table name is not found in the registry.
    """
    if table_name not in SCHEMA_REGISTRY:
        raise KeyError(
            f"Schema not found for '{table_name}'. "
            f"Available schemas: {list(SCHEMA_REGISTRY.keys())}"
        )
    return SCHEMA_REGISTRY[table_name]
