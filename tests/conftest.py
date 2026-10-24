"""
Pytest fixtures for ecommerce data lakehouse tests.
"""

import os
import shutil
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType,
)


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing with Delta Lake and AQE support.

    Attempts to configure Delta extensions and packages via configure_spark_with_delta_pip
    or Maven coordinates. If Delta jars are not present in the local offline cache, falls
    back gracefully to standard SparkSession so transformation unit tests still pass.
    """
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("ecommerce-lakehouse-tests")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
    )

    # Attempt to load Delta extensions and packages
    try:
        from delta import configure_spark_with_delta_pip
        builder = (
            builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        builder = configure_spark_with_delta_pip(
            builder,
            extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"]
        )
        spark_session = builder.getOrCreate()
    except Exception:
        # Fallback without DeltaCatalog if Delta jars are unavailable in clean/offline environment
        fallback_builder = (
            SparkSession.builder
            .master("local[2]")
            .appName("ecommerce-lakehouse-tests-fallback")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "2g")
            .config("spark.ui.enabled", "false")
        )
        spark_session = fallback_builder.getOrCreate()

    spark_session.sparkContext.setLogLevel("ERROR")
    yield spark_session
    spark_session.stop()


@pytest.fixture
def skewed_df(spark):
    """Create a DataFrame with skewed merchant_id distribution.

    MERCHANT_001 accounts for 60% of records (600 out of 1000).
    Includes complete temporal columns for deduplication and aggregation testing.
    """
    data = []
    base_ts = datetime(2024, 1, 15, 10, 0, 0)
    # 600 records for MERCHANT_001 (60% - the hot key)
    for i in range(600):
        data.append(("MERCHANT_001", f"order_{i}", 100.0 + i, "2024-01-15", base_ts))
    # 100 records for MERCHANT_002 (10%)
    for i in range(100):
        data.append(("MERCHANT_002", f"order_{600 + i}", 200.0 + i, "2024-01-15", base_ts))
    # 50 each for MERCHANT_003..008
    for m in range(3, 9):
        for i in range(50):
            idx = 700 + (m - 3) * 50 + i
            data.append((f"MERCHANT_{m:03d}", f"order_{idx}", 50.0 + i, "2024-01-15", base_ts))

    schema = StructType([
        StructField("merchant_id", StringType(), True),
        StructField("order_id", StringType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("event_date", StringType(), False),
        StructField("event_timestamp", TimestampType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def skewed_df_with_nulls(spark, skewed_df):
    """Create a skewed DataFrame containing null and default merchant_ids across multiple dates."""
    base_ts1 = datetime(2024, 1, 15, 10, 0, 0)
    base_ts2 = datetime(2024, 1, 16, 11, 0, 0)
    null_data = [
        (None, "order_null_1", 10.0, "2024-01-15", base_ts1),
        ("", "order_empty_1", 20.0, "2024-01-15", base_ts1),
        ("NULL", "order_null_2", 30.0, "2024-01-16", base_ts2),
        ("UNKNOWN", "order_unknown_1", 40.0, "2024-01-16", base_ts2),
    ]
    null_schema = StructType([
        StructField("merchant_id", StringType(), True),
        StructField("order_id", StringType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("event_date", StringType(), False),
        StructField("event_timestamp", TimestampType(), True),
    ])
    null_df = spark.createDataFrame(null_data, null_schema)
    return skewed_df.unionByName(null_df)


@pytest.fixture
def merchant_dim_df(spark):
    """Create a small merchant dimension table (for broadcast joins)."""
    merchants = [
        ("MERCHANT_001", "MegaStore", "electronics", "platinum"),
        ("MERCHANT_002", "FashionHub", "fashion", "gold"),
        ("MERCHANT_003", "GroceryMart", "grocery", "silver"),
        ("MERCHANT_004", "HomeDecor", "home_garden", "bronze"),
        ("MERCHANT_005", "SportZone", "sports", "gold"),
        ("MERCHANT_006", "TechGear", "electronics", "silver"),
        ("MERCHANT_007", "BookWorld", "books", "bronze"),
        ("MERCHANT_008", "PetShop", "pets", "silver"),
    ]
    schema = StructType([
        StructField("merchant_id", StringType(), False),
        StructField("merchant_name", StringType(), False),
        StructField("category", StringType(), True),
        StructField("tier", StringType(), True),
    ])
    return spark.createDataFrame(merchants, schema)


@pytest.fixture
def temp_delta_path(tmp_path):
    """Provide a temporary path for Delta table tests."""
    delta_path = str(tmp_path / "delta_test")
    yield delta_path
    if os.path.exists(delta_path):
        shutil.rmtree(delta_path, ignore_errors=True)


@pytest.fixture
def sample_orders_df(spark):
    """Create sample orders DataFrame with timestamps for deterministic deduplication testing.

    Includes two updates for ORD001:
    - Initial order: ₹100.0 at 10:00 AM (pending)
    - Updated order: ₹200.0 at 11:30 AM (confirmed)
    Deterministic deduplication must retain the newer ₹200.0 update.
    """
    data = [
        ("ORD001", "MERCHANT_001", "CUST_001", "pending", 100.0, "INR",
         "credit_card", "Mumbai", "Maharashtra", datetime(2024, 1, 15, 10, 0, 0), "2024-01-15"),
        ("ORD002", "MERCHANT_001", "CUST_002", "shipped", 250.0, "INR",
         "upi", "Delhi", "Delhi", datetime(2024, 1, 15, 11, 0, 0), "2024-01-15"),
        ("ORD003", "MERCHANT_002", "CUST_001", "delivered", 75.0, "INR",
         "wallet", "Pune", "Maharashtra", datetime(2024, 1, 16, 9, 30, 0), "2024-01-16"),
        ("ORD001", "MERCHANT_001", "CUST_001", "confirmed", 200.0, "INR",
         "credit_card", "Mumbai", "Maharashtra", datetime(2024, 1, 15, 11, 30, 0), "2024-01-15"),
    ]
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("merchant_id", StringType(), True),
        StructField("customer_id", StringType(), False),
        StructField("order_status", StringType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("shipping_address_city", StringType(), True),
        StructField("shipping_address_state", StringType(), True),
        StructField("event_timestamp", TimestampType(), False),
        StructField("event_date", StringType(), False),
    ])
    return spark.createDataFrame(data, schema)
