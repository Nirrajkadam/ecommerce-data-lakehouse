"""
Pytest fixtures for ecommerce data lakehouse tests.
"""

import os
import shutil
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType,
)


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing with Delta Lake support."""
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("ecommerce-lakehouse-tests")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def skewed_df(spark):
    """Create a DataFrame with skewed merchant_id distribution.

    MERCHANT_001 gets 60% of records (600 out of 1000).
    """
    data = []
    # 600 records for MERCHANT_001 (60% — the hot key)
    for i in range(600):
        data.append(("MERCHANT_001", f"order_{i}", 100.0 + i))
    # 100 records for MERCHANT_002 (10%)
    for i in range(100):
        data.append(("MERCHANT_002", f"order_{600 + i}", 200.0 + i))
    # 50 each for MERCHANT_003..008
    for m in range(3, 9):
        for i in range(50):
            idx = 700 + (m - 3) * 50 + i
            data.append((f"MERCHANT_{m:03d}", f"order_{idx}", 50.0 + i))

    schema = StructType([
        StructField("merchant_id", StringType(), True),
        StructField("order_id", StringType(), False),
        StructField("total_amount", DoubleType(), False),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def skewed_df_with_nulls(spark, skewed_df):
    """Create a skewed DataFrame that also has null/default merchant_ids."""
    null_data = [
        (None, "order_null_1", 10.0),
        ("", "order_empty_1", 20.0),
        ("NULL", "order_null_2", 30.0),
        ("UNKNOWN", "order_unknown_1", 40.0),
    ]
    null_schema = StructType([
        StructField("merchant_id", StringType(), True),
        StructField("order_id", StringType(), False),
        StructField("total_amount", DoubleType(), False),
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
    """Create a sample orders DataFrame for layer tests."""
    data = [
        ("ORD001", "MERCHANT_001", "CUST_001", "confirmed", 150.0, "INR",
         "credit_card", "Mumbai", "Maharashtra", "2024-01-15"),
        ("ORD002", "MERCHANT_001", "CUST_002", "shipped", 250.0, "INR",
         "upi", "Delhi", "Delhi", "2024-01-15"),
        ("ORD003", "MERCHANT_002", "CUST_001", "delivered", 75.0, "INR",
         "wallet", "Pune", "Maharashtra", "2024-01-16"),
        ("ORD001", "MERCHANT_001", "CUST_001", "confirmed", 150.0, "INR",
         "credit_card", "Mumbai", "Maharashtra", "2024-01-15"),  # Duplicate
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
        StructField("event_date", StringType(), False),
    ])
    return spark.createDataFrame(data, schema)
