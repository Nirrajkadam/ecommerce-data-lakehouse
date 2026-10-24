"""
Tests for Medallion layers (Bronze, Silver, Gold).
"""

import pytest
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, TimestampType
)
from src.layers.silver_layer import SilverLayer
from src.layers.gold_layer import GoldLayer
from src.optimizations.skew_handler import SkewHandler


class TestSilverLayer:
    """Tests for Silver layer data processing and deduplication."""

    def test_clean_orders_deduplicates_retaining_newest_timestamp(self, spark, sample_orders_df):
        """Verify that duplicate orders are deduplicated and the newest record is retained."""
        silver = SilverLayer(spark)
        cleaned = silver._clean_orders(sample_orders_df)

        # ORD001 appears twice in fixture: initial ₹100.0 at 10:00 AM, updated ₹200.0 at 11:30 AM
        ord001_records = cleaned.filter(F.col("order_id") == "ORD001").collect()
        assert len(ord001_records) == 1

        ord001 = ord001_records[0]
        # Must retain the newer ₹200.0 record with "confirmed" status, NOT the older ₹100.0 "pending" record
        assert ord001["total_amount"] == 200.0
        assert ord001["order_status"] == "confirmed"

        # Total unique orders should be exactly 3
        assert cleaned.count() == 3

    def test_clean_orders_filters_invalid_amounts(self, spark):
        """Verify that orders with invalid or non-positive amounts are filtered."""
        data = [
            ("ORD_VALID", "M1", 100.0, "confirmed", "2024-01-01", datetime(2024, 1, 1, 10, 0)),
            ("ORD_ZERO", "M1", 0.0, "confirmed", "2024-01-01", datetime(2024, 1, 1, 10, 0)),
            ("ORD_NEG", "M1", -50.0, "confirmed", "2024-01-01", datetime(2024, 1, 1, 10, 0)),
        ]
        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("merchant_id", StringType(), True),
            StructField("total_amount", DoubleType(), False),
            StructField("order_status", StringType(), False),
            StructField("event_date", StringType(), False),
            StructField("event_timestamp", TimestampType(), True),
        ])
        df = spark.createDataFrame(data, schema)

        silver = SilverLayer(spark)
        cleaned = silver._clean_orders(df)

        # Only the valid order should remain
        assert cleaned.count() == 1
        assert cleaned.first()["order_id"] == "ORD_VALID"

    def test_silver_handles_null_merchant_ids(
        self, spark, skewed_df_with_nulls, merchant_dim_df
    ):
        """Verify that Silver layer handles null merchant_ids during joins without dropping records."""
        silver = SilverLayer(spark)
        result = silver.enrich_with_merchant_data(
            skewed_df_with_nulls, merchant_dim_df
        )

        # All records should be preserved (including null merchant_ids)
        assert result.count() == skewed_df_with_nulls.count()

    def test_enrichment_adds_merchant_columns(
        self, spark, skewed_df, merchant_dim_df
    ):
        """Verify that enrichment adds merchant dimension columns."""
        silver = SilverLayer(spark)
        result = silver.enrich_with_merchant_data(skewed_df, merchant_dim_df)

        assert "merchant_name" in result.columns
        assert "category" in result.columns
        assert "tier" in result.columns


class TestGoldLayer:
    """Tests for Gold layer business aggregations."""

    def test_gold_aggregation_preserves_distinct_dates_for_null_merchants(self, spark):
        """Regression test: verify that null merchants across distinct dates do not collapse into one null-date row.

        Reproduced issue: ₹100 on 2024-01-15 and ₹200 on 2024-01-16 with null merchant
        previously became one ₹300 row with null date due to string concat().
        Native multi-column grouping must preserve two distinct rows with their respective dates.
        """
        data = [
            (None, "ORD_N1", 100.0, "2024-01-15"),
            (None, "ORD_N2", 200.0, "2024-01-16"),
            ("MERCHANT_001", "ORD_M1", 500.0, "2024-01-15"),
        ]
        schema = StructType([
            StructField("merchant_id", StringType(), True),
            StructField("order_id", StringType(), False),
            StructField("total_amount", DoubleType(), False),
            StructField("event_date", StringType(), False),
        ])
        df = spark.createDataFrame(data, schema)

        handler = SkewHandler(spark)
        summary = handler.salted_aggregation(
            df=df,
            group_cols=["merchant_id", "event_date"],
            agg_exprs={"total_amount": "sum", "order_id": "count"},
            num_salts=5,
            id_col="order_id",
        )

        rows = summary.collect()
        # Should have 3 distinct rows:
        # 1. merchant=None, date='2024-01-15', amount=100.0
        # 2. merchant=None, date='2024-01-16', amount=200.0
        # 3. merchant='MERCHANT_001', date='2024-01-15', amount=500.0
        assert len(rows) == 3

        null_rows = [r for r in rows if r["merchant_id"] is None]
        assert len(null_rows) == 2

        dates = {r["event_date"] for r in null_rows}
        assert "2024-01-15" in dates
        assert "2024-01-16" in dates

        # Ensure amounts are distinct per date, not aggregated together into 300
        amounts = {r["event_date"]: r["total_amount"] for r in null_rows}
        assert amounts["2024-01-15"] == 100.0
        assert amounts["2024-01-16"] == 200.0
