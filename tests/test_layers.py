"""
Tests for Medallion layers (Bronze, Silver, Gold).
"""

import pytest
from pyspark.sql import functions as F
from src.layers.silver_layer import SilverLayer


class TestSilverLayer:
    """Tests for Silver layer data processing."""

    def test_clean_orders_deduplicates(self, spark, sample_orders_df):
        """Verify that duplicate orders are removed."""
        silver = SilverLayer(spark)
        cleaned = silver._clean_orders(sample_orders_df)

        # ORD001 appears twice in fixture — should be deduped to 1
        ord001_count = cleaned.filter(F.col("order_id") == "ORD001").count()
        assert ord001_count == 1

        # Total should be 3 unique orders
        assert cleaned.count() == 3

    def test_clean_orders_filters_invalid_amounts(self, spark):
        """Verify that orders with invalid amounts are filtered."""
        data = [
            ("ORD_VALID", "M1", 100.0, "confirmed", "2024-01-01"),
            ("ORD_ZERO", "M1", 0.0, "confirmed", "2024-01-01"),
            ("ORD_NEG", "M1", -50.0, "confirmed", "2024-01-01"),
        ]
        df = spark.createDataFrame(
            data, ["order_id", "merchant_id", "total_amount", "order_status", "event_date"]
        )

        silver = SilverLayer(spark)
        cleaned = silver._clean_orders(df)

        # Only the valid order should remain
        assert cleaned.count() == 1
        assert cleaned.first()["order_id"] == "ORD_VALID"

    def test_silver_handles_null_merchant_ids(
        self, spark, skewed_df_with_nulls, merchant_dim_df
    ):
        """Verify that Silver layer handles null merchant_ids in joins."""
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
