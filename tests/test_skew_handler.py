"""
Tests for the Data Skew Handler module.
"""

import pytest
from pyspark.sql import functions as F
from src.optimizations.skew_handler import SkewHandler


class TestSkewDetection:
    """Tests for skew detection functionality."""

    def test_detect_skew_identifies_hot_keys(self, spark, skewed_df):
        """Verify that detect_skew correctly identifies MERCHANT_001 as hot."""
        handler = SkewHandler(spark)
        report = handler.detect_skew(skewed_df, "merchant_id", threshold=5.0)

        assert report["is_skewed"] is True
        assert report["skew_ratio"] > 5.0
        assert report["top_keys"][0][0] == "MERCHANT_001"
        # MERCHANT_001 should have ~60% of records
        assert report["top_keys"][0][2] > 50.0

    def test_detect_skew_balanced_data(self, spark):
        """Verify that balanced data is NOT flagged as skewed."""
        data = [(f"KEY_{i}", i) for i in range(100)]
        df = spark.createDataFrame(data, ["key", "value"])

        handler = SkewHandler(spark)
        report = handler.detect_skew(df, "key", threshold=5.0)

        assert report["is_skewed"] is False
        assert report["skew_ratio"] <= 5.0

    def test_detect_skew_returns_recommendation(self, spark, skewed_df):
        """Verify that recommendations are provided based on severity."""
        handler = SkewHandler(spark)
        report = handler.detect_skew(skewed_df, "merchant_id")

        assert "recommendation" in report
        assert len(report["recommendation"]) > 0


class TestNullKeyIsolation:
    """Tests for null/default key isolation."""

    def test_isolate_null_keys_separates_correctly(self, spark, skewed_df_with_nulls):
        """Verify that null keys are correctly separated."""
        handler = SkewHandler(spark)
        normal_df, null_df = handler.isolate_null_keys(
            skewed_df_with_nulls, "merchant_id"
        )

        # Null records should be separated
        assert null_df.count() > 0

        # Normal records should not contain null merchant_ids
        null_in_normal = normal_df.filter(
            F.col("merchant_id").isNull() |
            F.col("merchant_id").isin(["", "NULL", "UNKNOWN"])
        ).count()
        assert null_in_normal == 0

        # Total should be preserved
        assert normal_df.count() + null_df.count() == skewed_df_with_nulls.count()

    def test_isolate_null_keys_custom_values(self, spark):
        """Verify custom null values are handled."""
        data = [
            ("VALID_KEY", 1),
            ("CUSTOM_NULL", 2),
            ("ALSO_NULL", 3),
            ("VALID_KEY_2", 4),
        ]
        df = spark.createDataFrame(data, ["key", "value"])

        handler = SkewHandler(spark)
        normal_df, null_df = handler.isolate_null_keys(
            df, "key", null_values=["CUSTOM_NULL", "ALSO_NULL"]
        )

        assert normal_df.count() == 2
        assert null_df.count() == 2


class TestSaltedJoin:
    """Tests for salted join correctness."""

    def test_salted_join_produces_correct_results(
        self, spark, skewed_df, merchant_dim_df
    ):
        """Verify that salted join produces same results as naive join."""
        handler = SkewHandler(spark)

        # Naive join (ground truth)
        naive_result = skewed_df.join(
            merchant_dim_df, on="merchant_id", how="inner"
        ).count()

        # Salted join
        salted_result = handler.salted_join(
            skewed_df, merchant_dim_df, "merchant_id",
            num_salts=5, join_type="inner"
        ).count()

        assert salted_result == naive_result

    def test_salted_join_preserves_all_columns(
        self, spark, skewed_df, merchant_dim_df
    ):
        """Verify that salted join preserves all columns from both sides."""
        handler = SkewHandler(spark)
        result = handler.salted_join(
            skewed_df, merchant_dim_df, "merchant_id", num_salts=3
        )

        # Should have columns from both DataFrames
        assert "merchant_id" in result.columns
        assert "order_id" in result.columns
        assert "total_amount" in result.columns
        assert "merchant_name" in result.columns
        assert "category" in result.columns

        # Should NOT have salt columns
        assert "_salt" not in result.columns
        assert "_salted_key" not in result.columns


class TestSaltedAggregation:
    """Tests for two-stage salted aggregation."""

    def test_salted_aggregation_matches_naive(self, spark, skewed_df):
        """Verify that salted aggregation produces same results as naive."""
        handler = SkewHandler(spark)

        # Naive aggregation (ground truth)
        naive_result = (
            skewed_df.groupBy("merchant_id")
            .agg(
                F.sum("total_amount").alias("total_amount"),
                F.count("order_id").alias("order_id"),
            )
            .orderBy("merchant_id")
            .collect()
        )

        # Salted aggregation
        salted_result = (
            handler.salted_aggregation(
                skewed_df,
                "merchant_id",
                {"total_amount": "sum", "order_id": "count"},
                num_salts=5,
            )
            .orderBy("merchant_id")
            .collect()
        )

        # Compare results
        assert len(salted_result) == len(naive_result)
        for naive_row, salted_row in zip(naive_result, salted_result):
            assert naive_row["merchant_id"] == salted_row["merchant_id"]
            assert abs(naive_row["total_amount"] - salted_row["total_amount"]) < 0.01
            assert naive_row["order_id"] == salted_row["order_id"]


class TestAQEConfiguration:
    """Tests for AQE configuration."""

    def test_aqe_config_sets_all_params(self, spark):
        """Verify that AQE configuration sets all required parameters."""
        handler = SkewHandler(spark)
        handler.configure_aqe_skew(skew_factor=3)

        assert spark.conf.get("spark.sql.adaptive.enabled") == "true"
        assert spark.conf.get("spark.sql.adaptive.skewJoin.enabled") == "true"
        assert spark.conf.get(
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor"
        ) == "3"


class TestFixSkewedJoin:
    """Tests for the full skew-fix playbook."""

    def test_fix_skewed_join_handles_null_keys(
        self, spark, skewed_df_with_nulls, merchant_dim_df
    ):
        """Verify the full playbook handles null keys correctly."""
        handler = SkewHandler(spark)
        result = handler.fix_skewed_join(
            skewed_df_with_nulls, merchant_dim_df, "merchant_id",
            dim_size_mb=20.0,
        )

        # All records should be preserved
        assert result.count() == skewed_df_with_nulls.count()
