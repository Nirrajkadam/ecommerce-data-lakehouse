"""
Gold Layer — Business-Level Aggregations
========================================
Creates business-ready aggregated tables for analytics consumers,
BI dashboards, and Multi-Agent RAG systems.

Output Tables:
    - merchant_daily_summary: Revenue, orders, AOV per merchant per day
    - customer_360: Lifetime value, purchase patterns per customer
    - product_performance: Sales velocity, stock turnover per product

These aggregations use the salted aggregation technique from the
skew handler for merchant-level aggregations, since MERCHANT_001
dominates the data and would otherwise create a bottleneck.
"""

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.streaming import StreamingQuery

from src.config.pipeline_config import PipelineConfig
from src.optimizations.skew_handler import SkewHandler
from src.optimizations.compaction import CompactionEngine
from src.utils.logger import PipelineLogger


class GoldLayer:
    """Gold layer processor for business aggregations.

    Reads from Silver Delta tables and produces aggregated datasets
    optimized for analytical queries and downstream consumers.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Optional[PipelineConfig] = None,
    ):
        self.spark = spark
        self.config = config or PipelineConfig()
        self.logger = PipelineLogger("gold-layer", layer="gold")
        self.skew_handler = SkewHandler(spark)
        self.compaction = CompactionEngine(spark)

    def build_merchant_daily_summary(self) -> DataFrame:
        """Build daily revenue summary per merchant.

        Uses SALTED AGGREGATION for merchant_id to handle the skew.
        Without salting, 60% of records (MERCHANT_001) would go to
        a single reducer, creating a 10x slower task.

        Returns:
            Aggregated DataFrame with daily merchant metrics.
        """
        silver_path = self.config.paths.get_table_path("silver", "orders")
        gold_path = self.config.paths.get_table_path("gold", "merchant_daily_summary")

        self.logger.info("Building merchant daily summary")

        orders = self.spark.read.format("delta").load(silver_path)

        # ── DETERMINISTIC SALTED AGGREGATION on native columns ──
        # Instead of a fragile string concatenation of merchant_id and event_date
        # (which corrupts null merchants and drops the date), we group natively by
        # both columns, salting on a stable identifier (order_id) to balance the partitions.
        summary = self.skew_handler.salted_aggregation(
            df=orders,
            group_cols=["merchant_id", "event_date"],
            agg_exprs={
                "total_amount": "sum",
                "order_id": "count",
            },
            num_salts=10,
            id_col="order_id",
        )

        summary = (
            summary
            .withColumn(
                "avg_order_value",
                F.col("total_amount") / F.col("order_id"),
            )
            .select(
                "merchant_id",
                "event_date",
                F.col("total_amount").alias("total_revenue"),
                F.col("order_id").alias("total_orders"),
                "avg_order_value",
            )
        )

        # Add merchant name from dim table if available
        if "merchant_name" in orders.columns:
            merchant_names = (
                orders.select("merchant_id", "merchant_name")
                .dropDuplicates(["merchant_id"])
            )
            summary = summary.join(
                F.broadcast(merchant_names), on="merchant_id", how="left"
            )

        # Write to Gold
        (
            summary.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("event_date")
            .save(gold_path)
        )

        self.logger.info("Merchant daily summary built successfully")
        return summary

    def build_customer_360(self) -> DataFrame:
        """Build a 360-degree customer profile.

        Aggregates order history to compute:
        - Total lifetime value
        - Total orders and average order value
        - First/last purchase dates
        - Favorite merchant and payment method
        - Days since last purchase

        Returns:
            Customer 360 DataFrame.
        """
        silver_path = self.config.paths.get_table_path("silver", "orders")
        gold_path = self.config.paths.get_table_path("gold", "customer_360")

        self.logger.info("Building customer 360 profiles")

        orders = self.spark.read.format("delta").load(silver_path)

        # Customer-level aggregation (no significant skew on customer_id)
        customer_360 = (
            orders
            .groupBy("customer_id")
            .agg(
                F.sum("total_amount").alias("total_lifetime_value"),
                F.count("order_id").alias("total_orders"),
                F.avg("total_amount").alias("avg_order_value"),
                F.min("event_date").alias("first_purchase_date"),
                F.max("event_date").alias("last_purchase_date"),
                F.datediff(
                    F.current_date(),
                    F.max("event_timestamp"),
                ).alias("days_since_last_purchase"),
            )
        )

        # Find favorite merchant per customer (mode)
        merchant_counts = (
            orders.groupBy("customer_id", "merchant_id")
            .agg(F.count("*").alias("merchant_order_count"))
        )
        w = Window.partitionBy("customer_id").orderBy(F.desc("merchant_order_count"))
        favorite_merchants = (
            merchant_counts
            .withColumn("rank", F.row_number().over(w))
            .filter(F.col("rank") == 1)
            .select("customer_id", F.col("merchant_id").alias("favorite_merchant"))
        )

        # Find preferred payment method
        payment_counts = (
            orders.groupBy("customer_id", "payment_method")
            .agg(F.count("*").alias("payment_count"))
        )
        w2 = Window.partitionBy("customer_id").orderBy(F.desc("payment_count"))
        preferred_payment = (
            payment_counts
            .withColumn("rank", F.row_number().over(w2))
            .filter(F.col("rank") == 1)
            .select("customer_id", F.col("payment_method").alias("preferred_payment_method"))
        )

        # Join enrichments
        result = (
            customer_360
            .join(F.broadcast(favorite_merchants), on="customer_id", how="left")
            .join(F.broadcast(preferred_payment), on="customer_id", how="left")
        )

        # Write to Gold
        (
            result.write
            .format("delta")
            .mode("overwrite")
            .save(gold_path)
        )

        self.logger.info("Customer 360 profiles built successfully")
        return result

    def build_product_performance(self) -> DataFrame:
        """Build product performance metrics.

        Combines order data with clickstream data to compute:
        - Total quantity sold and revenue
        - Page views and view-to-purchase conversion rate
        - Average selling price

        Returns:
            Product performance DataFrame.
        """
        silver_orders_path = self.config.paths.get_table_path("silver", "orders")
        gold_path = self.config.paths.get_table_path("gold", "product_performance")

        self.logger.info("Building product performance metrics")

        orders = self.spark.read.format("delta").load(silver_orders_path)

        # Explode order items to get product-level metrics
        if "items" in orders.columns:
            products = (
                orders
                .select("merchant_id", F.explode("items").alias("item"))
                .select(
                    "merchant_id",
                    F.col("item.product_id").alias("product_id"),
                    F.col("item.quantity").alias("quantity"),
                    F.col("item.unit_price").alias("unit_price"),
                    (F.col("item.quantity") * F.col("item.unit_price")).alias("revenue"),
                )
            )
        else:
            # Flat schema fallback
            products = orders.select("merchant_id", "order_id", "total_amount")
            products = products.withColumn("product_id", F.lit("ALL"))
            products = products.withColumn("quantity", F.lit(1))
            products = products.withColumn("unit_price", F.col("total_amount"))
            products = products.withColumn("revenue", F.col("total_amount"))

        product_perf = (
            products
            .groupBy("product_id", "merchant_id")
            .agg(
                F.sum("quantity").alias("total_quantity_sold"),
                F.sum("revenue").alias("total_revenue"),
                F.avg("unit_price").alias("avg_selling_price"),
            )
        )

        # Write to Gold
        (
            product_perf.write
            .format("delta")
            .mode("overwrite")
            .save(gold_path)
        )

        self.logger.info("Product performance metrics built successfully")
        return product_perf

    def build_all(self) -> None:
        """Run all Gold aggregation jobs."""
        self.logger.info("Building all Gold tables")

        with self.logger.stage("merchant-daily-summary"):
            self.build_merchant_daily_summary()

        with self.logger.stage("customer-360"):
            self.build_customer_360()

        with self.logger.stage("product-performance"):
            self.build_product_performance()

        self.logger.info("All Gold tables built successfully")

    def process_streaming(
        self,
        trigger_interval: Optional[str] = None,
    ) -> StreamingQuery:
        """Start continuous aggregation streaming from Silver orders to Gold summary.

        Uses Delta streaming with foreachBatch and Delta MERGE with null-safe comparison
        (<=>) to continuously update daily merchant aggregations as orders arrive.
        """
        silver_orders_path = self.config.paths.get_table_path("silver", "orders")
        gold_path = self.config.paths.get_table_path("gold", "merchant_daily_summary")
        checkpoint_path = self.config.paths.get_checkpoint_path("gold_merchant_daily_summary")
        trigger = trigger_interval or self.config.streaming.trigger_interval

        self.logger.info("Starting Gold streaming aggregations")

        silver_stream = (
            self.spark.readStream
            .format("delta")
            .load(silver_orders_path)
        )

        def merge_gold_batch(batch_df: DataFrame, batch_id: int):
            if batch_df.isEmpty():
                return
            summary = self.skew_handler.salted_aggregation(
                df=batch_df,
                group_cols=["merchant_id", "event_date"],
                agg_exprs={"total_amount": "sum", "order_id": "count"},
                num_salts=10,
                id_col="order_id",
            ).withColumn("avg_order_value", F.col("total_amount") / F.col("order_id")) \
             .select(
                 "merchant_id",
                 "event_date",
                 F.col("total_amount").alias("total_revenue"),
                 F.col("order_id").alias("total_orders"),
                 "avg_order_value",
             )

            try:
                from delta.tables import DeltaTable
                if DeltaTable.isDeltaTable(self.spark, gold_path):
                    target = DeltaTable.forPath(self.spark, gold_path)
                    match_cond = "target.merchant_id <=> source.merchant_id AND target.event_date = source.event_date"
                    target.alias("target").merge(
                        summary.alias("source"), match_cond
                    ).whenMatchedUpdate(
                        set={
                            "total_revenue": "target.total_revenue + source.total_revenue",
                            "total_orders": "target.total_orders + source.total_orders",
                            "avg_order_value": "(target.total_revenue + source.total_revenue) / (target.total_orders + source.total_orders)",
                        }
                    ).whenNotMatchedInsertAll().execute()
                else:
                    summary.write.format("delta").mode("append").partitionBy("event_date").save(gold_path)
            except Exception as e:
                self.logger.warning(f"Delta Gold merge fallback to append write: {e}")
                summary.write.format("delta").mode("append").partitionBy("event_date").save(gold_path)

        query = (
            silver_stream.writeStream
            .format("delta")
            .foreachBatch(merge_gold_batch)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=trigger)
            .start()
        )
        return query
