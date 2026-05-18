"""
Gold: product engagement — views, add-to-cart, purchases by product.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipelines.config import LakehouseConfig
from pipelines.gold.common import merge_or_create, read_silver_events


def build_product_metrics(
    spark: SparkSession,
    cfg: LakehouseConfig,
    *,
    for_event_dates: Sequence[date] | None = None,
):
    e = read_silver_events(spark, cfg, for_event_dates=for_event_dates).where(F.col("product_id").isNotNull())
    et = F.col("event_type")
    views = F.sum(F.when(et == "product_view", 1).otherwise(0))
    atc = F.sum(F.when(et.isin("add_to_cart", "addtocart", "cart_add"), 1).otherwise(0))
    purchases = F.sum(F.when(et.isin("purchase", "order_complete", "checkout_complete"), 1).otherwise(0))
    return (
        e.groupBy("site_id", "event_date", "product_id")
        .agg(
            views.alias("product_views"),
            atc.alias("add_to_cart_events"),
            purchases.alias("purchase_events"),
        )
        .withColumn(
            "add_to_cart_rate",
            F.when(F.col("product_views") > 0, F.col("add_to_cart_events") / F.col("product_views")).otherwise(
                F.lit(None)
            ),
        )
        .withColumn(
            "purchase_conversion_rate",
            F.when(F.col("product_views") > 0, F.col("purchase_events") / F.col("product_views")).otherwise(
                F.lit(None)
            ),
        )
        .withColumn("_gold_updated_at", F.current_timestamp())
    )


def run_gold_product_metrics(
    spark: SparkSession,
    config: LakehouseConfig | None = None,
    *,
    for_event_dates: Sequence[date] | None = None,
) -> None:
    cfg = config or LakehouseConfig.from_env()
    agg = build_product_metrics(spark, cfg, for_event_dates=for_event_dates)
    merge_or_create(
        spark,
        cfg,
        "product_metrics",
        agg,
        merge_condition="""
            t.site_id = s.site_id
            AND t.event_date = s.event_date
            AND t.product_id <=> s.product_id
        """,
        partition_cols=["event_date"],
    )


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    cfg = LakehouseConfig.from_env()

    print("Starting gold product metrics pipeline")
    print(f"Reading from: {cfg.silver_fqn}")
    print("Writing to: product_metrics")

    run_gold_product_metrics(
        spark=spark,
        config=cfg,
    )

    print("Gold product metrics pipeline completed")
