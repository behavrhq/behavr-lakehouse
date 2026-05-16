"""Lightweight contract checks for silver output columns."""

from pyspark.sql import types as T

from pipelines.silver.transforms import normalize_silver_events


def test_silver_schema_has_expected_columns(spark):
    schema = T.StructType(
        [
            T.StructField("event_id", T.StringType()),
            T.StructField("occurred_at", T.StringType()),
            T.StructField("site_id", T.StringType()),
            T.StructField("event_type", T.StringType()),
        ]
    )
    df = spark.createDataFrame([("e", "2026-01-01T00:00:00Z", "s", "page_view")], schema)
    out = normalize_silver_events(df)
    names = set(out.columns)
    for required in (
        "event_id",
        "occurred_at_utc",
        "site_id",
        "event_type",
        "event_date",
        "event_hour",
        "search_query",
        "product_id",
        "category_id",
        "zero_results",
        "page_url",
        "utm_source",
        "_silver_updated_at",
    ):
        assert required in names
