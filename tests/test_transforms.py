from pyspark.sql import types as T

from pipelines.silver.transforms import normalize_silver_events


def test_normalize_extracts_search_and_utm(spark):
    schema = T.StructType(
        [
            T.StructField("event_id", T.StringType()),
            T.StructField("occurred_at", T.StringType()),
            T.StructField("site_id", T.StringType()),
            T.StructField("event_type", T.StringType()),
            T.StructField("session_id", T.StringType()),
            T.StructField("user_id", T.StringType()),
            T.StructField("url", T.StringType()),
            T.StructField("referrer", T.StringType()),
            T.StructField(
                "properties",
                T.MapType(T.StringType(), T.StringType()),
            ),
        ]
    )
    rows = [
        (
            "e1",
            "2026-05-16T12:00:00Z",
            "s1",
            "SEARCH",
            "sess",
            "u1",
            "https://x.com?q=1",
            "https://ref",
            {"query": "shoes", "utm_source": "newsletter"},
        )
    ]
    raw = spark.createDataFrame(rows, schema)
    out = normalize_silver_events(raw).collect()[0]
    assert out["search_query"] == "shoes"
    assert out["utm_source"] == "newsletter"
    assert out["event_type"] == "search"
    assert out["event_hour"] == 12


def test_normalize_filters_invalid_rows(spark):
    schema = T.StructType(
        [
            T.StructField("event_id", T.StringType(), True),
            T.StructField("occurred_at", T.StringType(), True),
            T.StructField("site_id", T.StringType(), True),
            T.StructField("event_type", T.StringType(), True),
        ]
    )
    rows = [
        ("ok", "2026-05-16T12:00:00Z", "s1", "page_view"),
        (None, "2026-05-16T12:00:00Z", "s1", "page_view"),
    ]
    raw = spark.createDataFrame(rows, schema)
    assert normalize_silver_events(raw).count() == 1


def test_zero_results_coercion(spark):
    schema = T.StructType(
        [
            T.StructField("event_id", T.StringType()),
            T.StructField("occurred_at", T.StringType()),
            T.StructField("site_id", T.StringType()),
            T.StructField("event_type", T.StringType()),
            T.StructField("properties", T.MapType(T.StringType(), T.StringType())),
        ]
    )
    rows = [("e1", "2026-05-16T12:00:00Z", "s1", "search", {"query": "x", "zero_results": "true"})]
    raw = spark.createDataFrame(rows, schema)
    row = normalize_silver_events(raw).first()
    assert row["zero_results"] is True


def test_bronze_sdk_column_names_map_to_silver(spark):
    """Bronze renames siteId → event_site_id and anonymousId → anonymous_id."""
    schema = T.StructType(
        [
            T.StructField("event_id", T.StringType()),
            T.StructField("occurred_at", T.StringType()),
            T.StructField("event_site_id", T.StringType()),
            T.StructField("event_type", T.StringType()),
            T.StructField("anonymous_id", T.StringType()),
        ]
    )
    rows = [("e1", "2026-05-16T12:00:00Z", "shop-1", "page_view", "anon-9")]
    raw = spark.createDataFrame(rows, schema)
    row = normalize_silver_events(raw).first()
    assert row["site_id"] == "shop-1"
    assert row["user_id"] == "anon-9"
