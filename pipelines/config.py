"""Central configuration for catalog, paths, and table names."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v is not None and v != "" else default


@dataclass(frozen=True)
class LakehouseConfig:
    """Unity Catalog and storage layout."""

    catalog: str = "behavr"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"

    raw_events_path: str = "s3://behavr-lake/raw/events/"
    checkpoint_base: str = "s3://behavr-lake/checkpoints/pipelines"

    bronze_table: str = "raw_events"
    silver_table: str = "events"

    @property
    def bronze_fqn(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{self.bronze_table}"

    @property
    def silver_fqn(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.{self.silver_table}"

    @classmethod
    def from_env(cls) -> LakehouseConfig:
        return cls(
            catalog=_env("BEHAVR_CATALOG", "behavr"),
            bronze_schema=_env("BEHAVR_BRONZE_SCHEMA", "bronze"),
            silver_schema=_env("BEHAVR_SILVER_SCHEMA", "silver"),
            gold_schema=_env("BEHAVR_GOLD_SCHEMA", "gold"),
            raw_events_path=_env("BEHAVR_RAW_EVENTS_PATH", "s3://behavr-lake/raw/events/"),
            checkpoint_base=_env("BEHAVR_CHECKPOINT_BASE", "s3://behavr-lake/checkpoints/pipelines"),
            bronze_table=_env("BEHAVR_BRONZE_TABLE", "raw_events"),
            silver_table=_env("BEHAVR_SILVER_TABLE", "events"),
        )
