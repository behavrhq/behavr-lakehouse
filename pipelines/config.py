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

    # Raw append-only event storage
    raw_events_path: str = "s3://behavr-lake/raw/events/"

    # Unity Catalog volume-backed pipeline state
    pipeline_state_volume: str = "/Volumes/behavr/bronze/pipeline_state"

    bronze_table: str = "raw_events"
    silver_table: str = "events"

    @property
    def bronze_fqn(self) -> str:
        return (
            f"{self.catalog}."
            f"{self.bronze_schema}."
            f"{self.bronze_table}"
        )

    @property
    def silver_fqn(self) -> str:
        return (
            f"{self.catalog}."
            f"{self.silver_schema}."
            f"{self.silver_table}"
        )

    @property
    def schema_tracking_path(self) -> str:
        return (
            f"{self.pipeline_state_volume}/schemas"
        )

    @property
    def checkpoint_path(self) -> str:
        return (
            f"{self.pipeline_state_volume}/checkpoints"
        )

    @classmethod
    def from_env(cls) -> "LakehouseConfig":
        return cls(
            catalog=_env(
                "BEHAVR_CATALOG",
                "behavr",
            ),
            bronze_schema=_env(
                "BEHAVR_BRONZE_SCHEMA",
                "bronze",
            ),
            silver_schema=_env(
                "BEHAVR_SILVER_SCHEMA",
                "silver",
            ),
            gold_schema=_env(
                "BEHAVR_GOLD_SCHEMA",
                "gold",
            ),
            raw_events_path=_env(
                "BEHAVR_RAW_EVENTS_PATH",
                "s3://behavr-lake/raw/events/",
            ),
            pipeline_state_volume=_env(
                "BEHAVR_PIPELINE_STATE_VOLUME",
                "/Volumes/behavr/bronze/pipeline_state",
            ),
            bronze_table=_env(
                "BEHAVR_BRONZE_TABLE",
                "raw_events",
            ),
            silver_table=_env(
                "BEHAVR_SILVER_TABLE",
                "events",
            ),
        )
