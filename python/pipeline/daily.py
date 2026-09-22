"""Task 3.3: daily revenue from already-clean UTC events."""

from datetime import date

import polars as pl


def daily_revenue(events: pl.DataFrame, day: date) -> pl.DataFrame:
    """Pass read_events(...).events, with versions resolved before date filtering."""
    return (
        events.filter(pl.col("event_time").dt.date() == day)
        .group_by(["app_id", "media_source"])
        .agg(pl.col("revenue_usd").sum().alias("revenue"))
        .sort(["revenue", "app_id", "media_source"], descending=[True, False, False])
    )
