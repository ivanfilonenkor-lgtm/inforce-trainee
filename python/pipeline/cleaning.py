"""Validate individual CSV records, then resolve event versions with Polars."""

from __future__ import annotations

import csv
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import polars as pl


SCHEMA = {
    "event_id": pl.String,
    "user_id": pl.String,
    "app_id": pl.String,
    "event_name": pl.String,
    "event_time": pl.Datetime("us", "UTC"),
    "ingested_at": pl.Datetime("us", "UTC"),
    "country": pl.String,
    "media_source": pl.String,
    "campaign": pl.String,
    "revenue_usd": pl.Float64,
    "is_test": pl.Boolean,
}

# Descending order, identical to SQL task 2.1. All fields are non-null.
# The payload order is a deterministic fallback
VERSION_ORDER = [
    "ingested_at", "event_time", "app_id", "user_id",
    "event_name", "country", "media_source", "campaign", "revenue_usd",
]

@dataclass
class CleaningResult:
    events: pl.DataFrame
    quarantine: list[dict]
    stats: dict[str, int]
    deliveries: pl.DataFrame


def empty_events() -> pl.DataFrame:
    return pl.DataFrame(schema=SCHEMA)


def parse_timestamp(value: str) -> datetime:
    """Naive vendor timestamps mean UTC; explicit offsets are converted to UTC."""
    if "T" not in value and " " not in value:
        raise ValueError("timestamp must include a time")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_revenue(value: str) -> tuple[float, bool]:
    try:
        amount = float(value.replace(",", "."))
        if math.isfinite(amount):
            return amount, False
    except (ValueError, OverflowError):
        pass
    return 0.0, True


def normalise_country(value: str) -> str:
    """Validate the two-letter format"""
    code = value.strip().upper()
    if len(code) == 2 and code.isascii() and code.isalpha():
        return code
    return "XX"


def latest_versions(deliveries: pl.DataFrame) -> pl.DataFrame:
    """Keep the latest non-test version per ID, matching SQL task 2.1."""
    return (
        deliveries.filter(~pl.col("is_test"))
        .sort(VERSION_ORDER, descending=True)
        .unique(subset="event_id", keep="first", maintain_order=True)
        .sort("event_id")
    )


def read_events(path: str | Path) -> CleaningResult:
    """Clean UTF-8 CSV with one record per physical line.

    Quoted commas are supported; embedded newlines are not.
    Bad records are quarantined; invalid headers fail the whole run.
    """
    valid = []
    quarantine = []
    stats = dict(rows_in=0, rows_out=0, duplicates_removed=0, rows_quarantined=0,
                 test_rows_dropped=0, invalid_revenue=0)
    with Path(path).open("rb") as source:
        header_line = source.readline().decode("utf-8-sig")
        if not header_line:
            raise ValueError("CSV file is empty")
        header = next(csv.reader([header_line], strict=True))
        if len(header) != len(SCHEMA) or set(header) != set(SCHEMA):
            raise ValueError(f"CSV header must contain exactly: {', '.join(SCHEMA)}")
        for line_number, raw in enumerate(source, start=2):
            stats["rows_in"] += 1
            try:
                values = next(csv.reader([raw.decode("utf-8")], strict=True))
                if len(values) != len(header):
                    raise ValueError(f"expected {len(header)} fields, got {len(values)}")
                row = {key: value.strip() for key, value in zip(header, values)}
                for key in ("event_id", "user_id", "app_id", "event_name"):
                    if not row[key]:
                        raise ValueError(f"empty required field: {key}")
                row["event_time"] = parse_timestamp(row["event_time"])
                row["ingested_at"] = parse_timestamp(row["ingested_at"])
                flag = row["is_test"].lower()
                if flag not in ("true", "false", "1", "0"):
                    raise ValueError("is_test must be true/false or 1/0")
                row["is_test"] = flag in ("true", "1")
                row["country"] = normalise_country(row["country"])
                row["revenue_usd"], bad_revenue = parse_revenue(row["revenue_usd"])
                stats["invalid_revenue"] += int(bad_revenue)
                valid.append(row)
            except (ValueError, OverflowError, csv.Error) as exc:
                quarantine.append({
                    "line_number": line_number,
                    "reason": str(exc),
                    "raw_text": raw.decode("utf-8", errors="replace").rstrip("\r\n"),
                })
    deliveries = pl.DataFrame(valid, schema=SCHEMA) if valid else empty_events()
    events = latest_versions(deliveries)
    test_count = deliveries.filter(pl.col("is_test")).height
    stats.update(rows_out=events.height,
                 duplicates_removed=deliveries.height - test_count - events.height,
                 rows_quarantined=len(quarantine),
                 test_rows_dropped=test_count)
    return CleaningResult(events, quarantine, stats, deliveries)


if __name__ == "__main__":
    import argparse
    import json
    import sys

    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="Preview task 3.1 CSV cleaning")
    parser.add_argument("--input", type=Path, default=Path("data/events_raw.csv"))
    args = parser.parse_args()
    result = read_events(args.input)
    print(result.events)
    print(json.dumps(result.stats, indent=2))
    for rejected in result.quarantine:
        print(json.dumps(rejected, ensure_ascii=False))
