"""Task 3.2: incremental loading into local Parquet snapshots; one writer."""

import argparse
import json
from datetime import date, datetime, time, timezone
from pathlib import Path

import polars as pl

from .cleaning import latest_versions, read_events
from .storage import read_events as read_saved_events, save_events


def run(input_dir: Path, output_dir: Path, since: date) -> dict:
    boundary = datetime.combine(since, time.min, tzinfo=timezone.utc)
    result = read_events(input_dir / "events_raw.csv")
    incoming = result.events.filter(pl.col("ingested_at") >= boundary)
    saved = read_saved_events(output_dir)
    events = latest_versions(pl.concat([saved, incoming]))
    stats = {
        **result.stats,
        "selected_events": incoming.height,
        "snapshot_events": events.height,
        "since": since.isoformat(),
    }
    save_events(output_dir, events, result.quarantine, stats)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean CSV and save partitioned Parquet")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--since", type=date.fromisoformat, required=True,
                        help="Inclusive ingested_at date in UTC, YYYY-MM-DD")
    args = parser.parse_args()
    print(json.dumps(run(args.input, args.output, args.since)))


if __name__ == "__main__":
    main()
