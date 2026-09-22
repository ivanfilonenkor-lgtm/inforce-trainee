"""Save complete snapshots; CURRENT identifies the published result."""

import json
import os
from pathlib import Path
from uuid import uuid4

import polars as pl

from .cleaning import empty_events


def read_events(output: Path) -> pl.DataFrame:
    pointer = output / "CURRENT"
    if not pointer.exists():
        return empty_events()
    snapshot_id = pointer.read_text(encoding="utf-8").strip()
    if len(snapshot_id) != 32 or any(c not in "0123456789abcdef" for c in snapshot_id):
        raise ValueError("Invalid CURRENT snapshot ID")
    return pl.read_parquet(output / "snapshots" / snapshot_id / "events.parquet")


def save_events(output: Path, events: pl.DataFrame, quarantine: list[dict], stats: dict) -> Path:
    snapshot_id = uuid4().hex
    snapshot = output / "snapshots" / snapshot_id
    snapshot.mkdir(parents=True)
    # A complete table also represents an empty result without special readers.
    events.write_parquet(snapshot / "events.parquet")
    dated = events.with_columns(pl.col("event_time").dt.date().alias("event_date"))
    for key, partition in dated.partition_by("event_date", as_dict=True).items():
        directory = snapshot / "events" / f"event_date={key[0].isoformat()}"
        directory.mkdir(parents=True)
        partition.drop("event_date").write_parquet(directory / "part-00000.parquet")
    with (snapshot / "quarantine.jsonl").open("w", encoding="utf-8") as handle:
        for row in quarantine:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    (snapshot / "summary.json").write_text(
        json.dumps(stats, indent=2) + "\n", encoding="utf-8"
    )
    temporary_pointer = output / f".CURRENT-{snapshot_id}"
    temporary_pointer.write_text(snapshot_id + "\n", encoding="utf-8")
    os.replace(temporary_pointer, output / "CURRENT")
    return snapshot
