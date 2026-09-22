# Assumptions and limitations

- Equivalent synthetic CSVs were generated for development; input CSVs are not included in this repository.
- SQL runs in DuckDB 1.4 or later, with the session timezone set to UTC. `events_staging` contains normalized deliveries, including test records and duplicate deliveries. Staging must be prepared before running the SQL tasks.
- Event IDs are globally unique across apps. The chosen interpretation is the latest non-test delivery: test records are excluded before ranking, and later test flags do not delete older non-test events.
- Equal ingestion timestamps use the payload ordering in task 2.1. This is deterministic, not proof of the vendor's intended correction order.
- CSV is UTF-8 with one record per physical line. Embedded newlines are unsupported. Invalid rows are quarantined; a missing file or invalid header fails the run.
- Required event ID, user ID, app ID, and event name must be nonblank. Empty campaign and media-source strings are retained rather than invented.
- Timestamps without an offset are assumed UTC. Reporting dates are UTC dates.
- Country normalization validates two ASCII letters, not ISO membership. `UK` and `ZZ` remain unchanged; invalid formats become `XX`.
- Empty, invalid, or non-finite revenue becomes zero and is counted. Decimal commas are supported; thousands separators are not. Negative revenue is allowed because it may represent refunds.
- Invalid-revenue counters cover structurally valid input deliveries before test filtering and deduplication. CLI cleaning counters cover the whole CSV; `selected_events` is after the ingestion cutoff and `snapshot_events` includes saved history.
- Task 2.2 intentionally leaves the calendar-gap fix as an explanation, as permitted by its Explain prompt. Its windows operate on available rows; it does not implement a complete calendar, enforce launch-date filtering, or prove complete lifetime history.
- Task 2.3 assumes matching, non-null join keys across normalized sources; missing cost is not treated as zero spend. No stable campaign identifier or rename mapping is provided.
- Incremental cutoffs are inclusive and manually supplied. They are not automatically advanced checkpoints. No fixed finite replay window is guaranteed safe without source visibility/delay guarantees.
- Runs are sequential, with one writer. The Python pipeline reads the whole input and rewrites a complete snapshot; it has no writer lock, automatic snapshot cleanup, or power-loss durability guarantee.
- Readers must resolve `CURRENT` once and read only that snapshot. Combining every snapshot would duplicate history. Local publication guarantees do not extend automatically to cloud-sync or network-filesystem behavior.
- SQL reports use `clean_events`; task 2.4 writes a separate `clean_events_loaded` demonstration. No dashboard refresh or scheduler integration is implemented.
- Task 4 is a proposed design, not an implemented warehouse. Production scaling, operational notifications, and automatic reference-data repair are outside this trainee implementation.
