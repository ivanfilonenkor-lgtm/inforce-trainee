# Written answers

The input CSVs were not supplied in the conversation, so equivalent synthetic data was generated for development. SQL was exercised in DuckDB, and Python uses the standard library and Polars. The SQL examples read typed, normalized `events_staging`; the Python cleaner reads the raw CSV. Section 4 describes proposed modelling choices, not additional implemented tables.

## 2.1 — Deduplication and equal ingestion timestamps

The query keeps one row per `event_id` using `ROW_NUMBER()`. It orders by `ingested_at DESC`, then `event_time`, `app_id`, `user_id`, `event_name`, `country`, `media_source`, `campaign`, and `revenue_usd`, all descending. Therefore, equal ingestion timestamps are resolved by a repeatable comparison of the remaining payload fields rather than an arbitrary row order. If all compared values are identical, either physical row produces the same output. This fallback is deterministic, but it cannot identify the vendor's intended correction when timestamps tie; a vendor revision number would be preferable if available.

## 2.2 — Missing days and moving averages

The current query aggregates the available events by app and UTC event date. Its running total covers the available history, its moving average uses the current row plus six preceding rows, and `LAG` returns the preceding available row. If dates are missing, seven rows are not necessarily seven calendar days, and the percentage change is not necessarily against yesterday. Missing days therefore matter: the current output does not provide a complete calendar-day series.

To fix this, I would generate an app/date calendar from each app's launch date through an explicit reporting end date, left join the daily revenue, and fill missing revenue with zero before applying the windows. This includes apps without events and makes the seven-row window represent seven consecutive days. During the first six days after launch, I would average only the days since launch rather than inventing pre-launch days. The current code does not implement this calendar or explicitly filter pre-launch events, and a true lifetime total also requires complete source history. The percentage change returns `NULL` when the preceding revenue is absent or zero, because the percentage is then undefined.

## 2.3 — Joining costs and handling zero spend

I aggregate revenue and costs separately to the same grain: UTC date, app, media source, and campaign. I then use a `FULL OUTER JOIN` on all four keys, which preserves both spend-only and revenue-only groups without multiplying daily costs across individual events.

A campaign with costs but no revenue represents spend with no recorded revenue for that day; it may indicate weak performance, delayed revenue, or incomplete attribution. Its displayed revenue is zero and, for positive costs, its ROAS is zero. A campaign with revenue but no cost record may be organic traffic or a missing/unmatched cost feed, so missing cost remains `NULL` rather than being described as free acquisition. The `has_revenue` and `has_cost` flags distinguish missing source groups from recorded zero values. `NULLIF(cost_usd, 0)` makes ROAS `NULL` for zero costs; missing costs also yield `NULL`, avoiding division by zero and misleading ratios.

## 2.4 — Incremental selection, corrections, and replay

I filter on `ingested_at`, not `event_time`, because an event that happened days ago can arrive or be corrected today. The inclusive lower bound is explicit in the SQL and must be chosen by the caller; there is no automatic checkpoint. Incoming non-test deliveries are combined with saved rows for the same event IDs, and the same version ordering as in 2.1 selects the winner. `MERGE` inserts missing IDs and updates existing IDs only when values differ, so replaying old deliveries cannot replace a newer stored version or add duplicate events.

There is no universally safe fixed number of event-date days to reload without a guaranteed maximum delay. If ingestion timestamps reliably describe when records become visible and the source guarantees completeness through a saved boundary, an inclusive ingestion boundary can pick up late event dates without rescanning their entire event-time history. If older ingestion timestamps can become visible later, a reliable source cursor or an overlap justified by that visibility delay is needed; without such a guarantee, a finite overlap cannot guarantee completeness. A wider replay increases scanning, ranking, and potential writes, while an unjustifiably narrow one risks missing data. The `EXISTS` restriction reduces the saved candidates being ranked, but does not guarantee that the database avoids scanning the target. The first load must cover the required history, and the example assumes sequential runs against valid staging data.

## 2.5 — Data-quality checks

Each query returns violating rows or IDs, with zero rows meaning no violation of that particular rule.

1. **Missing, blank, or duplicate event IDs — fail the pipeline.** Each event needs a valid, unique identifier so repeated deliveries do not inflate revenue and corrections can be applied to the right event.
2. **Test or unknown test flags, missing app IDs, timestamps, or revenue — fail the pipeline.** These records violate the reporting dataset's basic contract and should not be published.
3. **Unknown apps or events before the recorded launch date — warn.** I would investigate the event and reference data because the app catalogue may be incomplete or outdated; this should not automatically discard valid revenue.

The SQL only identifies problems; a runner must implement failure or notification behavior. These checks currently query `clean_events`, which feeds reports 2.2 and 2.3; if those reports switch to `clean_events_loaded`, the checks should target that published dataset too. They do not establish completeness, and an empty dataset passes them. The revenue null check does not detect non-finite numbers; the Python cleaner handles those separately.

## 3.1 — Cleaning decisions

The cleaner returns a typed event dataframe, quarantined records, counters, and normalized deliveries for the SQL demonstration. Invalid CSV structure, missing required identity fields, invalid timestamps, or unrecognized test flags quarantine the individual record with its line number, reason, and source text. Invalid or missing revenue, including non-finite values, becomes `0.0` and is counted rather than causing the event to be rejected. Decimal commas are supported, but thousands separators are not.

Timestamps are converted to UTC, with timezone-free values assumed to already be UTC. Country normalization currently checks format only: two ASCII letters are uppercased, and invalid formats become `XX`; it does not validate an ISO country list, so `UK` and `ZZ` remain unchanged. This is my interpretation of the two-letter requirement, not a claim that every retained code identifies a real country. Test filtering and version selection match 2.1.

The reader assumes UTF-8 and one CSV record per physical line; quoted commas work, but embedded newlines are unsupported. A bad header fails the file because the columns cannot safely be interpreted. Invalid-revenue counts cover structurally valid deliveries before test filtering and deduplication, while quarantined records are counted separately. The implementation holds the normalized deliveries in memory.

## 3.2 — Pipeline and crash behavior

The CLI cleans the CSV, applies the inclusive UTC `ingested_at` boundary, combines selected events with the previously published events, and resolves versions again. It writes event-date-partitioned Parquet, a complete event file for subsequent loads, quarantine JSONL, and a summary into a new snapshot directory. Readers must resolve `CURRENT` and read only that snapshot, not combine historical snapshot directories. Repeating a run creates a new physical snapshot but the same logical events; the full CSV is still read and the complete result is rewritten.

### Explain: interruption during output writing

The script writes all output into a new directory and switches `CURRENT` only after those writes finish. If it dies halfway through writing, readers keep the previous published snapshot, or have no published snapshot if this was the first run, while the incomplete directory remains unused. To strengthen this, I would add a single-writer lock, cleanup of abandoned snapshots, and explicit disk synchronization before publication. For production infrastructure, I would use transactional storage with documented commit and durability guarantees rather than assume local file replacement protects against power loss or synchronized-folder failures.

The current implementation assumes one writer and retains old snapshots. The CLI prints a one-line JSON summary.

## 3.3 Reading someone else's code

Problems ranked primarily by their impact on the reported numbers:

1. **Repeated deliveries and corrections are summed.** There is no selection of the latest version per event ID, so successful execution can still produce inflated revenue.
2. **Test traffic is included.** QA activity must not reach reporting.
3. **The date filter precedes version resolution.** A correction that moves an event to another date can leave an obsolete event in the old day's report.
4. **String prefixes are not UTC date handling.** Offsets can move an event to a different UTC day, malformed timestamps may pass, and missing values can break filtering.
5. **Revenue conversion is unsafe.** Decimal commas and invalid strings can fail conversion; missing/non-finite numbers are not replaced and counted according to 3.1.
6. **Malformed records have no quarantine path.** A bad record may stop processing without a separately inspectable rejection.
7. **Concatenated keys are ambiguous.** `(a-b, c)` and `(a, b-c)` both become `a-b-c`; missing or non-string values can also fail concatenation.
8. **Empty results can fail.** A dataframe constructed from an empty list has no `revenue` column for the final sort.
9. **Row-by-row aggregation adds overhead.** `iterrows()` creates Python-level row objects where a grouped aggregation is clearer and generally more efficient.
10. **Whole-file reading limits scale.** Chunking would need cross-chunk version handling, not just independent aggregation of each chunk.

The replacement is `daily_revenue(events, day)` in `python/pipeline/daily.py`. It accepts an already-clean dataframe and a `datetime.date`, filters the UTC event date, groups by the separate `app_id` and `media_source` columns, sums revenue, and sorts by revenue with stable key tie-breakers. I changed the signature to reuse the cleaning contract from 3.1 instead of duplicating parsing and deduplication inside aggregation. Callers must pass the full relevant clean dataset, such as `read_events(path).events` or the current published snapshot; the function does not clean arbitrary raw frames itself. A day with no events returns an empty dataframe with the expected columns.

## 4.1 Star schema

Name of the fact table would be `fact_event`, with one row per unique non-test `event_id`, containing its latest accepted version. Its measures would be `revenue_usd` and an event count of one, linked to app, date, country, media source, campaign, and event-type dimensions. For example, the app dimension would hold the app name, platform, store ID, and launch date. I would keep advertising costs in a separate daily fact table with one row per date, app, media source, and campaign, holding cost, impressions, and clicks.

## 4.2 A dimension that changes

I would keep the old campaign dimension row and create a new version with the new name and its effective date. Each version would have its own key and validity period, while a stable campaign ID would link the versions together. Facts would reference the version valid at event time, so even an old event arriving after the rename would retain the historical name. 

## 4.3 Idempotency

For a marketing manager: Uploading the same data twice should leave the report unchanged after the first upload. For example, retrying an upload containing a $10 purchase must not turn it into $20 of revenue.

For an engineer: An unguarded `INSERT … SELECT` appends the same daily groups again on every retry, while a uniqueness constraint alone may reject the retry without applying corrections. I would use a `MERGE` keyed by the full daily grain, or transactionally replace affected dates with complete recalculated results. Events must be deduplicated before aggregation, and a correction that moves an event to another date or group requires recalculating both its old and new groups.

## 4.4 Late data

The pipeline picks up the event through Thursday's `ingested_at`, provided the selected ingestion range includes it. It resolves duplicates and saves the event in Monday's partition because its `event_time` is Monday. Monday's revenue and dependent running totals or moving averages must then be refreshed. I would send the client a revised Monday report, stating that late vendor data changed the total and showing the amount of the change.

## 4.5 Partitioning

I would partition by the UTC date of `event_time` because the main reports ask for revenue on the day events happened. A Monday report can then read Monday's partition even if some events arrived on Thursday. Partitioning by `ingested_at` would help arrival-based processing, but that same Monday report might need several ingestion-date partitions. My choice makes “show everything ingested today” slower because those records can be spread across many event dates.

## 5 — Debugging Sunday's zero revenue

I would keep the app ID, reporting timezone, revenue definition, and snapshot/run identity consistent across these checks, stopping to investigate once a discrepancy is localized.

1. **Does a direct query against the dashboard's data source also return zero?** Check both row counts and sums to distinguish missing data, `NULL`, and an actual zero.
   - **Yes:** The symptom exists in the underlying data or reporting query; investigate upstream.
   - **No:** Check dashboard filters, caching, refresh status, timezone settings, and whether missing values are displayed as zero.

2. **Are the expected Sunday deliveries present in the raw source for this app?** Compare event/ingestion times, types, and volumes with the app's normal activity and vendor records where available; a few rows alone do not prove completeness.
   - **Yes:** Select representative event IDs and trace them through processing.
   - **No:** Check delayed or missing deliveries, missing API pages, access problems, and tracking failures, while confirming whether no activity is legitimate.

3. **Did those events survive cleaning with the expected revenue, date, app ID, and selected version?**
   - **Yes:** Cleaning appears correct for those records; inspect publication.
   - **No:** Inspect quarantine reasons, invalid-revenue counts, test flags, deduplication winners, and UTC conversion, distinguishing genuine corrections from cleaning errors.

4. **Are the cleaned events in the published output of the run that should cover Sunday?** In this implementation, inspect the snapshot referenced by `CURRENT` and its event-date partitions.
   - **Yes:** Loading succeeded for those events; inspect reporting aggregation.
   - **No:** Check the `since` boundary, actual processing range, written counts, output location, and publication step; a successful run may have processed no relevant records.

5. **Does an independent calculation from the published clean events match the reporting result?** Use the same date boundaries and business rules.
   - **Yes:** The calculation is consistent with that dataset; check whether zero is genuine, for example no purchases or offsetting refunds, and verify that the expectation refers to the same snapshot.
   - **No:** Inspect joins, filters, grouping keys, date boundaries, and refresh of the reporting table or materialized result.

Pipeline success does not establish data completeness, and the absence of deployments does not rule out changes in vendor data, configuration, access, or delivery timing.

## Running the Python code

Use Python 3.11 or later and install Polars with `python -m pip install "polars>=1.30,<2"`.
From the repository root, enter the `python` directory and run:

```shell
cd python
python -m pipeline.run --input ../data --output ../out --since 2026-01-01
```

The input directory must contain `events_raw.csv`; input CSVs are intentionally excluded from this submission.
