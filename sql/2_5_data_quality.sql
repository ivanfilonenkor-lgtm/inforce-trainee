-- Dialect: DuckDB >= 1.4
-- Run after 2_1_deduplication.sql. Each query returns zero rows on success.

-- 1. FAIL: event IDs must be present and unique.
SELECT event_id, COUNT(*) AS row_count
FROM clean_events
GROUP BY event_id
HAVING event_id IS NULL
    OR TRIM(event_id) = ''
    OR COUNT(*) > 1;

-- 2. FAIL: test traffic and missing required values must not reach reports.
SELECT event_id, app_id, event_time, ingested_at, revenue_usd, is_test
FROM clean_events
WHERE is_test IS DISTINCT FROM FALSE
   OR app_id IS NULL
   OR TRIM(app_id) = ''
   OR event_time IS NULL
   OR ingested_at IS NULL
   OR revenue_usd IS NULL;

-- 3. WARN: investigate unknown apps and events before the app launch date.
SELECT
    e.event_id,
    e.app_id,
    CAST(e.event_time AS DATE) AS event_date,
    a.launched_on
FROM clean_events AS e
LEFT JOIN apps AS a
    ON e.app_id = a.app_id
WHERE a.app_id IS NULL
   OR CAST(e.event_time AS DATE) < a.launched_on;
