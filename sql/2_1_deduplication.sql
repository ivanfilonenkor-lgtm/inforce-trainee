-- Dialect: DuckDB >= 1.4
CREATE OR REPLACE VIEW clean_events AS
WITH ranked_events AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY
                ingested_at DESC,
                event_time DESC,
                app_id DESC,
                user_id DESC,
                event_name DESC,
                country DESC,
                media_source DESC,
                campaign DESC,
                revenue_usd DESC
        ) AS rn
    FROM events_staging
    WHERE is_test = FALSE
)

SELECT
    event_id,
    user_id,
    app_id,
    event_name,
    event_time,
    ingested_at,
    country,
    media_source,
    campaign,
    revenue_usd,
    is_test
FROM ranked_events
WHERE rn = 1;
