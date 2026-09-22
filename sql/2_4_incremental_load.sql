-- Dialect: DuckDB >= 1.4
CREATE TABLE IF NOT EXISTS clean_events_loaded (
    event_id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    app_id VARCHAR(255) NOT NULL,
    event_name VARCHAR(255) NOT NULL,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE NOT NULL,
    country VARCHAR(2) NOT NULL,
    media_source VARCHAR(255) NOT NULL,
    campaign VARCHAR(255) NOT NULL,
    revenue_usd DOUBLE PRECISION NOT NULL,
    is_test BOOLEAN NOT NULL CHECK (is_test = FALSE)
);

-- Change the inclusive UTC boundary below for a replay.
MERGE INTO clean_events_loaded AS target
USING (
    WITH incoming AS (
        SELECT
            event_id, user_id, app_id, event_name, event_time,
            ingested_at, country, media_source, campaign, revenue_usd, is_test
        FROM events_staging
        WHERE is_test = FALSE
          AND ingested_at >= TIMESTAMP WITH TIME ZONE '2026-01-01 00:00:00+00:00'
    )
    SELECT
        event_id, user_id, app_id, event_name, event_time,
        ingested_at, country, media_source, campaign, revenue_usd, is_test
    FROM (
        SELECT
            candidates.*,
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
        FROM (
            SELECT
                event_id, user_id, app_id, event_name, event_time,
                ingested_at, country, media_source, campaign, revenue_usd, is_test
            FROM incoming

            UNION ALL

            SELECT
                existing.event_id, existing.user_id, existing.app_id,
                existing.event_name, existing.event_time, existing.ingested_at,
                existing.country, existing.media_source, existing.campaign,
                existing.revenue_usd, existing.is_test
            FROM clean_events_loaded AS existing
            WHERE EXISTS (
                SELECT 1
                FROM incoming AS batch
                WHERE batch.event_id = existing.event_id
            )
        ) AS candidates
    ) AS ranked_events
    WHERE rn = 1
) AS source
ON target.event_id = source.event_id
WHEN MATCHED AND (
       target.user_id      IS DISTINCT FROM source.user_id
    OR target.app_id       IS DISTINCT FROM source.app_id
    OR target.event_name   IS DISTINCT FROM source.event_name
    OR target.event_time   IS DISTINCT FROM source.event_time
    OR target.ingested_at  IS DISTINCT FROM source.ingested_at
    OR target.country      IS DISTINCT FROM source.country
    OR target.media_source IS DISTINCT FROM source.media_source
    OR target.campaign     IS DISTINCT FROM source.campaign
    OR target.revenue_usd  IS DISTINCT FROM source.revenue_usd
    OR target.is_test      IS DISTINCT FROM source.is_test
) THEN
    UPDATE SET
        user_id = source.user_id,
        app_id = source.app_id,
        event_name = source.event_name,
        event_time = source.event_time,
        ingested_at = source.ingested_at,
        country = source.country,
        media_source = source.media_source,
        campaign = source.campaign,
        revenue_usd = source.revenue_usd,
        is_test = source.is_test
WHEN NOT MATCHED THEN
    INSERT (
        event_id, user_id, app_id, event_name, event_time,
        ingested_at, country, media_source, campaign, revenue_usd, is_test
    )
    VALUES (
        source.event_id, source.user_id, source.app_id, source.event_name,
        source.event_time, source.ingested_at, source.country,
        source.media_source, source.campaign, source.revenue_usd, source.is_test
    );
