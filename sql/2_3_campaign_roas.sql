-- Dialect: DuckDB >= 1.4
WITH revenue AS (
    SELECT
        CAST(event_time AS DATE) AS event_date,
        app_id,
        media_source,
        campaign,
        SUM(revenue_usd) AS revenue_usd
    FROM clean_events
    GROUP BY
        CAST(event_time AS DATE),
        app_id,
        media_source,
        campaign
),
costs AS (
    SELECT
        date AS event_date,
        app_id,
        media_source,
        campaign,
        SUM(cost_usd) AS cost_usd
    FROM campaign_costs
    GROUP BY
        date,
        app_id,
        media_source,
        campaign
)
SELECT
    COALESCE(r.event_date, c.event_date) AS event_date,
    COALESCE(r.app_id, c.app_id) AS app_id,
    COALESCE(r.media_source, c.media_source) AS media_source,
    COALESCE(r.campaign, c.campaign) AS campaign,
    COALESCE(r.revenue_usd, 0.0) AS revenue_usd,
    c.cost_usd,
    r.event_date IS NOT NULL AS has_revenue,
    c.event_date IS NOT NULL AS has_cost,
    COALESCE(r.revenue_usd, 0.0)
        / NULLIF(c.cost_usd, 0.0) AS roas
FROM revenue AS r
FULL OUTER JOIN costs AS c
    ON r.event_date = c.event_date
   AND r.app_id = c.app_id
   AND r.media_source = c.media_source
   AND r.campaign = c.campaign
ORDER BY
    event_date,
    app_id,
    media_source,
    campaign;
