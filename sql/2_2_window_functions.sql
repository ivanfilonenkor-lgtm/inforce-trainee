-- Dialect: DuckDB >= 1.4
WITH daily_revenue AS (
    SELECT
        app_id,
        CAST(event_time AS DATE) AS event_date,
        SUM(revenue_usd) AS daily_revenue
    FROM clean_events
    GROUP BY
        app_id,
        CAST(event_time AS DATE)
),

daily_metrics AS (
    SELECT
        app_id,
        event_date,
        daily_revenue,

        SUM(daily_revenue) OVER (
            PARTITION BY app_id
            ORDER BY event_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_revenue,

        AVG(daily_revenue) OVER (
            PARTITION BY app_id
            ORDER BY event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS revenue_7d_avg,

        LAG(daily_revenue) OVER (
            PARTITION BY app_id
            ORDER BY event_date
        ) AS previous_day_revenue

    FROM daily_revenue
)

SELECT
    app_id,
    event_date,
    daily_revenue,
    running_revenue,
    revenue_7d_avg,

    CASE
        WHEN previous_day_revenue IS NULL
          OR previous_day_revenue = 0
        THEN NULL
        ELSE
            (daily_revenue - previous_day_revenue)
            / previous_day_revenue * 100.0
    END AS day_over_day_pct

FROM daily_metrics
ORDER BY
    app_id,
    event_date;