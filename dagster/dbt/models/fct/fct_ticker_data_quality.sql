-- models/fct/fct_ticker_data_quality.sql

WITH base AS (
    SELECT
        ticker,
        date::date,
        close
    FROM {{ source('market_data', 'raw_price') }}
    WHERE close IS NOT NULL
),

deduped AS (
    SELECT DISTINCT ticker, date, close
    FROM base
),

per_ticker AS (
    SELECT
        ticker,
        MIN(date) AS first_date,
        MAX(date) AS last_date,
        COUNT(*) AS num_data_points,
        COUNT(*) FILTER (WHERE close <= 1e-5) AS num_zero_close,
        STDDEV(close) AS std_close,
        COUNT(*) - COUNT(DISTINCT date) AS num_duplicate_dates
    FROM base
    GROUP BY ticker
),

calendar_gaps AS (
    SELECT
        ticker,
        date,
        LEAD(date) OVER (PARTITION BY ticker ORDER BY date) AS next_date
    FROM deduped
),

gap_stats AS (
    SELECT
        ticker,
        MAX((next_date - date) - 1) AS largest_gap_days,
        COUNT(*) FILTER (
            WHERE (next_date - date - 1) > 3
        ) AS num_gaps_gt_3_days,
        COUNT(*) FILTER (
            WHERE (next_date - date - 1) > 5
        ) AS num_gaps_gt_5_days
    FROM calendar_gaps
    WHERE next_date IS NOT NULL
    GROUP BY ticker
),

weekday_coverage AS (
    SELECT
        ticker,
        COUNT(DISTINCT date) AS num_unique_days,
        COUNT(*) FILTER (WHERE EXTRACT(DOW FROM date) BETWEEN 1 AND 5) AS num_weekdays
    FROM deduped
    GROUP BY ticker
),

joined AS (
    SELECT
        p.ticker,
        p.first_date,
        p.last_date,
        (p.last_date - p.first_date) + 1 AS data_duration_days,
        p.num_data_points,
        ROUND(p.num_data_points::numeric / NULLIF((p.last_date - p.first_date) + 1, 0), 3) AS completeness_ratio,
        g.largest_gap_days,
        g.num_gaps_gt_3_days,
        g.num_gaps_gt_5_days,
        ROUND(p.std_close::numeric, 5) AS std_close,
        p.num_zero_close,
        ROUND(w.num_unique_days::numeric / NULLIF((p.last_date - p.first_date) + 1, 0), 3) AS weekday_coverage,
        (p.last_date >= CURRENT_DATE - INTERVAL '1 day') AS has_recent_data,
        p.num_duplicate_dates
    FROM per_ticker p
    LEFT JOIN gap_stats g ON p.ticker = g.ticker
    LEFT JOIN weekday_coverage w ON p.ticker = w.ticker
)

SELECT * FROM joined
