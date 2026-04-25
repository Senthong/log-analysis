-- ============================================================
-- ADVANCED SQL QUERIES — Window Functions + CTEs
-- Run these manually in psql to explore the data
-- ============================================================

-- ── 1. Top 10 endpoints by traffic (với running total) ──────
WITH endpoint_counts AS (
    SELECT
        endpoint,
        COUNT(*)                              AS total_hits,
        COUNT(*) FILTER (WHERE status_code >= 400) AS error_hits,
        ROUND(AVG(response_size), 2)          AS avg_size
    FROM stg_logs
    WHERE is_valid = TRUE
    GROUP BY endpoint
),
ranked AS (
    SELECT *,
        RANK() OVER (ORDER BY total_hits DESC)   AS hit_rank,
        SUM(total_hits) OVER ()                  AS grand_total,
        ROUND(total_hits * 100.0 / SUM(total_hits) OVER (), 2) AS traffic_pct,
        SUM(total_hits) OVER (ORDER BY total_hits DESC) AS running_total
    FROM endpoint_counts
)
SELECT * FROM ranked ORDER BY hit_rank LIMIT 10;


-- ── 2. Hourly traffic pattern — detect peak hours ───────────
SELECT
    request_hour,
    COUNT(*)                                        AS total_requests,
    COUNT(DISTINCT ip_address)                      AS unique_visitors,
    COUNT(*) FILTER (WHERE status_code >= 400)      AS errors,
    ROUND(AVG(response_size), 0)                    AS avg_size,
    -- so sánh với giờ trước (LAG)
    LAG(COUNT(*)) OVER (ORDER BY request_hour)      AS prev_hour_requests,
    ROUND(
        (COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY request_hour)) * 100.0
        / NULLIF(LAG(COUNT(*)) OVER (ORDER BY request_hour), 0),
    2)                                              AS pct_change_vs_prev_hour
FROM stg_logs
WHERE is_valid = TRUE
GROUP BY request_hour
ORDER BY request_hour;


-- ── 3. IP anomaly detection — ai gửi quá nhiều request? ─────
WITH ip_stats AS (
    SELECT
        ip_address,
        request_date,
        COUNT(*)                                          AS total_req,
        COUNT(*) FILTER (WHERE status_code >= 400)        AS error_req,
        COUNT(DISTINCT endpoint)                          AS unique_endpoints,
        COUNT(*) FILTER (WHERE is_bot = TRUE)             AS bot_signals,
        ROUND(
            COUNT(*) FILTER (WHERE status_code >= 400) * 100.0
            / NULLIF(COUNT(*), 0),
        2)                                                AS error_rate
    FROM stg_logs
    WHERE is_valid = TRUE
    GROUP BY ip_address, request_date
),
percentiles AS (
    SELECT
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_req) AS p95_requests,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_req) AS p99_requests
    FROM ip_stats
)
SELECT
    s.*,
    p.p95_requests,
    CASE
        WHEN s.total_req > p.p99_requests THEN 'Extremely high volume (>p99)'
        WHEN s.error_rate > 50            THEN 'High error rate (>50%)'
        WHEN s.total_req > p.p95_requests
         AND s.error_rate > 30            THEN 'High volume + errors'
        ELSE NULL
    END                                               AS anomaly_reason
FROM ip_stats s
CROSS JOIN percentiles p
WHERE s.total_req > p.p95_requests OR s.error_rate > 50
ORDER BY s.total_req DESC
LIMIT 20;


-- ── 4. Status code trend theo ngày (pivot-like) ─────────────
SELECT
    request_date,
    COUNT(*) FILTER (WHERE status_class = '2xx')  AS s_2xx,
    COUNT(*) FILTER (WHERE status_class = '3xx')  AS s_3xx,
    COUNT(*) FILTER (WHERE status_class = '4xx')  AS s_4xx,
    COUNT(*) FILTER (WHERE status_class = '5xx')  AS s_5xx,
    COUNT(*)                                      AS total,
    ROUND(
        COUNT(*) FILTER (WHERE status_code >= 500) * 100.0 / COUNT(*),
    2)                                            AS server_error_rate,
    -- 7-day rolling avg error rate
    ROUND(AVG(
        COUNT(*) FILTER (WHERE status_code >= 400) * 100.0 / COUNT(*)
    ) OVER (
        ORDER BY request_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2)                                         AS rolling_7d_error_rate
FROM stg_logs
WHERE is_valid = TRUE
GROUP BY request_date
ORDER BY request_date;


-- ── 5. Funnel analysis: page_view → product → checkout ──────
WITH funnel AS (
    SELECT
        request_date,
        COUNT(*) FILTER (WHERE path = '/index.html' OR path = '/')   AS home_visits,
        COUNT(*) FILTER (WHERE path LIKE '/products%')                AS product_views,
        COUNT(*) FILTER (WHERE path = '/cart')                        AS cart_adds,
        COUNT(*) FILTER (WHERE path = '/checkout')                    AS checkouts
    FROM stg_logs
    WHERE is_valid = TRUE AND status_code = 200
    GROUP BY request_date
)
SELECT *,
    ROUND(product_views * 100.0 / NULLIF(home_visits, 0), 2)   AS home_to_product_pct,
    ROUND(cart_adds    * 100.0 / NULLIF(product_views, 0), 2)  AS product_to_cart_pct,
    ROUND(checkouts    * 100.0 / NULLIF(cart_adds, 0), 2)      AS cart_to_checkout_pct
FROM funnel
ORDER BY request_date;


-- ── 6. Bot vs Human traffic breakdown ───────────────────────
SELECT
    request_date,
    client_type,
    COUNT(*)                             AS total_requests,
    COUNT(DISTINCT ip_address)           AS unique_ips,
    ROUND(AVG(response_size), 0)         AS avg_response_size,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY request_date), 2) AS pct_of_day
FROM stg_logs
WHERE is_valid = TRUE
GROUP BY request_date, client_type
ORDER BY request_date, total_requests DESC;
