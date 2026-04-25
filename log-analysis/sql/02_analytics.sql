-- ============================================================
-- ADVANCED SQL QUERIES — Log Analysis
-- Flex: CTEs, Window Functions, Percentiles, Lateral joins
-- ============================================================


-- ── 1. Traffic trend với 3-hour rolling average ──────────────
-- Window function: AVG OVER sliding window
WITH hourly AS (
    SELECT
        log_date,
        log_hour,
        COUNT(*) AS requests
    FROM stg_logs
    WHERE is_valid = TRUE
    GROUP BY log_date, log_hour
)
SELECT
    log_date,
    log_hour,
    requests,
    ROUND(AVG(requests) OVER (
        ORDER BY log_date, log_hour
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2)                                              AS rolling_avg_3h,
    requests - LAG(requests) OVER (
        ORDER BY log_date, log_hour
    )                                                  AS delta_from_prev_hour
FROM hourly
ORDER BY log_date, log_hour;


-- ── 2. Response time percentiles per endpoint ─────────────────
-- PERCENTILE_CONT: p50, p95, p99 để detect slow endpoints
SELECT
    endpoint_group,
    method,
    COUNT(*)                                           AS total_requests,
    ROUND(AVG(response_ms), 2)                         AS avg_ms,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY response_ms) AS p50_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_ms) AS p95_ms,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY response_ms) AS p99_ms,
    MAX(response_ms)                                   AS max_ms
FROM stg_logs
WHERE is_valid = TRUE
  AND log_date = CURRENT_DATE
GROUP BY endpoint_group, method
ORDER BY p95_ms DESC;


-- ── 3. IP abuse detection ─────────────────────────────────────
-- CTE + HAVING: tìm IP gửi > 500 req/hour hoặc error rate cao
WITH ip_stats AS (
    SELECT
        ip_address,
        log_date,
        log_hour,
        COUNT(*)                                        AS total_req,
        COUNT(*) FILTER (WHERE status_code >= 400)      AS error_req,
        COUNT(DISTINCT endpoint)                        AS unique_endpoints,
        ROUND(
            COUNT(*) FILTER (WHERE status_code >= 400) * 100.0
            / NULLIF(COUNT(*), 0), 2
        )                                               AS error_rate
    FROM stg_logs
    WHERE is_valid = TRUE
    GROUP BY ip_address, log_date, log_hour
)
SELECT
    ip_address,
    log_date,
    log_hour,
    total_req,
    error_req,
    error_rate,
    unique_endpoints,
    CASE
        WHEN total_req > 1000 AND error_rate > 50 THEN 'HIGH'
        WHEN total_req > 500  OR  error_rate > 30 THEN 'MEDIUM'
        ELSE 'LOW'
    END                                                 AS threat_level
FROM ip_stats
WHERE total_req > 200 OR error_rate > 20
ORDER BY total_req DESC, error_rate DESC;


-- ── 4. Error surge detection (so sánh với baseline) ──────────
-- CTE chained: tính baseline rồi compare
WITH daily_error_rate AS (
    SELECT
        log_date,
        log_hour,
        COUNT(*)                                        AS total,
        COUNT(*) FILTER (WHERE status_code >= 500)      AS server_errors,
        ROUND(
            COUNT(*) FILTER (WHERE status_code >= 500) * 100.0
            / NULLIF(COUNT(*), 0), 2
        )                                               AS error_rate_pct
    FROM stg_logs
    WHERE is_valid = TRUE
    GROUP BY log_date, log_hour
),
baseline AS (
    SELECT
        log_hour,
        ROUND(AVG(error_rate_pct), 2)                   AS avg_error_rate,
        ROUND(STDDEV(error_rate_pct), 2)                AS stddev_error_rate
    FROM daily_error_rate
    WHERE log_date < CURRENT_DATE   -- historical baseline
    GROUP BY log_hour
)
SELECT
    d.log_date,
    d.log_hour,
    d.error_rate_pct                                    AS current_rate,
    b.avg_error_rate                                    AS baseline_rate,
    b.stddev_error_rate,
    ROUND(d.error_rate_pct - b.avg_error_rate, 2)       AS deviation,
    CASE
        WHEN d.error_rate_pct > b.avg_error_rate + 3 * b.stddev_error_rate THEN 'ANOMALY'
        WHEN d.error_rate_pct > b.avg_error_rate + 2 * b.stddev_error_rate THEN 'WARNING'
        ELSE 'NORMAL'
    END                                                 AS status
FROM daily_error_rate d
JOIN baseline b ON b.log_hour = d.log_hour
WHERE d.log_date = CURRENT_DATE
ORDER BY d.log_hour;


-- ── 5. Top slowest endpoints with ranking ─────────────────────
-- RANK + DENSE_RANK để xếp hạng trong từng category
WITH endpoint_perf AS (
    SELECT
        endpoint_group,
        method,
        COUNT(*)                                        AS hits,
        ROUND(AVG(response_ms), 2)                      AS avg_ms,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_ms) AS p95_ms
    FROM stg_logs
    WHERE is_valid = TRUE
      AND log_date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY endpoint_group, method
    HAVING COUNT(*) > 50  -- loại endpoints ít traffic
)
SELECT
    endpoint_group,
    method,
    hits,
    avg_ms,
    p95_ms,
    RANK() OVER (ORDER BY p95_ms DESC)                  AS rank_by_p95,
    RANK() OVER (ORDER BY avg_ms DESC)                  AS rank_by_avg,
    NTILE(4) OVER (ORDER BY p95_ms DESC)                AS quartile  -- 1=slowest 25%
FROM endpoint_perf
ORDER BY p95_ms DESC;


-- ── 6. Funnel analysis: request flow ─────────────────────────
-- Tỷ lệ chuyển đổi từng bước (page_view → api → purchase)
WITH step_counts AS (
    SELECT
        log_date,
        COUNT(*) FILTER (WHERE endpoint_group = '/page')     AS page_views,
        COUNT(*) FILTER (WHERE endpoint_group = '/api')      AS api_calls,
        COUNT(*) FILTER (WHERE endpoint_group = '/checkout') AS checkouts,
        COUNT(*) FILTER (WHERE endpoint_group = '/purchase') AS purchases
    FROM stg_logs
    WHERE is_valid = TRUE
      AND status_code < 400
    GROUP BY log_date
)
SELECT
    log_date,
    page_views,
    api_calls,
    checkouts,
    purchases,
    ROUND(api_calls * 100.0 / NULLIF(page_views, 0), 2)      AS page_to_api_pct,
    ROUND(checkouts * 100.0 / NULLIF(api_calls, 0), 2)       AS api_to_checkout_pct,
    ROUND(purchases * 100.0 / NULLIF(checkouts, 0), 2)       AS checkout_to_purchase_pct
FROM step_counts
ORDER BY log_date DESC;
