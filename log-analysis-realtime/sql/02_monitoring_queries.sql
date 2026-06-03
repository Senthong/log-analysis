-- ============================================================
-- monitoring_queries.sql
-- Chạy các query này để monitor pipeline real-time
-- ============================================================

-- 1. Tổng số rows đã ingest + row mới nhất
SELECT
    COUNT(*)                          AS total_rows,
    MAX(ingested_at)                  AS last_ingested_at,
    NOW() - MAX(ingested_at)          AS lag,
    COUNT(*) FILTER (WHERE is_valid)  AS valid_rows,
    COUNT(*) FILTER (WHERE NOT is_valid) AS invalid_rows
FROM stg_logs_stream;

-- 2. Throughput theo phút (số msg/phút)
SELECT
    DATE_TRUNC('minute', ingested_at) AS minute,
    COUNT(*)                           AS messages,
    COUNT(DISTINCT ip_address)         AS unique_ips
FROM stg_logs_stream
WHERE ingested_at >= NOW() - INTERVAL '10 minutes'
GROUP BY 1
ORDER BY 1 DESC;

-- 3. Status code distribution (real-time)
SELECT
    status_class,
    COUNT(*)                              AS total,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM stg_logs_stream
WHERE ingested_at >= NOW() - INTERVAL '5 minutes'
GROUP BY status_class
ORDER BY total DESC;

-- 4. Top endpoints (last 5 minutes)
SELECT
    endpoint,
    COUNT(*)   AS hits,
    COUNT(*) FILTER (WHERE status_code >= 400) AS errors,
    ROUND(AVG(response_size), 0)               AS avg_size_bytes
FROM stg_logs_stream
WHERE ingested_at >= NOW() - INTERVAL '5 minutes'
GROUP BY endpoint
ORDER BY hits DESC
LIMIT 10;

-- 5. Suspicious IPs (high error rate, real-time)
SELECT
    ip_address,
    COUNT(*)                                          AS total_requests,
    COUNT(*) FILTER (WHERE status_code >= 400)        AS error_requests,
    ROUND(
        COUNT(*) FILTER (WHERE status_code >= 400) * 100.0 / COUNT(*),
    2)                                                AS error_rate_pct,
    COUNT(DISTINCT endpoint)                          AS unique_endpoints
FROM stg_logs_stream
WHERE ingested_at >= NOW() - INTERVAL '5 minutes'
GROUP BY ip_address
HAVING COUNT(*) > 20
   AND COUNT(*) FILTER (WHERE status_code >= 400) * 1.0 / COUNT(*) > 0.5
ORDER BY error_rate_pct DESC;

-- 6. Bot traffic % (real-time)
SELECT
    client_type,
    COUNT(*)                                              AS total,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)  AS pct
FROM stg_logs_stream
WHERE ingested_at >= NOW() - INTERVAL '5 minutes'
GROUP BY client_type
ORDER BY total DESC;
