"""
analytics.py
Build aggregation tables từ stg_logs dùng advanced SQL:
  - Window functions (RANK, LAG, SUM OVER, PERCENTILE_CONT)
  - CTEs
  - Conditional aggregation
"""
from db import get_conn


def build_hourly_traffic(cur):
    cur.execute("""
        INSERT INTO agg_hourly_traffic
            (request_date, request_hour, total_requests, unique_ips,
             error_count, error_rate, avg_response_size, bot_requests)
        SELECT
            request_date,
            request_hour,
            COUNT(*)                                             AS total_requests,
            COUNT(DISTINCT ip_address)                          AS unique_ips,
            COUNT(*) FILTER (WHERE status_code >= 400)          AS error_count,
            ROUND(
                COUNT(*) FILTER (WHERE status_code >= 400) * 100.0
                / NULLIF(COUNT(*), 0), 2
            )                                                   AS error_rate,
            ROUND(AVG(response_size), 2)                        AS avg_response_size,
            COUNT(*) FILTER (WHERE is_bot = TRUE)               AS bot_requests
        FROM stg_logs
        WHERE is_valid = TRUE
        GROUP BY request_date, request_hour
        ON CONFLICT (request_date, request_hour) DO UPDATE SET
            total_requests    = EXCLUDED.total_requests,
            unique_ips        = EXCLUDED.unique_ips,
            error_count       = EXCLUDED.error_count,
            error_rate        = EXCLUDED.error_rate,
            avg_response_size = EXCLUDED.avg_response_size,
            bot_requests      = EXCLUDED.bot_requests
    """)
    print(f"  [analytics] agg_hourly_traffic: {cur.rowcount} rows")


def build_endpoint_stats(cur):
    """Dùng window function RANK() để rank endpoints theo traffic."""
    cur.execute("""
        INSERT INTO agg_endpoint_stats
            (report_date, endpoint, total_hits, success_hits,
             error_hits, not_found_hits, avg_size, hit_rank)
        WITH base AS (
            SELECT
                request_date,
                endpoint,
                COUNT(*)                                            AS total_hits,
                COUNT(*) FILTER (WHERE status_code < 400)          AS success_hits,
                COUNT(*) FILTER (WHERE status_code >= 400)         AS error_hits,
                COUNT(*) FILTER (WHERE status_code = 404)          AS not_found_hits,
                ROUND(AVG(response_size), 2)                       AS avg_size
            FROM stg_logs
            WHERE is_valid = TRUE AND endpoint IS NOT NULL
            GROUP BY request_date, endpoint
        )
        SELECT
            request_date,
            endpoint,
            total_hits,
            success_hits,
            error_hits,
            not_found_hits,
            avg_size,
            RANK() OVER (
                PARTITION BY request_date
                ORDER BY total_hits DESC
            ) AS hit_rank
        FROM base
        ON CONFLICT (report_date, endpoint) DO UPDATE SET
            total_hits     = EXCLUDED.total_hits,
            success_hits   = EXCLUDED.success_hits,
            error_hits     = EXCLUDED.error_hits,
            not_found_hits = EXCLUDED.not_found_hits,
            avg_size       = EXCLUDED.avg_size,
            hit_rank       = EXCLUDED.hit_rank
    """)
    print(f"  [analytics] agg_endpoint_stats: {cur.rowcount} rows")


def build_ip_anomalies(cur):
    """
    Dùng PERCENTILE_CONT để tìm IPs có request count > p95
    hoặc error_rate > 50% — dấu hiệu của bot/attack.
    """
    cur.execute("""
        INSERT INTO agg_ip_anomalies
            (report_date, ip_address, total_requests, error_requests,
             error_rate, unique_endpoints, is_suspicious, anomaly_reason)
        WITH ip_stats AS (
            SELECT
                request_date,
                ip_address,
                COUNT(*)                                         AS total_requests,
                COUNT(*) FILTER (WHERE status_code >= 400)       AS error_requests,
                ROUND(
                    COUNT(*) FILTER (WHERE status_code >= 400) * 100.0
                    / NULLIF(COUNT(*), 0), 2
                )                                                AS error_rate,
                COUNT(DISTINCT endpoint)                         AS unique_endpoints
            FROM stg_logs
            WHERE is_valid = TRUE
            GROUP BY request_date, ip_address
        ),
        thresholds AS (
            SELECT
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_requests) AS p95,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_requests) AS p99
            FROM ip_stats
        )
        SELECT
            s.request_date,
            s.ip_address,
            s.total_requests,
            s.error_requests,
            s.error_rate,
            s.unique_endpoints,
            CASE
                WHEN s.total_requests > t.p99 THEN TRUE
                WHEN s.error_rate > 50         THEN TRUE
                ELSE FALSE
            END AS is_suspicious,
            CASE
                WHEN s.total_requests > t.p99 THEN 'Volume > p99 threshold'
                WHEN s.error_rate > 50         THEN 'Error rate > 50%'
                ELSE NULL
            END AS anomaly_reason
        FROM ip_stats s
        CROSS JOIN thresholds t
        WHERE s.total_requests > t.p95 OR s.error_rate > 50
        ON CONFLICT (report_date, ip_address) DO UPDATE SET
            total_requests   = EXCLUDED.total_requests,
            error_requests   = EXCLUDED.error_requests,
            error_rate       = EXCLUDED.error_rate,
            unique_endpoints = EXCLUDED.unique_endpoints,
            is_suspicious    = EXCLUDED.is_suspicious,
            anomaly_reason   = EXCLUDED.anomaly_reason
    """)
    print(f"  [analytics] agg_ip_anomalies: {cur.rowcount} rows")


def build_data_quality_report(cur):
    """Data quality check — tính quality_score 0-100."""
    cur.execute("""
        INSERT INTO data_quality_report
            (report_date, total_raw, total_valid, total_invalid, invalid_rate,
             null_ip_count, invalid_method, invalid_status, duplicate_count, quality_score)
        WITH base AS (
            SELECT
                request_date,
                COUNT(*)                                              AS total_raw,
                COUNT(*) FILTER (WHERE is_valid = TRUE)               AS total_valid,
                COUNT(*) FILTER (WHERE is_valid = FALSE)              AS total_invalid,
                COUNT(*) FILTER (WHERE ip_address IS NULL)            AS null_ip,
                COUNT(*) FILTER (
                    WHERE invalid_reason LIKE 'Invalid method%'
                )                                                     AS invalid_method,
                COUNT(*) FILTER (
                    WHERE invalid_reason LIKE 'Invalid status%'
                )                                                     AS invalid_status
            FROM stg_logs
            GROUP BY request_date
        ),
        dupes AS (
            SELECT
                request_date,
                COUNT(*) - COUNT(DISTINCT (ip_address, requested_at, path)) AS dup_count
            FROM stg_logs
            GROUP BY request_date
        )
        SELECT
            b.request_date,
            b.total_raw,
            b.total_valid,
            b.total_invalid,
            ROUND(b.total_invalid * 100.0 / NULLIF(b.total_raw, 0), 2) AS invalid_rate,
            b.null_ip,
            b.invalid_method,
            b.invalid_status,
            d.dup_count,
            -- quality score: starts 100, -1 per 1% invalid rate, -0.5 per dup pct
            GREATEST(0, ROUND(
                100
                - (b.total_invalid * 100.0 / NULLIF(b.total_raw, 0))
                - (d.dup_count * 50.0 / NULLIF(b.total_raw, 0)),
            2))                                                         AS quality_score
        FROM base b
        JOIN dupes d ON d.request_date = b.request_date
        ON CONFLICT (report_date) DO UPDATE SET
            total_raw       = EXCLUDED.total_raw,
            total_valid     = EXCLUDED.total_valid,
            total_invalid   = EXCLUDED.total_invalid,
            invalid_rate    = EXCLUDED.invalid_rate,
            null_ip_count   = EXCLUDED.null_ip_count,
            invalid_method  = EXCLUDED.invalid_method,
            invalid_status  = EXCLUDED.invalid_status,
            duplicate_count = EXCLUDED.duplicate_count,
            quality_score   = EXCLUDED.quality_score
    """)
    print(f"  [analytics] data_quality_report: {cur.rowcount} rows")


def run_analytics():
    print("[analytics] Running...")
    conn = get_conn()
    cur = conn.cursor()
    build_hourly_traffic(cur)
    build_endpoint_stats(cur)
    build_ip_anomalies(cur)
    build_data_quality_report(cur)
    conn.commit()
    cur.close()
    conn.close()
    print("[analytics] Done")


if __name__ == "__main__":
    run_analytics()
