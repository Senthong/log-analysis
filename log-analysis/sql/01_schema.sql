-- ============================================================
-- LAYER 1: RAW (parsed log entries)
-- ============================================================
CREATE TABLE IF NOT EXISTS raw_logs (
    id            SERIAL PRIMARY KEY,
    ip_address    VARCHAR(45),
    method        VARCHAR(10),
    path          VARCHAR(500),
    protocol      VARCHAR(20),
    status_code   INT,
    response_size INT,
    referer       TEXT,
    user_agent    TEXT,
    requested_at  TIMESTAMP,
    ingested_at   TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- LAYER 2: STAGING (cleaned + enriched)
-- ============================================================
CREATE TABLE IF NOT EXISTS stg_logs (
    id             INT PRIMARY KEY,
    ip_address     VARCHAR(45),
    method         VARCHAR(10),
    path           VARCHAR(500),
    endpoint       VARCHAR(200),
    status_code    INT,
    status_class   VARCHAR(5),     -- 2xx / 3xx / 4xx / 5xx
    response_size  INT,
    referer        TEXT,
    user_agent     TEXT,
    is_bot         BOOLEAN,
    client_type    VARCHAR(20),    -- browser / bot / api_client / unknown
    requested_at   TIMESTAMP,
    request_date   DATE,
    request_hour   INT,
    is_valid       BOOLEAN,
    invalid_reason VARCHAR(200)
);

-- ============================================================
-- LAYER 3: AGGREGATIONS
-- ============================================================

CREATE TABLE IF NOT EXISTS agg_hourly_traffic (
    request_date      DATE,
    request_hour      INT,
    total_requests    INT,
    unique_ips        INT,
    error_count       INT,
    error_rate        NUMERIC(5,2),
    avg_response_size NUMERIC(10,2),
    bot_requests      INT,
    PRIMARY KEY (request_date, request_hour)
);

CREATE TABLE IF NOT EXISTS agg_endpoint_stats (
    report_date    DATE,
    endpoint       VARCHAR(200),
    total_hits     INT,
    success_hits   INT,
    error_hits     INT,
    not_found_hits INT,
    avg_size       NUMERIC(10,2),
    hit_rank       INT,
    PRIMARY KEY (report_date, endpoint)
);

CREATE TABLE IF NOT EXISTS agg_ip_anomalies (
    report_date      DATE,
    ip_address       VARCHAR(45),
    total_requests   INT,
    error_requests   INT,
    error_rate       NUMERIC(5,2),
    unique_endpoints INT,
    is_suspicious    BOOLEAN,
    anomaly_reason   VARCHAR(200),
    PRIMARY KEY (report_date, ip_address)
);

CREATE TABLE IF NOT EXISTS data_quality_report (
    report_date     DATE PRIMARY KEY,
    total_raw       INT,
    total_valid     INT,
    total_invalid   INT,
    invalid_rate    NUMERIC(5,2),
    null_ip_count   INT,
    invalid_method  INT,
    invalid_status  INT,
    duplicate_count INT,
    quality_score   NUMERIC(5,2)
);
