# 📋 Log Analysis & Data Quality Pipeline

Parse, validate, analyze Nginx access logs using PostgreSQL advanced SQL + Python.

## Architecture

```
access.log (Nginx Combined Format — 50,000 lines)
        │
        ▼
┌─────────────────┐
│   Raw Layer     │  raw_logs — parsed log entries
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Staging Layer   │  stg_logs — enriched + validated
│                 │  • client_type: browser/bot/api_client
│                 │  • status_class: 2xx/3xx/4xx/5xx
│                 │  • is_valid flag + invalid_reason
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│ Analytics Layer (Advanced SQL)          │
│  agg_hourly_traffic   — peak hours      │
│  agg_endpoint_stats   — RANK() by hits  │
│  agg_ip_anomalies     — PERCENTILE_CONT │
│  data_quality_report  — quality score   │
└────────┬────────────────────────────────┘
         │
         ▼
┌─────────────────┐
│ Reports         │  CSV exports + HTML report
└─────────────────┘
```

## Tech Stack

| Tool | Purpose |
|---|---|
| **Python** | Log parsing, pipeline orchestration |
| **PostgreSQL 15** | Storage + advanced SQL analytics |
| **Docker + Compose** | Containerization |
| **Git** | Version control |

## Key SQL Concepts Used

| Concept | Where |
|---|---|
| `RANK() OVER (PARTITION BY ...)` | Rank endpoints per day |
| `LAG()` | Compare hourly traffic vs previous hour |
| `PERCENTILE_CONT(0.95/0.99)` | IP anomaly thresholds |
| `SUM() OVER (ORDER BY ...)` | Running totals |
| `AVG() OVER (ROWS BETWEEN ...)` | 7-day rolling error rate |
| CTEs | Multi-step analytics queries |
| Conditional aggregation | Status code pivot by day |

## Project Structure

```
log-analysis/
├── scripts/
│   ├── db.py              # DB connection
│   ├── parser.py          # Regex parser, batch insert
│   ├── staging.py         # Enrich + validate
│   ├── analytics.py       # Window functions, CTEs
│   ├── report.py          # CSV + HTML export
│   └── run_pipeline.py    # Orchestrator
├── sql/
│   ├── 01_schema.sql      # All table definitions
│   └── 02_analytics_queries.sql  # Standalone SQL for exploration
├── data/
│   └── access.log         # 50,000 line Nginx log (generated)
├── reports/               # Output: CSV + HTML report
├── docker-compose.yml
├── Dockerfile
└── README.md
```

## Quick Start

### Option A — Docker (recommended)

```bash
git clone https://github.com/senthong/log-analysis.git
cd log-analysis
docker-compose up --build
# Pipeline runs automatically, check reports/ when done
```

## Output Reports

After running, `reports/` contains:
- `data_quality.csv` — quality score per day
- `top_endpoints.csv` — ranked endpoint stats
- `hourly_traffic.csv` — hourly breakdown
- `suspicious_ips.csv` — anomaly flagged IPs
- `report_YYYY-MM-DD.html` — full HTML dashboard
