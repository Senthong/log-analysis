"""
report.py
Export analytics results ra CSV + HTML report.
Chạy sau analytics.py để tạo deliverable cho stakeholders.
"""
import os
import csv
from datetime import datetime
from db import get_conn

REPORT_DIR = os.path.join(os.path.dirname(__file__), "..", "reports")


def ensure_dir():
    os.makedirs(REPORT_DIR, exist_ok=True)


def export_csv(cur, query: str, filename: str, params=None):
    cur.execute(query, params or ())
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    path = os.path.join(REPORT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    print(f"  [report] Exported {len(rows)} rows → {filename}")
    return rows, cols


def export_html_report(data: dict, run_date: str):
    """Tạo HTML report đẹp từ query results."""

    def table_html(cols, rows, limit=20):
        if not rows:
            return "<p>No data</p>"
        header = "".join(f"<th>{c}</th>" for c in cols)
        body = ""
        for row in rows[:limit]:
            cells = "".join(f"<td>{v if v is not None else '-'}</td>" for v in row)
            body += f"<tr>{cells}</tr>"
        return f"<table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Log Analysis Report — {run_date}</title>
<style>
  body {{ font-family: Arial, sans-serif; margin: 40px; background: #f5f7fa; color: #222; }}
  h1   {{ color: #1e5799; }}
  h2   {{ color: #2563eb; border-bottom: 2px solid #2563eb; padding-bottom: 6px; margin-top: 40px; }}
  table {{ border-collapse: collapse; width: 100%; background: #fff; margin-top: 12px; }}
  th   {{ background: #1e5799; color: white; padding: 8px 12px; text-align: left; font-size: 13px; }}
  td   {{ padding: 7px 12px; border-bottom: 1px solid #e2e8f0; font-size: 13px; }}
  tr:hover {{ background: #f0f4ff; }}
  .meta {{ color: #666; font-size: 13px; margin-bottom: 30px; }}
  .badge-ok  {{ background: #22c55e; color: white; padding: 2px 8px; border-radius: 4px; font-size: 12px; }}
  .badge-warn {{ background: #f59e0b; color: white; padding: 2px 8px; border-radius: 4px; font-size: 12px; }}
  .badge-err {{ background: #ef4444; color: white; padding: 2px 8px; border-radius: 4px; font-size: 12px; }}
</style>
</head>
<body>
<h1>📊 Log Analysis Report</h1>
<p class="meta">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Date range: {run_date}</p>

<h2>1. Data Quality Summary</h2>
{table_html(*data['quality'])}

<h2>2. Top Endpoints by Traffic</h2>
{table_html(*data['endpoints'])}

<h2>3. Hourly Traffic Pattern</h2>
{table_html(*data['hourly'])}

<h2>4. Suspicious IPs (Anomaly Detection)</h2>
{table_html(*data['anomalies'])}

</body>
</html>"""

    path = os.path.join(REPORT_DIR, f"report_{run_date}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  [report] HTML report → reports/report_{run_date}.html")


def run_report():
    ensure_dir()
    run_date = datetime.now().strftime("%Y-%m-%d")
    print(f"[report] Generating for {run_date}")

    conn = get_conn()
    cur = conn.cursor()

    quality_rows, quality_cols = export_csv(cur, """
        SELECT * FROM data_quality_report ORDER BY report_date DESC LIMIT 30
    """, "data_quality.csv")

    endpoint_rows, endpoint_cols = export_csv(cur, """
        SELECT report_date, endpoint, total_hits, success_hits, error_hits,
               not_found_hits, avg_size, hit_rank
        FROM agg_endpoint_stats
        WHERE hit_rank <= 10
        ORDER BY report_date DESC, hit_rank
    """, "top_endpoints.csv")

    hourly_rows, hourly_cols = export_csv(cur, """
        SELECT * FROM agg_hourly_traffic
        ORDER BY request_date DESC, request_hour
    """, "hourly_traffic.csv")

    anomaly_rows, anomaly_cols = export_csv(cur, """
        SELECT * FROM agg_ip_anomalies
        WHERE is_suspicious = TRUE
        ORDER BY total_requests DESC
        LIMIT 50
    """, "suspicious_ips.csv")

    export_html_report({
        "quality":   (quality_cols, quality_rows),
        "endpoints": (endpoint_cols, endpoint_rows),
        "hourly":    (hourly_cols, hourly_rows),
        "anomalies": (anomaly_cols, anomaly_rows),
    }, run_date)

    cur.close()
    conn.close()
    print("[report] Done — check reports/ folder")


if __name__ == "__main__":
    run_report()
