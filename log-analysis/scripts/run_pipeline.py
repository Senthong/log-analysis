"""
run_pipeline.py
Chạy toàn bộ pipeline theo thứ tự:
  1. parse   → raw_logs
  2. staging → stg_logs
  3. analytics → agg tables
  4. report  → CSV + HTML
"""
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))

from parser import load_raw
from staging import run_staging
from analytics import run_analytics
from report import run_report


def run(log_file: str = "data/access.log"):
    print("=" * 55)
    print(f"  Log Analysis Pipeline — {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 55)

    print("\n[1/4] Parsing log file...")
    load_raw(log_file)

    print("\n[2/4] Running staging...")
    run_staging()

    print("\n[3/4] Building analytics aggregations...")
    run_analytics()

    print("\n[4/4] Exporting reports...")
    run_report()

    print("\n✅ Pipeline complete. Check reports/ folder.")


if __name__ == "__main__":
    log_file = sys.argv[1] if len(sys.argv) > 1 else "data/access.log"
    run(log_file)
