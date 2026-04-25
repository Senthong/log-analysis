"""
parser.py
Parse Nginx Combined Log Format → load vào raw_logs table.
Hỗ trợ đọc từ file hoặc stdin (streaming).
"""
import re
import os
from datetime import datetime
from db import get_conn

# Nginx combined log format regex
LOG_PATTERN = re.compile(
    r'(?P<ip>\S+) \S+ \S+ '
    r'\[(?P<time>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<path>\S+) (?P<protocol>[^"]+)" '
    r'(?P<status>\d{3}) '
    r'(?P<size>\S+) '
    r'"(?P<referer>[^"]*)" '
    r'"(?P<agent>[^"]*)"'
)

TIME_FORMAT = "%d/%b/%Y:%H:%M:%S %z"

BATCH_SIZE = 1000  # insert theo batch để nhanh hơn


def parse_line(line: str) -> dict | None:
    """Parse một dòng log, return dict hoặc None nếu không match."""
    m = LOG_PATTERN.match(line.strip())
    if not m:
        return None
    try:
        return {
            "ip_address":    m.group("ip"),
            "method":        m.group("method"),
            "path":          m.group("path")[:500],
            "protocol":      m.group("protocol"),
            "status_code":   int(m.group("status")),
            "response_size": int(m.group("size")) if m.group("size") != "-" else 0,
            "referer":       None if m.group("referer") == "-" else m.group("referer"),
            "user_agent":    m.group("agent"),
            "requested_at":  datetime.strptime(m.group("time"), TIME_FORMAT).replace(tzinfo=None),
        }
    except (ValueError, KeyError):
        return None


def load_raw(log_file: str):
    """
    Parse log file và load vào raw_logs.
    Dùng batch insert (BATCH_SIZE=1000) để tối ưu performance.
    """
    if not os.path.exists(log_file):
        raise FileNotFoundError(f"Log file not found: {log_file}")

    conn = get_conn()
    cur = conn.cursor()

    total = 0
    failed = 0
    batch = []

    print(f"[parser] Reading {log_file}")

    with open(log_file, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            record = parse_line(line)
            if record:
                batch.append((
                    record["ip_address"],
                    record["method"],
                    record["path"],
                    record["protocol"],
                    record["status_code"],
                    record["response_size"],
                    record["referer"],
                    record["user_agent"],
                    record["requested_at"],
                ))
            else:
                failed += 1

            # Flush batch
            if len(batch) >= BATCH_SIZE:
                cur.executemany(
                    """
                    INSERT INTO raw_logs
                        (ip_address, method, path, protocol, status_code,
                         response_size, referer, user_agent, requested_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    batch,
                )
                conn.commit()
                total += len(batch)
                print(f"  [parser] Inserted {total} rows...")
                batch = []

    # Flush remainder
    if batch:
        cur.executemany(
            """
            INSERT INTO raw_logs
                (ip_address, method, path, protocol, status_code,
                 response_size, referer, user_agent, requested_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            batch,
        )
        conn.commit()
        total += len(batch)

    cur.close()
    conn.close()
    print(f"[parser] Done — {total} rows inserted, {failed} lines skipped")


if __name__ == "__main__":
    import sys
    log_path = sys.argv[1] if len(sys.argv) > 1 else "data/access.log"
    load_raw(log_path)
