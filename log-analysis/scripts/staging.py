"""
staging.py
Enrich raw_logs → stg_logs:
  - Phân loại client_type (browser / bot / api_client)
  - Tách endpoint (bỏ query string)
  - Gán status_class (2xx/3xx/4xx/5xx)
  - Validate và flag is_valid
"""
from db import get_conn

VALID_METHODS = {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}

BOT_KEYWORDS = [
    "bot", "crawler", "spider", "scraper", "curl", "wget",
    "python-requests", "java/", "go-http", "googlebot",
    "bingbot", "yandex", "baidu", "slurp", "duckduck",
]

def classify_client(user_agent: str) -> tuple[bool, str]:
    """Return (is_bot, client_type)."""
    if not user_agent:
        return False, "unknown"
    ua_lower = user_agent.lower()
    if any(k in ua_lower for k in BOT_KEYWORDS):
        return True, "bot"
    if "mozilla" in ua_lower or "webkit" in ua_lower:
        return False, "browser"
    if "python" in ua_lower or "java" in ua_lower or "go-http" in ua_lower:
        return False, "api_client"
    return False, "unknown"


def run_staging():
    print("[staging] Running...")
    conn = get_conn()
    cur = conn.cursor()

    # Fetch raw logs chưa được stage
    cur.execute("""
        SELECT r.id, r.ip_address, r.method, r.path, r.protocol,
               r.status_code, r.response_size, r.referer, r.user_agent, r.requested_at
        FROM raw_logs r
        WHERE NOT EXISTS (SELECT 1 FROM stg_logs s WHERE s.id = r.id)
    """)
    rows = cur.fetchall()
    print(f"  [staging] Processing {len(rows)} raw rows...")

    records = []
    for row in rows:
        (id_, ip, method, path, protocol,
         status, size, referer, agent, req_at) = row

        # Enrich
        endpoint = path.split("?")[0] if path else None
        status_class = f"{str(status)[0]}xx" if status else None
        is_bot, client_type = classify_client(agent or "")

        # Validate
        is_valid = True
        reason = None
        if not ip:
            is_valid, reason = False, "Missing IP"
        elif method not in VALID_METHODS:
            is_valid, reason = False, f"Invalid method: {method}"
        elif status not in range(100, 600):
            is_valid, reason = False, f"Invalid status: {status}"
        elif not req_at:
            is_valid, reason = False, "Missing timestamp"

        records.append((
            id_, ip, method, path, endpoint,
            status, status_class, size, referer, agent,
            is_bot, client_type,
            req_at,
            req_at.date() if req_at else None,
            req_at.hour if req_at else None,
            is_valid, reason,
        ))

    cur.executemany(
        """
        INSERT INTO stg_logs
            (id, ip_address, method, path, endpoint,
             status_code, status_class, response_size, referer, user_agent,
             is_bot, client_type,
             requested_at, request_date, request_hour,
             is_valid, invalid_reason)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO NOTHING
        """,
        records,
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"[staging] Done — {len(records)} rows staged")


if __name__ == "__main__":
    run_staging()
