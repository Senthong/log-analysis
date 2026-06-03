"""
streaming_job.py
────────────────
Spark Structured Streaming pipeline:
  Kafka topic nginx.raw_logs
      → parse + enrich (logic tái sử dụng từ parser.py + staging.py)
      → foreachBatch → PostgreSQL table stg_logs_stream

Chạy bằng spark-submit (xem docker-compose.yml).
"""
import re
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, current_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType,
)

# ── Config ───────────────────────────────────────────────────────────────
KAFKA_SERVERS  = "kafka:9092"
KAFKA_TOPIC    = "nginx.raw_logs"
PG_URL         = "jdbc:postgresql://postgres:5432/log_db"
PG_TABLE       = "stg_logs_stream"
PG_PROPS       = {
    "user":     "postgres",
    "password": "postgres",
    "driver":   "org.postgresql.Driver",
}
CHECKPOINT_DIR = "/tmp/spark_checkpoint/nginx_streaming"
TRIGGER_SECS   = "10 seconds"   # micro-batch interval

# ── Schema trả về từ UDF ─────────────────────────────────────────────────
PARSED_SCHEMA = StructType([
    StructField("ip_address",     StringType(),   True),
    StructField("method",         StringType(),   True),
    StructField("path",           StringType(),   True),
    StructField("endpoint",       StringType(),   True),
    StructField("protocol",       StringType(),   True),
    StructField("status_code",    IntegerType(),  True),
    StructField("status_class",   StringType(),   True),
    StructField("response_size",  IntegerType(),  True),
    StructField("user_agent",     StringType(),   True),
    StructField("is_bot",         BooleanType(),  True),
    StructField("client_type",    StringType(),   True),
    StructField("is_valid",       BooleanType(),  True),
    StructField("invalid_reason", StringType(),   True),
    StructField("requested_at",   TimestampType(), True),
])

# ── Parse / Enrich logic (copy từ parser.py + staging.py) ────────────────
_LOG_PATTERN = re.compile(
    r'(?P<ip>\S+) \S+ \S+ '
    r'\[(?P<time>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<path>\S+) (?P<protocol>[^"]+)" '
    r'(?P<status>\d{3}) '
    r'(?P<size>\S+) '
    r'"(?P<referer>[^"]*)" '
    r'"(?P<agent>[^"]*)"'
)
_TIME_FMT      = "%d/%b/%Y:%H:%M:%S %z"
_VALID_METHODS = {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}
_BOT_KEYWORDS  = [
    "bot", "crawler", "spider", "scraper",
    "curl", "wget", "python-requests",
    "googlebot", "bingbot", "yandex", "baidu",
]


def _classify_client(ua: str):
    """Return (is_bot: bool, client_type: str)."""
    if not ua:
        return False, "unknown"
    ua_l = ua.lower()
    if any(k in ua_l for k in _BOT_KEYWORDS):
        return True, "bot"
    if "mozilla" in ua_l or "webkit" in ua_l:
        return False, "browser"
    if "python" in ua_l or "java" in ua_l or "go-http" in ua_l:
        return False, "api_client"
    return False, "unknown"


def _parse_and_enrich(raw_line: str):
    """
    Parse 1 dòng Nginx log → tuple theo PARSED_SCHEMA.
    Return None nếu dòng không match regex (dòng lỗi / blank).
    """
    if not raw_line:
        return None

    m = _LOG_PATTERN.match(raw_line.strip())
    if not m:
        return None

    try:
        ip       = m.group("ip")
        method   = m.group("method")
        path     = m.group("path")[:500]
        protocol = m.group("protocol")
        status   = int(m.group("status"))
        raw_size = m.group("size")
        size     = int(raw_size) if raw_size != "-" else 0
        agent    = m.group("agent")
        req_at   = (datetime
                    .strptime(m.group("time"), _TIME_FMT)
                    .replace(tzinfo=None))

        endpoint     = path.split("?")[0]
        status_class = f"{str(status)[0]}xx"
        is_bot, client_type = _classify_client(agent)

        # Validate (mirror staging.py)
        is_valid = True
        reason   = None
        if not ip:
            is_valid, reason = False, "Missing IP"
        elif method not in _VALID_METHODS:
            is_valid, reason = False, f"Invalid method: {method}"
        elif not (100 <= status <= 599):
            is_valid, reason = False, f"Invalid status: {status}"

        return (
            ip, method, path, endpoint, protocol,
            status, status_class, size, agent,
            is_bot, client_type,
            is_valid, reason,
            req_at,
        )

    except Exception:
        return None


# Đăng ký UDF với Spark
_parse_udf = udf(_parse_and_enrich, PARSED_SCHEMA)


# ── foreachBatch sink ────────────────────────────────────────────────────
def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Ghi một micro-batch vào PostgreSQL.
    foreachBatch cho phép dùng JDBC (batch writer) thay vì row-by-row.
    """
    count = batch_df.count()
    if count == 0:
        print(f"[spark] Batch {batch_id}: empty, skipping.")
        return

    print(f"[spark] Batch {batch_id}: writing {count:,} rows → {PG_TABLE}")
    (batch_df
     .write
     .jdbc(url=PG_URL, table=PG_TABLE, mode="append", properties=PG_PROPS))
    print(f"[spark] Batch {batch_id}: done.")


# ── Main ─────────────────────────────────────────────────────────────────
def main():
    spark = (
        SparkSession.builder
        .appName("NginxLogRealtime")
        .config("spark.sql.shuffle.partitions", "4")        # giảm xuống vì data nhỏ
        .config("spark.sql.streaming.metricsEnabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("[spark] Starting Nginx Log Streaming Job")
    print(f"  Kafka  : {KAFKA_SERVERS} / topic={KAFKA_TOPIC}")
    print(f"  Sink   : PostgreSQL → {PG_TABLE}")
    print(f"  Trigger: every {TRIGGER_SECS}")
    print("-" * 50)

    # 1. Source: đọc raw bytes từ Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")       # chỉ xử lý message mới
        .option("maxOffsetsPerTrigger", 10_000)    # giới hạn mỗi batch
        .load()
    )

    # 2. Kafka value là bytes → cast sang string
    lines_df = raw_stream.selectExpr("CAST(value AS STRING) AS raw_line")

    # 3. Parse + enrich qua UDF
    parsed_df = (
        lines_df
        .withColumn("parsed", _parse_udf(col("raw_line")))
        .filter(col("parsed").isNotNull())          # bỏ dòng không parse được
        .select("parsed.*")                         # flatten struct → columns
        .withColumn("ingested_at", current_timestamp())
    )

    # 4. Sink: foreachBatch → JDBC PostgreSQL
    query = (
        parsed_df.writeStream
        .foreachBatch(_write_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=TRIGGER_SECS)
        .start()
    )

    print("[spark] Streaming query started. Waiting for data...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
