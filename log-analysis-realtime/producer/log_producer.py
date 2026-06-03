"""
log_producer.py
───────────────
Đọc access.log từng dòng → push lên Kafka topic nginx.raw_logs.
Thêm delay nhỏ để giả lập real-time stream.

Env vars:
  KAFKA_BOOTSTRAP_SERVERS  (default: localhost:9092)
  KAFKA_TOPIC              (default: nginx.raw_logs)
  LOG_FILE                 (default: data/access.log)
  DELAY_MS                 (default: 10  → ~100 msg/s)
  LOOP                     (default: false → stream file 1 lần)
                            set LOOP=true để lặp lại file liên tục
"""
import os
import time
import sys

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC             = os.getenv("KAFKA_TOPIC", "nginx.raw_logs")
LOG_FILE          = os.getenv("LOG_FILE", "data/access.log")
DELAY_MS          = float(os.getenv("DELAY_MS", "10"))
LOOP              = os.getenv("LOOP", "false").lower() == "true"
FLUSH_EVERY       = 500   # flush producer buffer mỗi N messages


def wait_for_kafka(max_retries: int = 10, wait_sec: int = 5) -> KafkaProducer:
    """Retry connect đến Kafka — container startup có thể chậm hơn healthcheck."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: v.encode("utf-8"),
                acks="all",               # đợi broker xác nhận
                retries=3,
                linger_ms=5,              # gom messages nhỏ lại trước khi gửi
            )
            print(f"[producer] Connected to Kafka at {BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            print(f"[producer] Kafka not ready (attempt {attempt}/{max_retries}), "
                  f"retrying in {wait_sec}s...")
            time.sleep(wait_sec)
    print("[producer] Could not connect to Kafka. Exiting.")
    sys.exit(1)


def stream_file(producer: KafkaProducer, log_file: str) -> int:
    """
    Đọc log_file từng dòng, push lên Kafka.
    Return: số messages đã gửi.
    """
    sent = 0
    skipped = 0

    with open(log_file, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                skipped += 1
                continue

            producer.send(TOPIC, value=line)
            sent += 1

            if sent % FLUSH_EVERY == 0:
                producer.flush()
                print(f"  [producer] Sent {sent:,} messages to '{TOPIC}'...")

            if DELAY_MS > 0:
                time.sleep(DELAY_MS / 1000)

    producer.flush()
    return sent


def main():
    if not os.path.exists(LOG_FILE):
        print(f"[producer] ERROR: Log file not found: {LOG_FILE}")
        print("  → Copy access.log vào thư mục data/ trước khi chạy.")
        sys.exit(1)

    producer = wait_for_kafka()

    print(f"[producer] Streaming '{LOG_FILE}' → topic '{TOPIC}'")
    print(f"  delay={DELAY_MS}ms | loop={LOOP}")
    print("-" * 50)

    run = 0
    while True:
        run += 1
        total = stream_file(producer, LOG_FILE)
        print(f"[producer] Run #{run} complete — {total:,} messages sent.")

        if not LOOP:
            break
        print("[producer] Looping — replaying file...")
        time.sleep(1)

    producer.close()
    print("[producer] Done.")


if __name__ == "__main__":
    main()
