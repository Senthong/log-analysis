# log-analysis-realtime

Real-time extension của project [log-analysis](https://github.com/Senthong/log-analysis).

## Architecture

```
access.log
    │
    ▼
log_producer.py  ──►  Kafka Topic: nginx.raw_logs
                              │
                              ▼
                  Spark Structured Streaming
                  (parse + enrich — same logic as parser.py + staging.py)
                              │
                              ▼
                    PostgreSQL: stg_logs_stream
                              │
                              ▼
                    monitoring_queries.sql
```

## Stack

| Layer | Tech |
|---|---|
| Message broker | Apache Kafka 7.5 (Confluent) |
| Stream processor | Apache Spark 3.5 Structured Streaming |
| Sink | PostgreSQL 15 |
| Containerization | Docker Compose |

## Quickstart

### 1. Chuẩn bị data

Copy `access.log` từ project log-analysis vào thư mục `data/`:

```bash
cp ../log-analysis/data/access.log ./data/access.log
```

### 2. Khởi động infrastructure

```bash
# Start Zookeeper + Kafka + PostgreSQL trước
docker-compose up -d zookeeper kafka postgres

# Đợi khoảng 20-30 giây cho Kafka sẵn sàng
# Kiểm tra Kafka đã ready chưa:
docker-compose logs kafka | grep "started"
```

### 3. Start Spark Streaming

```bash
docker-compose up -d spark

# Xem logs Spark
docker-compose logs -f spark
```

### 4. Start Producer

```bash
docker-compose up -d producer

# Xem logs producer
docker-compose logs -f producer
```

### 5. Monitor data vào PostgreSQL

```bash
# Kết nối PostgreSQL
docker exec -it rt_postgres psql -U postgres -d log_db

# Đếm rows đang vào
SELECT COUNT(*), MAX(ingested_at) FROM stg_logs_stream;

# Throughput theo phút
\i /docker-entrypoint-initdb.d/  -- (dùng query trong sql/02_monitoring_queries.sql)
```

## Spark UI

Truy cập **http://localhost:4040** để xem:
- Streaming query progress
- Batch processing time
- Input rate (records/sec)

## Cấu trúc thư mục

```
log-analysis-realtime/
├── docker-compose.yml          # Toàn bộ stack
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── log_producer.py         # Kafka producer
├── spark_streaming/
│   └── streaming_job.py        # Spark Structured Streaming job
├── sql/
│   ├── 01_schema.sql           # Schema (batch + stream tables)
│   └── 02_monitoring_queries.sql
├── data/                       # Đặt access.log vào đây
└── README.md
```

## Env vars (Producer)

| Var | Default | Mô tả |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker |
| `KAFKA_TOPIC` | `nginx.raw_logs` | Topic name |
| `LOG_FILE` | `data/access.log` | Path đến log file |
| `DELAY_MS` | `10` | Delay giữa các message (ms) |
| `LOOP` | `false` | Lặp lại file sau khi đọc xong |

## Concepts học được

| Concept | Nơi áp dụng |
|---|---|
| Kafka Producer API | `log_producer.py` |
| Topic / Partition / Offset | Kafka config trong docker-compose |
| Spark readStream | `streaming_job.py` — source |
| UDF (User Defined Function) | `_parse_udf` — parse log |
| foreachBatch | Sink → JDBC PostgreSQL |
| Checkpoint | Recovery sau khi restart |
| Micro-batch trigger | `processingTime=10 seconds` |
| maxOffsetsPerTrigger | Backpressure — giới hạn batch size |

## So sánh Batch vs Streaming

| | Batch (project cũ) | Streaming (project này) |
|---|---|---|
| Source | File tĩnh `access.log` | Kafka topic liên tục |
| Trigger | Manual / Airflow schedule | Tự động mỗi 10s |
| Latency | Phút / giờ | Giây |
| Fault recovery | Re-run script | Checkpoint + offset |
| Scale | Pandas / psycopg2 | Spark (distributed) |
