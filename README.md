# airflow-spark-pipeline

Batch data pipeline for **logistics delivery events**, orchestrated with Apache Airflow 2.x and processed with PySpark 3.x. Raw delivery events are ingested from a relational source, transformed into analytics-ready facts, and aggregated into daily metrics, all stored as Delta Lake tables on Azure ADLS Gen2.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         AIRFLOW SCHEDULER                               │
│                    (CeleryExecutor + Redis broker)                      │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │  triggers DAG: logistics_daily_pipeline
                               │  schedule: 0 6 * * 1-5 (Mon-Fri 06:00 UTC)
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     logistics_daily_pipeline DAG                        │
│                                                                         │
│  [check_source_availability]                                            │
│          │  PythonOperator: verifies JDBC connectivity                 │
│          ▼                                                              │
│  [ingest_delivery_events]                                               │
│          │  SparkSubmitOperator → spark_jobs/ingest_deliveries.py       │
│          │  Reads from JDBC with watermark, writes to RAW layer         │
│          ▼                                                              │
│  [transform_delivery_facts]                                             │
│          │  SparkSubmitOperator → spark_jobs/transform_deliveries.py    │
│          │  Deduplicates, computes duration, SLA flag, MERGE to CURATED │
│          ▼                                                              │
│  [aggregate_daily_metrics]                                              │
│          │  SparkSubmitOperator → spark_jobs/aggregate_deliveries.py    │
│          │  Courier-level KPIs, writes to AGGREGATED layer              │
│          ▼                                                              │
│  [notify_completion]                                                    │
│          PythonOperator: logs pipeline summary                         │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                  DELTA LAKE on Azure ADLS Gen2                          │
│                                                                         │
│  abfss://datalake@<account>.dfs.core.windows.net/logistics/             │
│                                                                         │
│  ├── raw/delivery_events/          ← RAW LAYER                         │
│  │       partitioned by year/month/day/courier_id                      │
│  │       append-only, preserves original source records                │
│  │                                                                     │
│  ├── curated/delivery_facts/       ← CURATED LAYER                     │
│  │       deduplicated facts, SLA flags, delivery duration              │
│  │       upserted via MERGE on event_id (idempotent)                   │
│  │                                                                     │
│  └── aggregated/daily_metrics/     ← AGGREGATED LAYER                  │
│          courier × day KPIs: on-time rate, avg duration, p95           │
│          overwritten per partition (event_date)                        │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Delta Lake Design

### Why MERGE on `event_id`?

Delivery events originate from mobile courier apps and IoT devices, where the same logical event can arrive multiple times due to network retries or at-least-once delivery semantics. A simple append would produce duplicate rows that corrupt downstream aggregations.

The curated layer uses a **Delta Lake MERGE** statement keyed on `event_id`:

- **WHEN MATCHED** (same `event_id` already exists in the target): update all columns with the incoming record. This handles late-arriving corrections: e.g. a status code corrected after manual review.
- **WHEN NOT MATCHED**: insert the new row.

This makes every pipeline run **idempotent**: re-running for the same date produces exactly the same result, which is essential for safe backfills and retry logic.

```sql
MERGE INTO curated.delivery_facts AS target
USING incoming AS source ON target.event_id = source.event_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

### Three-Layer Delta Architecture

| Layer | Path | Write mode | Partitioning | Purpose |
|-------|------|-----------|--------------|---------|
| **Raw** | `raw/delivery_events/` | Append | year/month/day/courier_id | Immutable landing zone, full audit trail |
| **Curated** | `curated/delivery_facts/` | MERGE (upsert) | event_date | Deduplicated facts with business logic |
| **Aggregated** | `aggregated/daily_metrics/` | Overwrite partition | event_date | Pre-computed KPIs for BI tools |

---

## Domain: Delivery Events

Each event represents a state transition in a parcel's lifecycle:

| `event_type` | Description |
|---|---|
| `pickup` | Courier collected parcel from sender / warehouse |
| `in-transit` | Parcel moving toward destination |
| `delivered` | Successfully handed to recipient |
| `failed` | Delivery attempt failed (wrong address, not home, etc.) |

**SLA**: A delivery is considered **on-time** if `delivery_duration_hours <= 48` (configurable in `config/pipeline_config.yaml`).

---

## Project Structure

```
.
├── dags/
│   ├── logistics_daily_pipeline.py   # Main Airflow DAG
│   └── utils/
│       └── spark_config.py           # Spark conf builder (ADLS auth)
├── spark_jobs/
│   ├── ingest_deliveries.py          # Layer 1: Raw ingest from JDBC
│   ├── transform_deliveries.py       # Layer 2: Facts + SLA computation
│   ├── aggregate_deliveries.py       # Layer 3: Daily KPI aggregation
│   └── utils/
│       ├── delta_utils.py            # MERGE, vacuum, table creation helpers
│       └── schema.py                 # Shared StructType definitions
├── config/
│   └── pipeline_config.yaml          # Spark tuning, paths, SLA config
├── tests/
│   └── test_transform_deliveries.py  # Pytest suite (local Spark)
├── docker-compose.yml                # Airflow stack (webserver + scheduler + postgres + redis)
├── requirements.txt
├── .env.example
└── README.md
```

---

## Quick Start with Docker Compose

### Prerequisites

- Docker Desktop >= 24.x
- Docker Compose v2
- Azure Storage Account with ADLS Gen2 enabled

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env with your Azure credentials and DB passwords
```

### 2. Start the stack

```bash
# Initialise Airflow metadata DB (first time only)
docker compose run --rm airflow-webserver airflow db migrate

# Create admin user (first time only)
docker compose run --rm airflow-webserver airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com

# Start all services
docker compose up -d

# Tail logs
docker compose logs -f airflow-scheduler
```

Airflow UI will be available at **http://localhost:8080**.

### 3. Configure Airflow Connections

Open the Airflow UI → **Admin → Connections** and create:

**`spark_default`** (Spark connection)
- Conn Type: `Spark`
- Host: `spark://spark-master` (or your cluster URL)
- Port: `7077`

**`delivery_jdbc`** (source database)
- Conn Type: `JDBC`
- Host: your DB host
- Schema: your database name
- Login / Password: DB credentials
- Extra: `{"driver_path": "/opt/spark/jars/postgresql.jar"}`

### 4. Set Airflow Variables

Go to **Admin → Variables** and add:

| Key | Example Value |
|-----|---------------|
| `azure_storage_account` | `mystorageaccount` |
| `azure_client_id` | `<UUID>` |
| `azure_client_secret` | `<secret>` |
| `azure_tenant_id` | `<UUID>` |
| `pipeline_sla_hours` | `48` |

### 5. Trigger the DAG

```bash
# Via CLI inside the container
docker compose exec airflow-scheduler airflow dags trigger logistics_daily_pipeline --conf '{"date": "2024-01-15"}'

# Or click the play button in the UI
```

---

## Running Tests

```bash
pip install -r requirements.txt
pytest tests/ -v
```

Tests use a local `SparkSession(master="local[1]")`, no cluster required.

---

## Configuration Reference

All pipeline knobs live in `config/pipeline_config.yaml`. Key settings:

- `spark.executor_memory`: memory per executor (default `4g`)
- `pipeline.lookback_days`: watermark lookback window for safe re-ingestion
- `pipeline.sla_hours`: on-time delivery threshold in hours (default `48`)

---

## License

MIT
