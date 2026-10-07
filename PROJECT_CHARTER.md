# Project Charter: TheLook E-Commerce Hybrid Lakehouse Platform

> **Version**: 2.1  
> **Last Updated**: 2026-10-07  
> **Owner**: Ömer Faruk Doğru — Senior Data / Analytics Engineer  
> **Status**: 🟡 In Progress — Sprint 1 Complete ✅ | Sprint 2 Starting

---

## 1. Executive Summary

This project delivers a **production-grade, end-to-end data platform** that combines batch and real-time streaming pipelines under a unified **Medallion Architecture (Bronze → Silver → Gold)**. It ingests the TheLook E-Commerce dataset alongside synthetically generated web/mobile clickstream events, processes them through validated and contracted layers, and surfaces them as Kimball-style dimensional models ready for BI consumption.

**Why this project matters:**
- Demonstrates mastery of the **full modern data stack** (Kafka, Spark, dbt, Airflow, Docker, Great Expectations) — technologies actively demanded in Senior/Lead Data Engineer roles.
- Produces a **living portfolio artifact** — not a toy demo, but a reproducible, tested, CI/CD-guarded platform that hiring managers can `docker compose up` and explore.
- Embodies **learning-by-building**: every architectural decision is documented with trade-offs, every pattern is intentional, and every shortcut is avoided.

---

## 2. Core Architectural Principles

### 2.1 Medallion Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                     │
│  ┌──────────────────┐    ┌──────────────────────────────────────────┐   │
│  │ TheLook Dataset  │    │ Synthetic Clickstream (Kafka Producer)  │   │
│  │ (Batch / CSV)    │    │ (Real-Time / JSON)                      │   │
│  └────────┬─────────┘    └────────────────┬─────────────────────────┘   │
│           │                               │                             │
│           ▼                               ▼                             │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                    BRONZE LAYER (SeaweedFS S3 / s3a://lakehouse)       │   │
│  │  • Raw, immutable, append-only                                   │   │
│  │  • Technical metadata: _ingested_at, _source_file, _batch_id     │   │
│  │  • Schema-on-read (Parquet for batch, Parquet for streaming)     │   │
│  │  • Retention: 90 days (configurable)                             │   │
│  └──────────────────────────┬───────────────────────────────────────┘   │
│                             │                                           │
│                    Data Contracts + GX Validation                        │
│                             │                                           │
│                             ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │            SILVER LAYER (SeaweedFS S3 + PostgreSQL)              │   │
│  │  • Cleaned, deduplicated, typed, conformed                       │   │
│  │  • Business keys resolved, nulls handled                        │   │
│  │  • SCD Type 2 where applicable (dim_products)                   │   │
│  │  • Incremental load strategy (merge/upsert)                     │   │
│  └──────────────────────────┬───────────────────────────────────────┘   │
│                             │                                           │
│                      dbt Transformations                                │
│                             │                                           │
│                             ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                    GOLD LAYER (PostgreSQL)                       │   │
│  │  • Kimball Star Schema: dim_users, dim_products, fct_orders      │   │
│  │  • Streaming marts: fct_clickstream_sessions (incremental)       │   │
│  │  • KPI aggregates: hourly_gmv, cart_abandonment, conversion_rate │   │
│  │  • Materialized views for BI consumption                        │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Dual Pipeline Architecture (Lambda-Inspired)

| Aspect | Batch Pipeline | Streaming Pipeline |
|--------|---------------|-------------------|
| **Source** | TheLook CSV dataset | Synthetic Kafka events |
| **Ingestion** | Python scripts → SeaweedFS Bronze | Kafka Producer → Topic |
| **Processing** | dbt models (Silver/Gold) | PySpark Structured Streaming |
| **Latency** | Daily / Hourly | Near real-time (30s micro-batch) |
| **Output** | PostgreSQL Star Schema | SeaweedFS Parquet → dbt incremental merge |
| **Quality Gate** | Great Expectations + dbt tests | Schema validation + Watermarking |

### 2.3 Non-Negotiable Design Principles

1. **Idempotency**: Every pipeline step must be safely re-runnable. No duplicates on backfill.
2. **Data Contracts First**: No data enters Silver without passing schema + quality validation.
3. **Immutable Bronze**: Raw data is never mutated. Failed records go to `_dead_letter/`.
4. **Infrastructure as Code**: Everything runs via `docker compose up`. No manual setup.
5. **Test Everything**: dbt tests, GX suites, pytest units, CI/CD linting — no untested code ships.
6. **No Secrets in Code**: All credentials via `.env` files and environment variables.
7. **Observability**: Structured logging, Airflow task-level monitoring, data freshness checks.

---

## 3. Technology Stack Rationale

### Why These Specific Technologies?

| Technology | Role | Why This Choice (Trade-offs) |
|-----------|------|------------------------------|
| **Docker Compose** | Local orchestration | Single-command reproducibility. Trade-off: Not K8s-grade, but sufficient for portfolio/POC. |
| **MinIO → SeaweedFS** | S3-compatible object store | MinIO Community Edition was archived/deprecated in 2025/2026. SeaweedFS is the drop-in open-source replacement. Same boto3 API, same S3A config — only endpoint URL changes. |
| **Kafka (KRaft)** | Event streaming | Zookeeper-free = simpler ops, lower resource footprint. Single broker is fine for demo-scale. |
| **PySpark 3.5+** | Stream processing | Structured Streaming with watermarking is production-standard. Trade-off: Heavy container, but demonstrates real Spark skills. |
| **dbt-core** | Transformation layer | Industry standard for SQL-first transformations. Incremental models + contracts = Gold standard for analytics engineering. |
| **dbt-postgres** | dbt adapter | PostgreSQL as DWH target keeps infra simple. Could swap to dbt-duckdb for embedded analytics. |
| **Airflow 2.8+** | Orchestration | TaskFlow API is modern Airflow. Trade-off: Complex setup, but it's what enterprises use. |
| **Great Expectations** | Data quality | Most mature open-source DQ framework. Integrates with Airflow & produces HTML reports. |
| **PostgreSQL** | DWH + Airflow metadata | Dual-purpose: Airflow metadata DB + analytical target. Keeps container count manageable. |
| **Pydantic v2** | Schema validation | Runtime type checking for Python data models. Complements GX for ingestion contracts. |
| **GitHub Actions** | CI/CD | Free for public repos. Runs Ruff, SQLFluff, pytest, dbt compile on every PR. |

### What We Deliberately Excluded (and Why)

| Excluded Tech | Reason |
|--------------|--------|
| **Delta Lake / Iceberg** | Adds JVM dependency complexity. Parquet on SeaweedFS demonstrates the same lakehouse concepts with simpler ops. Future enhancement candidate. |
| **Kubernetes / Helm** | Overkill for portfolio project. Docker Compose achieves the same reproducibility at lower complexity. |
| **Celery Executor** | LocalExecutor is sufficient for our DAG count. Avoids Redis/RabbitMQ container overhead. |
| **Superset / Metabase** | BI layer is out of scope. Gold layer is BI-ready; any tool can connect to PostgreSQL. |
| **Terraform** | No cloud deployment target. Docker Compose is our IaC equivalent for local infra. |

---

## 4. Data Domain & Sources

### 4.1 TheLook E-Commerce Dataset (Batch)

| Entity | Key Fields | Volume (Approx.) | Refresh |
|--------|-----------|-------------------|---------|
| `users` | user_id, name, email, age, gender, country, created_at | ~100K rows | Full load |
| `orders` | order_id, user_id, status, created_at, returned_at, num_of_item | ~100K rows | Incremental |
| `order_items` | id, order_id, product_id, sale_price, status | ~200K rows | Incremental |
| `products` | id, name, category, brand, department, retail_price | ~30K rows | Full load (SCD2) |
| `inventory_items` | id, product_id, created_at, sold_at, cost | ~200K rows | Incremental |
| `distribution_centers` | id, name, latitude, longitude | ~10 rows | Full load |

### 4.2 Synthetic Clickstream (Streaming)

| Field | Type | Description |
|-------|------|-------------|
| `event_id` | UUID | Unique event identifier |
| `session_id` | UUID | Browser/app session |
| `user_id` | Integer (nullable) | FK to users (null = anonymous) |
| `event_type` | Enum | `page_view`, `add_to_cart`, `remove_from_cart`, `purchase`, `search` |
| `page_url` | String | Simulated URI path |
| `product_id` | Integer (nullable) | FK to products (when applicable) |
| `search_query` | String (nullable) | Search term (when event_type = search) |
| `referrer` | String | Traffic source |
| `device_type` | Enum | `desktop`, `mobile`, `tablet` |
| `browser` | String | User agent category |
| `ip_address` | String | Simulated IP |
| `event_timestamp` | Timestamp | Event time (with intentional late-arriving events) |
| `received_at` | Timestamp | Kafka ingestion time |

---

## 5. Success Criteria

| Criterion | Measurement | Target |
|-----------|-------------|--------|
| **Reproducibility** | `docker compose up` starts all services | < 5 min cold start |
| **Data Quality** | GX + dbt test pass rate | 100% on happy path |
| **Streaming Latency** | Event → Silver layer | < 60 seconds |
| **Idempotency** | Re-run any DAG without duplicates | Zero duplicate rows |
| **Test Coverage** | pytest + dbt tests | > 80% critical path coverage |
| **CI/CD** | GitHub Actions on every PR | Lint + test + compile pass |
| **Documentation** | README + architecture diagrams | Senior-level, interview-ready |

---

## 6. Out of Scope (v1.0)

- Cloud deployment (AWS/GCP) — local Docker only
- BI dashboarding layer (Superset, Metabase, Looker)
- ML feature store / model serving
- CDC (Change Data Capture) from a live database
- Multi-node Kafka / Spark clusters
- Authentication / RBAC on SeaweedFS or Airflow (dev mode)
- Data catalog (DataHub, OpenMetadata) — future enhancement

---

## 7. Risk Register

| Risk | Impact | Mitigation |
|------|--------|------------|
| Docker resource exhaustion (RAM/CPU) | Services crash | Tune container limits; document minimum specs (16GB RAM recommended) |
| Kafka message loss on restart | Data gaps in streaming | Persistent volumes + consumer offset management |
| Small file problem in streaming output | Slow downstream queries | Trigger interval tuning (30s) + periodic compaction |
| dbt/Postgres lock contention | Transformation failures | Separate Airflow metadata DB from DWH (future) |
| Scope creep | Delayed delivery | Strict sprint boundaries; MVP-first mindset |

---

## 8. Guiding Philosophy

> **"The product we build matters as much as what we learn building it."**

Every design decision in this project is intentional. Every trade-off is documented. Every pattern is borrowed from real-world production systems. This is not a tutorial project — it is a demonstration of engineering maturity.
