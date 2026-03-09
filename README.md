# Klaflow

A hands-on emulation of Klaviyo's production architecture: real-time customer segmentation, per-customer feature vectors, and a contextual bandit decision engine. Runs entirely on local Kubernetes (kind), managed by Terraform.

```
                        ┌─────────────────────────────────────────────────────────┐
                        │                                                         │
  Producer ──► Kafka ──►│  Track 1: Flink Aggregation (fan-out counters)          │
   (Avro)    (Schema    │      Job 1: customer_id keyed ──► customer_counters     │
              Registry) │      Job 2: account_id keyed  ──► account_metrics       │
                        │                                                         │
                        │  Track 2: Segment Evaluation (SIP pattern)              │
                        │      Scan changed profiles ──► Evaluate conditions      │
                        │      ──► segment_membership                             │
                        │                                                         │
                        │  Feature Store: ClickHouse ──► Redis (<1ms reads)       │
                        │  ML Scoring: CLV tier, churn risk, discount sensitivity │
                        │  Decision Engine: Thompson Sampling contextual bandit   │
                        │                                                         │
                        │  API: 17 FastAPI endpoints over all of the above        │
                        │  UI:  React dashboard (profiles, segments, decisions)   │
                        └─────────────────────────────────────────────────────────┘
```

---

## Table of Contents

- [Architecture](#architecture)
- [Stack](#stack)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Running the Pipeline](#running-the-pipeline)
- [Frontend UI](#frontend-ui)
- [Exploring the System](#exploring-the-system)
  - [API](#1-api--what-users-see)
  - [ClickHouse](#2-clickhouse--the-query-layer)
  - [Kafka](#3-kafka--event-bus)
  - [Redis](#4-redis--feature-store)
  - [Prometheus + Grafana](#5-prometheus--grafana)
  - [Terraform](#6-terraform--infrastructure)
- [Data Model](#data-model)
- [Repo Structure](#repo-structure)
- [Running Tests](#running-tests)

---

## Architecture

Klaflow has **two parallel processing tracks** and a **decision layer** that sits downstream:

### Track 1 — Event Aggregation

Answers: *"What has this customer done, across how many dimensions, in each time window?"*

One incoming event fans out to many counter updates across multiple dimensions: `event_type x timeframe x campaign_id x product_category`. A single `email_opened` event for campaign X by customer Y updates counters like `email_opened_7d`, `email_opened_30d`, `email_opened_campaign_X_7d`, `total_events_1h`, etc.

Time windows: **1h, 24h, 3d, 7d, 30d** — an event only increments counters for windows it falls within. An event from 10 days ago increments `_30d` counters but NOT `_7d`.

Two isolated Flink jobs:
- **Job 1** (customer aggregation): keyed by `customer_id`, writes to `customer_counters`
- **Job 2** (account aggregation): keyed by `account_id`, writes to `account_metrics`

They are separate so an operational incident in one cannot impact the other.

### Track 2 — Segment Evaluation (SIP Pattern)

Answers: *"Which segment does this customer belong to right now?"*

Uses a two-phase approach (Klaviyo's Segmentation Improvement Project pattern):

1. **Scan** (lightweight): find profiles with counters updated in the last 10 minutes
2. **Evaluate** (heavy): check all segment conditions for those profiles, write to `segment_membership`

This separation is what makes time-based segment conditions tractable at scale.

### Decision Layer

Sits downstream of the feature store. For a given customer:

1. Fetch pre-computed feature vector from Redis (~1ms)
2. Score each arm (action) using a linear model
3. Apply Thompson Sampling to balance exploration vs. exploitation
4. Log the decision; a background job closes the reward loop

---

## Stack

| Layer                | Technology                           | K8s Namespace |
|----------------------|--------------------------------------|---------------|
| Event streaming      | Apache Kafka 3.7 (Bitnami Helm)      | `streaming`   |
| Schema enforcement   | Confluent Schema Registry (Avro)     | `streaming`   |
| Customer aggregation | Flink / PyFlink (Job 1)              | `processing`  |
| Account aggregation  | Flink / PyFlink (Job 2)              | `processing`  |
| Batch aggregation    | confluent_kafka (non-Flink variant)  | `processing`  |
| Segment evaluation   | Python worker (SIP pattern)          | `processing`  |
| Analytics store      | ClickHouse (ReplacingMergeTree)      | `analytics`   |
| Feature store        | Redis                                | `features`    |
| ML scoring           | Python FastAPI service               | `decisions`   |
| Decision engine      | Thompson Sampling bandit             | `decisions`   |
| Query API            | FastAPI (17 endpoints)               | `api`         |
| Frontend UI          | React 18 + TypeScript + Tailwind     | `ui`          |
| Observability        | Prometheus + Grafana                 | `monitoring`  |
| Infrastructure       | Terraform (kubernetes + helm)        | —             |
| Kubernetes           | kind                                 | —             |

---

## Prerequisites

```bash
brew install kind kubectl terraform
# Docker Desktop must be running
python3 --version  # 3.9+
```

## Quick Start

```bash
# 1. Create kind cluster + verify prerequisites
./scripts/setup.sh

# 2. Build images, deploy all infrastructure via Terraform
./scripts/deploy.sh

# 3. Tear down everything when done
./scripts/teardown.sh
```

---

## Running the Pipeline

After `deploy.sh` completes, run the pipeline steps in order:

### Step 1: Seed events into Kafka

```bash
# Build and load the producer image
docker build -f docker/producer/Dockerfile -t klaflow-producer . && \
kind load docker-image klaflow-producer:latest --name klaflow-us

# Run seed mode: generates 90 days of events for 18,500 customers across 5 merchants
kubectl -n processing run producer-seed \
  --image=klaflow-producer:latest \
  --image-pull-policy=Never \
  --restart=Never \
  --overrides='{
    "spec":{"containers":[{
      "name":"producer-seed",
      "image":"klaflow-producer:latest",
      "imagePullPolicy":"Never",
      "env":[
        {"name":"KAFKA_BOOTSTRAP","value":"kafka.streaming.svc.cluster.local:9092"},
        {"name":"SCHEMA_REGISTRY_URL","value":"http://schema-registry.streaming.svc.cluster.local:8081"}
      ],
      "command":["python","producer.py","--mode","seed"]
    }]}
  }'

# Watch progress (~5M events, takes a few minutes)
kubectl -n processing logs -f producer-seed
```

### Step 2: Run batch aggregation (fan-out counters)

```bash
docker build -f docker/batch_aggregation/Dockerfile -t klaflow-batch-aggregation . && \
kind load docker-image klaflow-batch-aggregation:latest --name klaflow-us

kubectl -n processing run batch-agg \
  --image=klaflow-batch-aggregation:latest \
  --image-pull-policy=Never \
  --restart=Never \
  --overrides='{
    "spec":{"containers":[{
      "name":"batch-agg",
      "image":"klaflow-batch-aggregation:latest",
      "imagePullPolicy":"Never",
      "env":[
        {"name":"KAFKA_BOOTSTRAP","value":"kafka.streaming.svc.cluster.local:9092"},
        {"name":"KAFKA_TOPIC","value":"customer-events"},
        {"name":"SCHEMA_REGISTRY_URL","value":"http://schema-registry.streaming.svc.cluster.local:8081"},
        {"name":"CLICKHOUSE_HOST","value":"clickhouse.analytics.svc.cluster.local"},
        {"name":"CLICKHOUSE_PORT","value":"8123"},
        {"name":"CLICKHOUSE_USER","value":"klaflow"},
        {"name":"CLICKHOUSE_PASSWORD","value":"klaflow-pass"}
      ],
      "command":["python","batch_aggregation.py"]
    }]}
  }'

kubectl -n processing logs -f batch-agg
```

### Step 3: Run feature writer + ML scoring

The feature writer reads counters from ClickHouse, computes derived features (open rates,
lifecycle state), calls the ML scoring service for CLV tier / churn risk / discount sensitivity,
and writes the results to both **Redis** (for the decision engine) and **ClickHouse** (for
segment evaluation). This is what populates the ML-dependent segments (`at_risk`, `vip_lapsed`,
`discount_sensitive`).

```bash
docker build -f docker/feature_store/Dockerfile -t klaflow-feature-store . && \
kind load docker-image klaflow-feature-store:latest --name klaflow-us

kubectl -n processing run feature-writer \
  --image=klaflow-feature-store:latest \
  --image-pull-policy=Never \
  --restart=Never \
  --overrides='{
    "spec":{"containers":[{
      "name":"feature-writer",
      "image":"klaflow-feature-store:latest",
      "imagePullPolicy":"Never",
      "env":[
        {"name":"CLICKHOUSE_HOST","value":"clickhouse.analytics.svc.cluster.local"},
        {"name":"CLICKHOUSE_PORT","value":"8123"},
        {"name":"CLICKHOUSE_USER","value":"klaflow"},
        {"name":"CLICKHOUSE_PASSWORD","value":"klaflow-pass"},
        {"name":"REDIS_HOST","value":"redis-master.features.svc.cluster.local"},
        {"name":"REDIS_PORT","value":"6379"},
        {"name":"ML_SCORING_URL","value":"http://ml-scoring.decisions.svc.cluster.local:8000"}
      ],
      "command":["python","feature_writer.py","--full"]
    }]}
  }'

kubectl -n processing logs -f feature-writer
# Expected: "Found 18248 profiles to update" → "Updated 18248/18248 feature vectors"
```

What it computes per customer:

| Feature | Source | How |
|---------|--------|-----|
| `email_open_rate_7d/30d` | ClickHouse counters | opens / (opens + clicks) |
| `purchase_count_30d` | ClickHouse counters | `purchase_made_30d` |
| `avg_order_value` | ClickHouse counters | `purchase_amount_30d / purchase_count_30d` |
| `cart_abandon_rate_30d` | ClickHouse counters | `cart_abandoned_30d` |
| `lifecycle_state` | Derived | new/active/at_risk/lapsed/churned based on recency |
| `clv_tier` | ML scoring service | low/medium/high/vip based on purchase value |
| `churn_risk_score` | ML scoring service | 0.0–1.0 based on inactivity signals |
| `discount_sensitivity` | ML scoring service | ratio of campaign-attributed purchases |

The **ML scoring service** (`src/ml_models/score_customers.py`) runs as a FastAPI deployment
in the `decisions` namespace. It exposes `POST /score` and applies three toy models:

- **CLV tier**: `purchase_count_30d × avg_order_value` → bracket into low/medium/high/vip
- **Churn risk**: additive score from days_since_last_purchase, low open rates, no recent purchases
- **Discount sensitivity**: ratio of purchases that had a campaign_id attached

### Step 4: Run segment evaluation

Now that ML-derived metrics (`clv_tier`, `days_since_last_purchase`, `churn_risk_score`,
`discount_sensitivity`) are written to ClickHouse, segment evaluation can populate all 7 segments.

```bash
docker build -f docker/segment_worker/Dockerfile -t klaflow-segment-worker . && \
kind load docker-image klaflow-segment-worker:latest --name klaflow-us

kubectl -n processing run segment-eval \
  --image=klaflow-segment-worker:latest \
  --image-pull-policy=Never \
  --restart=Never \
  --overrides='{
    "spec":{"containers":[{
      "name":"segment-eval",
      "image":"klaflow-segment-worker:latest",
      "imagePullPolicy":"Never",
      "env":[
        {"name":"CLICKHOUSE_HOST","value":"clickhouse.analytics.svc.cluster.local"},
        {"name":"CLICKHOUSE_PORT","value":"8123"},
        {"name":"CLICKHOUSE_USER","value":"klaflow"},
        {"name":"CLICKHOUSE_PASSWORD","value":"klaflow-pass"}
      ],
      "command":["python","segment_evaluator.py","--full"]
    }]}
  }'

kubectl -n processing logs -f segment-eval
```

Expected segment distribution after the full pipeline:

| Segment | Members | Description |
|---------|---------|-------------|
| `active_browsers` | ~16,000 | page_viewed_7d >= 5 |
| `high_engagers` | ~8,000 | email_opened_7d >= 3 OR link_clicked_7d >= 2 |
| `at_risk` | ~6,000 | days_since_last_purchase > 30, no recent purchases |
| `cart_abandoners` | ~5,000 | cart_abandoned_3d >= 1 |
| `recent_purchasers` | ~4,000 | purchase_made_7d >= 1 |
| `vip_lapsed` | ~10 | VIP tier + 60+ days inactive |
| `discount_sensitive` | 0 | No campaign-attributed purchases in synthetic data |

### Step 5: Query the results

```bash
kubectl port-forward svc/klaflow-api 8000:80 -n api &

# Segment summary
curl -s http://localhost:8000/segments | python3 -m json.tool

# Customer features (from Redis)
curl -s "http://localhost:8000/customers/merchant_001:cust_0001/features?account_id=merchant_001" | python3 -m json.tool

# ML scores
curl -s "http://localhost:8000/customers/merchant_001:cust_0001/scores?account_id=merchant_001" | python3 -m json.tool

# Account metrics
curl -s http://localhost:8000/accounts/merchant_001/metrics | python3 -m json.tool

# Bandit decision
curl -s -X POST http://localhost:8000/decide \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"merchant_001:cust_0001","account_id":"merchant_001"}' | python3 -m json.tool
```

---

## Frontend UI

A React dashboard that connects to the live FastAPI backend. Modeled on Klaviyo's production UI — clean, data-dense, professional SaaS design.

**Stack:** React 18, TypeScript, Tailwind CSS, React Router v6, TanStack Query, Recharts, Vite.

### Running locally (development)

```bash
cd src/ui
npm install
npm run dev
# → http://localhost:3001 (proxies /api to localhost:8000)
```

Make sure the API port-forward is active:

```bash
kubectl port-forward svc/klaflow-api 8000:80 -n api &
```

### Running on Kubernetes

```bash
# Build and load the UI image
docker build -f docker/ui/Dockerfile -t klaflow-ui:latest .
kind load docker-image klaflow-ui:latest --name klaflow-us

# Deploy via Terraform (terraform/modules/ui/)
cd terraform && terraform apply

# Access via port-forward
kubectl port-forward svc/klaflow-ui 3000:3000 -n ui &
open http://localhost:3000
```

### Pages

| Page | Route | Description |
|------|-------|-------------|
| **Dashboard** | `/dashboard` | 4 KPI cards (total profiles, active segments, events/24h, revenue/7d) + segment membership bar chart |
| **Profiles** | `/profiles` | Paginated table (50/page), searchable by customer ID. Shows lifecycle state (color-coded badge), CLV tier, churn risk (progress bar), segment pills. Click a row to open the profile detail. |
| **Customer Profile** | `/profiles/:id` | Left panel: customer properties, segment memberships, "Get Decision" button (calls `POST /decide` inline). Right panel with 4 tabs: Activity (counter history), Segments (in/out with evaluation timestamps), Decisions (history with reward outcomes), Features (full Redis feature vector with plain-English labels). |
| **Segments** | `/segments` | List with member counts, % of total, and human-readable condition summaries. Click through to segment detail. |
| **Segment Detail** | `/segments/:name` | Condition definition, member count, percentage, and paginated member table with links to customer profiles. |
| **Decisions** | `/decisions` | Bandit operator view: arm performance table (alpha/beta/expected Thompson), arm distribution pie chart, live decision feed (auto-refreshes every 15s). |
| **Flows** | `/flows` | Stub — placeholder cards for Welcome Series, Abandoned Cart, Win-Back flows. Phase 2 banner. |
| **Campaigns** | `/campaigns` | Stub — Phase 2 banner. |

### Design

- **Sidebar:** Deep navy (`#1B2559`) with white text, persistent navigation
- **Accent:** Klaviyo green (`#27AE60`)
- **Font:** Inter (Google Fonts)
- **Tables:** Zebra striping, hover highlight, sticky headers
- **Loading:** Skeleton screens (shimmer animation), not spinners
- **Empty states:** Descriptive message, not blank
- **Errors:** Inline error with retry button
- **Transitions:** 150ms ease

### API endpoints added for the UI

These endpoints were added to `src/api/main.py` alongside the original 10:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/dashboard/stats` | GET | KPI aggregates: total profiles, active segments, events in last 24h, revenue |
| `/customers` | GET | Paginated customer list with feature data from Redis. Query params: `page`, `page_size`, `search` |
| `/customers/{id}/events` | GET | Counter history for a customer from ClickHouse (paginated) |
| `/customers/{id}/all-segments` | GET | All segment evaluations (both in-segment and not-in-segment) |
| `/segments/{name}/detail` | GET | Segment metadata: member count, percentage, condition definition |
| `/decisions/recent` | GET | Recent decisions across all customers for the live feed |

CORS middleware is enabled for frontend development (`allow_origins=["*"]`).

---

## Exploring the System

### 1. API — What users see

```bash
kubectl port-forward svc/klaflow-api 8000:80 -n api &
```

**Segment queries** (read from ClickHouse `segment_membership`):

```bash
# All segments with member counts
curl -s http://localhost:8000/segments | python3 -m json.tool

# Customers in a specific segment
curl -s "http://localhost:8000/segments/high_engagers/customers?limit=10" | python3 -m json.tool

# Which segments a specific customer belongs to
curl -s http://localhost:8000/customers/merchant_001:cust_0001/segments | python3 -m json.tool
```

**Feature store** (read from Redis):

```bash
# Full feature vector for a customer
curl -s "http://localhost:8000/customers/merchant_001:cust_0001/features?account_id=merchant_001" | python3 -m json.tool

# ML scores only (CLV tier, churn risk, discount sensitivity)
curl -s "http://localhost:8000/customers/merchant_001:cust_0001/scores?account_id=merchant_001" | python3 -m json.tool
```

**Decision engine** (Thompson Sampling bandit):

```bash
# Get a bandit decision for a customer
curl -s -X POST http://localhost:8000/decide \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"merchant_001:cust_0001","account_id":"merchant_001"}' | python3 -m json.tool

# Past decisions and outcomes
curl -s "http://localhost:8000/decisions/merchant_001:cust_0001/history" | python3 -m json.tool

# Current arm weights and win rates
curl -s http://localhost:8000/bandit/arm-stats | python3 -m json.tool
```

**Account metrics** (read from ClickHouse `account_metrics`):

```bash
curl -s http://localhost:8000/accounts/merchant_001/metrics | python3 -m json.tool
```

**Health check:**

```bash
curl -s http://localhost:8000/health
```

Source: `src/api/main.py`

### 2. ClickHouse — The query layer

ClickHouse stores **aggregated counters**, not raw events. This is the critical distinction from a naive Kafka-to-ClickHouse pipeline.

```bash
# Open a ClickHouse client
kubectl -n analytics exec -it svc/clickhouse -- \
  clickhouse-client --user klaflow --password klaflow-pass
```

**Tables** (defined in `src/clickhouse/schema.sql`):

```sql
SHOW TABLES;
-- customer_counters     ← fan-out counters from aggregation (Track 1)
-- segment_membership    ← segment evaluation results (Track 2)
-- account_metrics       ← per-merchant metrics (Job 2)
-- decision_log          ← bandit decisions
-- reward_log            ← reward closure outcomes

-- See what windowed counters look like for a customer
SELECT metric_name, metric_value
FROM customer_counters FINAL
WHERE customer_id = 'merchant_001:cust_0001'
ORDER BY metric_name;

-- Check which metrics exist and how many customers have each
SELECT metric_name, count() AS customers
FROM customer_counters FINAL
GROUP BY metric_name
ORDER BY customers DESC
LIMIT 20;

-- Segment membership summary
SELECT segment_name,
       countIf(in_segment = 1) AS members,
       count() AS evaluated
FROM segment_membership FINAL
GROUP BY segment_name
ORDER BY members DESC;

-- See a customer's segment memberships
SELECT segment_name, in_segment, evaluated_at
FROM segment_membership FINAL
WHERE customer_id = 'merchant_001:cust_0001';

-- Account metrics
SELECT * FROM account_metrics FINAL
WHERE account_id = 'merchant_001'
ORDER BY metric_name;
```

All tables use `ReplacingMergeTree` — duplicate writes are deduplicated by the version column (`computed_at` or `evaluated_at`). Use `FINAL` in queries to get deduplicated results.

### 3. Kafka — Event bus

```bash
# Exec into a Kafka pod
kubectl -n streaming exec -it kafka-controller-0 -- bash

# List topics
/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Check consumer groups and their lag
/opt/bitnami/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
/opt/bitnami/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group klaflow-batch-aggregation-v5

# Peek at messages (Ctrl+C to stop) — these are Avro-encoded, not human-readable
/opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic customer-events --from-beginning --max-messages 5

# Topic details
/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic customer-events
```

**Schema Registry:**

```bash
kubectl port-forward svc/schema-registry 8081:8081 -n streaming &

# List registered schemas
curl -s http://localhost:8081/subjects | python3 -m json.tool

# View the Avro schema
curl -s http://localhost:8081/subjects/customer-events-value/versions/latest | python3 -m json.tool
```

Source: Avro schema at `src/producer/schemas/customer_event.avsc`

### 4. Redis — Feature store

Pre-computed feature vectors for sub-millisecond reads at decision time. ClickHouse is too slow for per-customer synchronous lookups.

```bash
# Exec into Redis
kubectl -n features exec -it svc/redis-master -- redis-cli

# List some customer keys
KEYS customer:merchant_001:*
# (shows customer:{account_id}:{customer_id} keys)

# View a customer's full feature vector
HGETALL customer:merchant_001:merchant_001:cust_0001

# Check specific fields
HGET customer:merchant_001:merchant_001:cust_0001 clv_tier
HGET customer:merchant_001:merchant_001:cust_0001 churn_risk_score

# Count total feature vectors
DBSIZE

# Check TTL (features expire after 48 hours)
TTL customer:merchant_001:merchant_001:cust_0001
```

Feature vector fields (see `src/feature_store/feature_schema.py`):

| Field | Type | Description |
|-------|------|-------------|
| `email_open_rate_7d` | float | opens / sends in last 7d |
| `email_open_rate_30d` | float | opens / sends in last 30d |
| `purchase_count_30d` | int | purchases in last 30d |
| `days_since_last_purchase` | int | days since last purchase (999 = never) |
| `avg_order_value` | float | average order value |
| `cart_abandon_rate_30d` | float | cart abandonment rate |
| `preferred_send_hour` | int | 0-23, learned from open timestamps |
| `lifecycle_state` | string | new, active, at_risk, lapsed, churned |
| `clv_tier` | string | low, medium, high, vip |
| `churn_risk_score` | float | 0.0–1.0 |
| `discount_sensitivity` | float | 0.0–1.0 |
| `updated_at` | epoch_ms | last update timestamp |

### 5. Prometheus + Grafana

```bash
# Grafana (login: admin / klaflow)
kubectl port-forward svc/kube-prometheus-stack-grafana 3000:80 -n monitoring &
open http://localhost:3000

# Prometheus
kubectl port-forward svc/kube-prometheus-stack-prometheus 9090:9090 -n monitoring &
open http://localhost:9090
```

Useful Prometheus queries:
- `up` — which targets are being scraped
- `container_memory_usage_bytes{namespace="analytics"}` — ClickHouse memory
- `container_memory_usage_bytes{namespace="streaming"}` — Kafka memory
- `rate(container_cpu_usage_seconds_total{namespace="processing"}[5m])` — processing CPU

### 6. Terraform — Infrastructure

```bash
cd terraform

# See what Terraform manages
terraform state list

# Inspect a specific resource
terraform state show module.kafka.helm_release.kafka
terraform state show module.clickhouse.helm_release.clickhouse

# See what would change
terraform plan
```

Module dependency graph:
```
kafka (streaming)
clickhouse (analytics)        ──► decisions (depends on clickhouse + redis)
redis (features)              ──► api (depends on clickhouse + redis)
flink (processing)                  ──► ui (standalone, proxies to api)
observability (monitoring)
```

Source: `terraform/main.tf` and `terraform/modules/*/main.tf`

---

## Data Model

### Event Schema (Avro)

Every event entering Kafka is validated against `src/producer/schemas/customer_event.avsc`:

```json
{
  "event_id":      "uuid",
  "customer_id":   "merchant_001:cust_0042",
  "account_id":    "merchant_001",
  "event_type":    "email_opened | link_clicked | purchase_made | page_viewed | cart_abandoned",
  "timestamp":     1700000000000,
  "properties": {
    "campaign_id":       "camp_001 (nullable)",
    "product_id":        "prod_123 (nullable)",
    "product_category":  "electronics (nullable)",
    "amount":            99.99,
    "email_subject":     "Your order (nullable)"
  }
}
```

### ClickHouse Tables

| Table | Engine | Written By | Purpose |
|-------|--------|------------|---------|
| `customer_counters` | ReplacingMergeTree | Flink Job 1 / batch agg | Windowed counters per customer per dimension |
| `segment_membership` | ReplacingMergeTree | Segment evaluator | Which segment each customer belongs to |
| `account_metrics` | ReplacingMergeTree | Flink Job 2 / batch agg | Per-merchant aggregate metrics |
| `decision_log` | MergeTree | Decision engine | Every bandit decision made |
| `reward_log` | MergeTree | Reward closure job | Observed outcomes for decisions |

### Segments

| Segment | Window | Condition |
|---------|--------|-----------|
| `high_engagers` | 7d | `email_opened_7d >= 3 OR link_clicked_7d >= 2` |
| `recent_purchasers` | 7d | `purchase_made_7d >= 1` |
| `at_risk` | 30d | `days_since_last_purchase > 30 AND purchase_made_30d = 0` |
| `active_browsers` | 7d | `page_viewed_7d >= 5` |
| `cart_abandoners` | 3d | `cart_abandoned_3d >= 1` |
| `vip_lapsed` | 90d | `clv_tier = vip AND days_since_last_purchase > 60` |
| `discount_sensitive` | 30d | `discount_sensitivity >= 0.6` |

### Bandit Arms

| Arm | Description |
|-----|-------------|
| `email_no_offer` | Standard email, no incentive |
| `email_10pct_discount` | Email with 10% discount |
| `email_free_shipping` | Email with free shipping offer |
| `sms_nudge` | SMS message |
| `no_send` | Suppress — don't contact |

Reward signal: +1.0 purchase within 72h, +0.1 email opened, -0.5 unsubscribed, 0.0 no action.

### Synthetic Data

5 merchant accounts, 18,500 total customers:

| Account | Customers | Profile |
|---------|-----------|---------|
| merchant_001 | 1,000 | Small |
| merchant_002 | 5,000 | Mid-market |
| merchant_003 | 500 | Niche |
| merchant_004 | 10,000 | Large |
| merchant_005 | 2,000 | Mid-market |

Event rates per customer per day (Poisson): page_viewed (λ=3), email_opened (λ=0.4), cart_abandoned (λ=0.15), link_clicked (λ=0.1), purchase_made (λ=0.05).

Customer lifecycle: `new → active → at_risk → lapsed → churned` with weekly transition probabilities. Lifecycle state affects event generation rates (churned customers produce ~0 events).

---

## Repo Structure

```
klaflow/
├── terraform/
│   ├── providers.tf                        # K8s + Helm providers → kind-klaflow-us
│   ├── main.tf                             # Wires all modules together
│   └── modules/
│       ├── kafka/                           # Kafka + Schema Registry
│       ├── clickhouse/                      # ClickHouse + schema ConfigMap
│       ├── flink/                           # Flink Kubernetes Operator
│       ├── redis/                           # Redis standalone
│       ├── observability/                   # Prometheus + Grafana
│       ├── decisions/                       # ML scoring + reward logger
│       ├── api/                             # FastAPI deployment
│       └── ui/                              # React frontend (Deployment + Service)
├── src/
│   ├── producer/
│   │   ├── producer.py                      # Synthetic events: seed (90 days) or continuous
│   │   └── schemas/customer_event.avsc      # Avro schema (event contract)
│   ├── flink_jobs/
│   │   ├── customer_aggregation_job.py      # Job 1: per-customer fan-out counters (PyFlink)
│   │   ├── account_aggregation_job.py       # Job 2: per-account metrics (PyFlink)
│   │   └── batch_aggregation.py             # Batch variant (confluent_kafka, no Flink)
│   ├── segment_worker/
│   │   └── segment_evaluator.py             # SIP two-phase: scan changed → evaluate rules
│   ├── feature_store/
│   │   ├── feature_writer.py                # ClickHouse → compute features → Redis
│   │   └── feature_schema.py                # CustomerFeatureVector dataclass
│   ├── ml_models/
│   │   └── score_customers.py               # CLV tier, churn risk, discount sensitivity
│   ├── decision_engine/
│   │   ├── bandit.py                        # Thompson Sampling contextual bandit
│   │   ├── arms.py                          # 5 arms + reward constants
│   │   └── reward_logger.py                 # Background reward closure job
│   ├── api/
│   │   └── main.py                          # 17 FastAPI endpoints (10 original + 7 UI)
│   ├── ui/                                  # React frontend
│   │   ├── src/
│   │   │   ├── api/client.ts                # TanStack Query API client
│   │   │   ├── components/                  # Layout, Badge, Skeleton, ErrorState
│   │   │   └── pages/                       # Dashboard, Profiles, CustomerProfile,
│   │   │                                    #   Segments, SegmentDetail, Decisions,
│   │   │                                    #   Flows, Campaigns
│   │   ├── index.html
│   │   ├── vite.config.ts                   # Dev server + API proxy
│   │   ├── tailwind.config.js
│   │   └── package.json
│   └── clickhouse/
│       └── schema.sql                       # 5 table definitions
├── docker/                                  # Dockerfiles for each service
├── scripts/
│   ├── setup.sh                             # Prerequisites + kind cluster creation
│   ├── deploy.sh                            # Build images + terraform apply
│   └── teardown.sh                          # Terraform destroy + kind delete
└── tests/                                   # 170 pytest tests across all layers
```

## Running Tests

```bash
pip install pytest fastapi httpx redis requests numpy
python3 -m pytest tests/ -v
```

Tests mock external dependencies (Kafka, ClickHouse, Redis, PyFlink) and validate business logic: fan-out counters, segment rules, feature vector computation, bandit decisions, and API endpoints.

## Data Flow (End-to-End)

```
1. Producer generates synthetic customer events (Avro-validated via Schema Registry)
2. Events land in Kafka topic: customer-events
3. Batch aggregation (or Flink Job 1) reads events, computes windowed fan-out counters
     1 event → N counters (event_type x window x campaign x category)
     Only windows the event falls within are updated (7-day-old event → 30d only, not 7d)
4. Counters written to ClickHouse customer_counters table
5. Feature writer reads counters from ClickHouse, computes derived features
     (open rates, lifecycle state), calls ML scoring service (CLV tier, churn risk,
     discount sensitivity), writes feature vectors to Redis (48h TTL) AND writes
     ML-derived metrics back to ClickHouse (so segments can query them)
6. Segment evaluator scans for recently updated profiles (Phase 1)
     Then evaluates 7 segment conditions against windowed counters + ML metrics (Phase 2)
     Results written to ClickHouse segment_membership table
7. Decision engine reads feature vector from Redis, scores 5 arms,
     applies Thompson Sampling, logs decision to ClickHouse
8. Reward closure job checks outcomes (purchases within 72h), writes to reward_log
9. API serves queries over all of the above — segments, features, decisions, metrics
10. React UI connects to the API and presents dashboards, profile views,
      segment management, and a bandit operator console
```

The key ordering dependency: **feature writer must run before segment evaluation** for ML-dependent
segments (`at_risk`, `vip_lapsed`, `discount_sensitive`) to have non-zero members. The feature
writer writes `clv_tier`, `days_since_last_purchase`, `churn_risk_score`, and `discount_sensitivity`
back to `customer_counters` in ClickHouse, which the segment evaluator then queries.
