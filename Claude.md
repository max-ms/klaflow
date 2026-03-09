# Klaflow — Klaviyo Architecture Emulation

## What this project is

Klaflow is a hands-on emulation of Klaviyo's actual production architecture,
built to understand how a real-time segmentation and per-customer decisioning
platform works at scale. It emulates the specific architectural choices Klaviyo
made — not a generic event pipeline.

Built in two phases:

- **Phase 1**: Single-region pipeline — Klaviyo's core event → segment → decision loop
- **Phase 2**: Multi-region split — data residency, async replication, segment divergence

---

## Architecture overview (what we're emulating and why)

### The two-track design

Klaviyo's architecture has two separate tracks that run in parallel. Understanding
why they're separate is the key architectural insight:

**Track 1 — Event Aggregation (Kafka → Flink → RocksDB)**
Answers: "What has this customer done, across how many dimensions, in each time window?"
This is the counting system. One incoming event fans out to hundreds of dimension
counters per customer (event type × timeframe × campaign × product category...).
This produces hundreds of thousands of writes per second from relatively few input events.
Flink was chosen because it manages state internally (RocksDB-backed), which is
critical for idempotency at this fan-out cardinality. External state management
(e.g. writing to Cassandra per update) caused the data accuracy issues Klaviyo
moved away from.

**Track 2 — Segment Evaluation (ClickHouse)**
Answers: "Which segment does this customer belong to right now?"
This is the query layer. It does NOT receive a direct Flink sink stream.
Instead, aggregated counters from Track 1 are materialized into ClickHouse as
profile properties. ClickHouse then evaluates segment conditions (especially
time-based ones: "opened email 3–5 days ago") across the full profile dataset.
The SIP (Segmentation Improvement Project) insight: separate the lightweight
"which profiles need re-evaluation" scan from the heavy "re-evaluate these profiles"
worker pool. This is what makes tens of billions of segment membership changes
per day tractable.

**The two-tier Flink job separation:**
Klaviyo runs two separate Flink jobs — one for customer-level aggregations
(per-shopper behavioral counters) and one for account-level aggregations
(per-merchant billing, rate-limiting). They are isolated so an operational
incident in one cannot impact the other. We emulate this separation.

**The decision layer (new in this emulation vs. baseline Klaflow):**
Sits downstream of segment evaluation. Reads pre-computed customer feature
vectors from Redis and scores candidate actions (email variant A, email variant B,
SMS, no-send) using a simple scoring model. This is the contextual bandit layer
— the part Klaviyo is building toward. Even a toy version makes the architecture
complete end-to-end.

---

## Stack

| Layer                  | Technology                              | Namespace    |
|------------------------|-----------------------------------------|--------------|
| Event streaming        | Apache Kafka (Bitnami Helm)             | streaming    |
| Schema enforcement     | Confluent Schema Registry (Avro)        | streaming    |
| Customer aggregation   | Apache Flink / PyFlink (job-1)          | processing   |
| Account aggregation    | Apache Flink / PyFlink (job-2)          | processing   |
| Segment evaluation     | ClickHouse (192-node emulated via sharding) | analytics |
| Feature store (hot)    | Redis                                   | features     |
| ML scoring (toy)       | Python scoring service                  | decisions    |
| Decision engine        | FastAPI bandit service                  | decisions    |
| Observability          | Prometheus + Grafana                    | monitoring   |
| Query API              | FastAPI                                 | api          |
| Infrastructure         | Terraform (kubernetes + helm providers) | —            |
| Kubernetes             | kind                                    | —            |

---

## Repo structure

```
klaflow/
├── CLAUDE.md                            ← this file (read first, every session)
├── terraform/
│   ├── providers.tf                     ← kubernetes + helm providers → klaflow-us
│   ├── main.tf                          ← calls all modules
│   ├── variables.tf
│   ├── outputs.tf
│   └── modules/
│       ├── kafka/                       ← Kafka + Schema Registry
│       ├── flink/                       ← Flink cluster (both jobs deploy here)
│       ├── clickhouse/                  ← ClickHouse + schema
│       ├── redis/                       ← Feature store
│       ├── decisions/                   ← Bandit scoring service
│       ├── observability/               ← Prometheus + Grafana
│       └── api/                         ← FastAPI query layer
├── src/
│   ├── producer/
│   │   ├── producer.py                  ← emits synthetic customer events
│   │   ├── schemas/
│   │   │   └── customer_event.avsc      ← Avro schema
│   │   └── requirements.txt
│   ├── flink_jobs/
│   │   ├── customer_aggregation_job.py  ← job-1: per-customer counters
│   │   ├── account_aggregation_job.py   ← job-2: per-account/merchant counters
│   │   └── requirements.txt
│   ├── feature_store/
│   │   ├── feature_writer.py            ← reads ClickHouse, writes features to Redis
│   │   ├── feature_schema.py            ← defines the feature vector structure
│   │   └── requirements.txt
│   ├── ml_models/
│   │   ├── score_customers.py           ← toy CLV tier, churn risk, discount sensitivity
│   │   └── requirements.txt
│   ├── decision_engine/
│   │   ├── bandit.py                    ← Thompson Sampling contextual bandit
│   │   ├── arms.py                      ← action space definition
│   │   ├── reward_logger.py             ← logs decisions + outcomes for retraining
│   │   └── requirements.txt
│   ├── api/
│   │   ├── main.py                      ← FastAPI: query + decision endpoints
│   │   └── requirements.txt
│   └── clickhouse/
│       └── schema.sql                   ← ClickHouse table definitions
├── docker/
│   ├── producer/Dockerfile
│   ├── flink_jobs/Dockerfile
│   ├── feature_store/Dockerfile
│   ├── ml_models/Dockerfile
│   ├── decision_engine/Dockerfile
│   └── api/Dockerfile
├── scripts/
│   ├── setup.sh                         ← prerequisite check + kind cluster creation
│   ├── deploy.sh                        ← terraform init + apply
│   └── teardown.sh                      ← terraform destroy + kind delete
└── tests/
    ├── test_producer.py
    ├── test_aggregation.py
    ├── test_segmentation.py
    ├── test_feature_store.py
    ├── test_decision_engine.py
    └── test_api.py
```

---

## Event model

All events are Avro-validated against `customer_event.avsc` before entering Kafka.

```json
{
  "event_id":      "uuid",
  "customer_id":   "string",
  "account_id":    "string",
  "event_type":    "email_opened | link_clicked | purchase_made | page_viewed | cart_abandoned",
  "timestamp":     "long (epoch ms)",
  "properties": {
    "campaign_id":    "string (optional)",
    "product_id":     "string (optional)",
    "product_category": "string (optional)",
    "amount":         "double (optional)",
    "email_subject":  "string (optional)"
  }
}
```

Note `account_id` is required — it's what separates customer-level from
account-level aggregation. Every event belongs to both a customer and a merchant account.

---

## Flink Job 1: Customer Aggregation

**Purpose:** High-cardinality fan-out. One incoming event produces many counter updates
across multiple dimensions and time windows.

**Key behavior:**
- Keyed by `customer_id`
- For each event, fan out to: (event_type × timeframe × campaign_id × product_category)
- Time windows: 1h, 24h, 7d, 30d rolling counts
- State backend: RocksDB (embedded in Flink, not external)
- Output: customer profile property updates → ClickHouse `customer_counters` table

**Why this matters architecturally:**
The fan-out is what creates hundreds of thousands of writes per second from
100K events/second. A single "email_opened" event for campaign X by customer Y
updates: opened_1h, opened_24h, opened_7d, opened_30d, opened_campaign_X_7d,
opened_campaign_X_30d, engagement_score_delta, and more. The RocksDB state
backend handles the large aggregation windows (up to 30 days of state per customer).

**Emulation target:** Demonstrate the fan-out pattern. Even with synthetic events,
show that 1 input event → N ClickHouse writes, where N is the number of
dimension combinations tracked.

---

## Flink Job 2: Account Aggregation

**Purpose:** Per-merchant counters for billing, rate-limiting, and dashboard metrics.
Operationally isolated from Job 1.

**Key behavior:**
- Keyed by `account_id`
- Tracks: events_per_hour, sends_today, active_customers_7d, revenue_7d
- Output: account metric updates → ClickHouse `account_metrics` table

**Why isolated:**
An operational incident in account aggregation (e.g. a single large merchant
generating a spike) must not degrade customer-level aggregation, which is on
the critical path for segmentation. Separate jobs = separate resource pools
= separate failure domains.

---

## ClickHouse schema design

ClickHouse is NOT a direct Flink sink for raw events. It is an OLAP query layer
that receives aggregated, pre-computed values. This is the critical distinction
from a naive Kafka→ClickHouse pipeline.

**Tables:**

```sql
-- Aggregated customer counters (written by Flink Job 1)
CREATE TABLE customer_counters (
    account_id       String,
    customer_id      String,
    metric_name      String,   -- e.g. "email_opened_7d", "purchase_made_30d"
    metric_value     Float64,
    computed_at      DateTime
) ENGINE = ReplacingMergeTree(computed_at)
  PARTITION BY toYYYYMM(computed_at)
  ORDER BY (account_id, customer_id, metric_name);

-- Segment membership (written by segment evaluation workers)
CREATE TABLE segment_membership (
    account_id       String,
    customer_id      String,
    segment_name     String,
    in_segment       UInt8,
    evaluated_at     DateTime
) ENGINE = ReplacingMergeTree(evaluated_at)
  ORDER BY (account_id, customer_id, segment_name);

-- Account-level metrics (written by Flink Job 2)
CREATE TABLE account_metrics (
    account_id       String,
    metric_name      String,
    metric_value     Float64,
    computed_at      DateTime
) ENGINE = ReplacingMergeTree(computed_at)
  ORDER BY (account_id, metric_name);
```

**Sharding:** Emulate Klaviyo's bi-level sharding by partitioning on
`(account_id, customer_id)`. In production this is a 192-node cluster;
here we simulate the sharding key logic even on a single node.

**The SIP query pattern:**
Segment evaluation runs in two phases:
1. Lightweight scan: `SELECT DISTINCT customer_id FROM customer_counters
   WHERE computed_at > now() - INTERVAL 1 HOUR` — finds profiles that
   changed and need re-evaluation
2. Heavy evaluation: worker pool picks up these profiles, evaluates all
   segment conditions against their current counters, writes results to
   `segment_membership`

This separation is what makes time-based segment conditions tractable.
"Opened email 3–5 days ago" requires constant re-evaluation as time passes —
the SIP pattern handles this without re-evaluating all profiles every time.

---

## Feature store (Redis)

Pre-computed customer feature vectors, maintained by the feature writer service.

**Why Redis and not ClickHouse directly:**
The decision engine needs to score 100K customers in ~1 second. ClickHouse
query latency (even fast) is too high for per-customer synchronous lookup
at decision time. Redis gives <1ms per customer read.

**Feature vector per customer (stored as Redis hash):**

```
customer:{account_id}:{customer_id} → {
    email_open_rate_7d:        float,   # opens / sends in last 7d
    email_open_rate_30d:       float,
    purchase_count_30d:        int,
    days_since_last_purchase:  int,
    avg_order_value:           float,
    cart_abandon_rate_30d:     float,
    lifecycle_state:           string,  # "new|active|at_risk|lapsed|churned"
    clv_tier:                  string,  # "low|medium|high|vip"
    churn_risk_score:          float,   # 0.0 – 1.0, from ML scoring service
    discount_sensitivity:      float,   # 0.0 – 1.0, from ML scoring service
    preferred_send_hour:       int,     # 0–23, learned from open timestamps
    updated_at:                epoch_ms
}
```

**Feature writer service:**
- Runs on a 5-minute polling loop
- Reads latest `customer_counters` from ClickHouse for recently updated profiles
- Computes derived features (open rate = opens / sends)
- Calls ML scoring service for CLV tier, churn risk, discount sensitivity
- Writes updated feature vector to Redis with TTL of 48h

---

## ML scoring service (toy models)

Computes the three scores that cannot be derived from simple event counts.
These are simplified deterministic models — the goal is architectural correctness
(scores exist as profile properties, feed the decision engine), not ML accuracy.

**CLV tier:** based on purchase_count_30d × avg_order_value bracket
- < $50 lifetime → "low"
- $50–$500 → "medium"
- $500–$2000 → "high"
- > $2000 → "vip"

**Churn risk score (0.0–1.0):**
```
score = 0.0
if days_since_last_purchase > 90: score += 0.4
if days_since_last_purchase > 180: score += 0.3
if email_open_rate_7d < 0.1: score += 0.2
if purchase_count_30d == 0 and clv_tier in ["high","vip"]: score += 0.1
return min(score, 1.0)
```

**Discount sensitivity (0.0–1.0):**
```
# Proxy: customers who only purchased during known discount campaigns
# are flagged as discount-sensitive
discount_purchase_ratio = purchases_with_campaign_id / total_purchases
return discount_purchase_ratio  # simple proxy, not a real model
```

These scores are written to the feature store and appear as first-class
customer properties available to segment conditions and the decision engine.

---

## Segmentation logic

Segment conditions are evaluated against `customer_counters` in ClickHouse.
Segment membership is written to `segment_membership`.

| Segment              | Window | Definition                                                       |
|----------------------|--------|------------------------------------------------------------------|
| `high_engagers`      | 7d     | email_open_rate_7d >= 0.3 OR link_clicked_7d >= 2               |
| `recent_purchasers`  | 7d     | purchase_count_7d >= 1                                           |
| `at_risk`            | 7d     | days_since_last_purchase > 30 AND purchase_count_30d == 0        |
| `active_browsers`    | 7d     | page_viewed_7d >= 5                                              |
| `cart_abandoners`    | 3d     | cart_abandon_rate_3d >= 1                                        |
| `vip_lapsed`         | 90d    | clv_tier == "vip" AND days_since_last_purchase > 60              |
| `discount_sensitive` | 30d    | discount_sensitivity >= 0.6                                      |

Note: `vip_lapsed` and `discount_sensitive` use ML-derived scores, demonstrating
that segment conditions can be built on top of model outputs — not just raw event counts.

---

## Decision engine (contextual bandit)

The layer Klaviyo is building toward. Sits downstream of the feature store.

**Action space (arms) — pre-defined, not dynamically generated:**

```python
ARMS = [
    "email_no_offer",
    "email_10pct_discount",
    "email_free_shipping",
    "sms_nudge",
    "no_send",
]
```

**Reward signal:**
- +1.0 if purchase within 72h of send
- +0.1 if email opened (weak signal)
- -0.5 if unsubscribed (negative, protect list health)
- 0 otherwise (no action observed)

**Decision flow per customer:**
1. Fetch feature vector from Redis (~1ms)
2. Score each arm using a simple linear model:
   `score(arm, features) = weights[arm] · features`
3. Apply Thompson Sampling: sample from Beta(α, β) per arm,
   select arm with highest sample
4. Write decision to `decision_log` table in ClickHouse:
   `(customer_id, arm_chosen, feature_vector_snapshot, timestamp)`
5. Push action to execution queue (Kafka topic: `send-decisions`)

**Reward closure:**
A separate background process joins `decision_log` with `customer_counters`
(purchases within 72h) to compute actual rewards and write to `reward_log`.
This is the training data for the next model update cycle.

**Model update (toy retraining):**
On a daily schedule, refit the linear weight vectors using reward_log data.
Weight recent observations 2x vs older (simple recency weighting).
Log old vs new weights to demonstrate that the policy is updating.

**API endpoint:**
```
POST /decide
Body: { "customer_id": "...", "account_id": "...", "context": "cart_abandoned" }
Returns: { "arm": "email_10pct_discount", "score": 0.73, "reasoning": {...} }
```

---

## API endpoints (full set)

```
# Segmentation queries (over ClickHouse)
GET  /segments                                  → all segments with member counts
GET  /segments/{segment_name}/customers         → customers in a segment
GET  /customers/{customer_id}/segments          → which segments a customer belongs to
GET  /customers/{customer_id}/features          → full feature vector from Redis
GET  /customers/{customer_id}/scores            → ML scores (CLV tier, churn, discount)

# Decision engine
POST /decide                                    → get bandit decision for a customer
GET  /decisions/{customer_id}/history           → past decisions + outcomes
GET  /bandit/arm-stats                          → current arm weights + win rates per arm

# Account metrics
GET  /accounts/{account_id}/metrics             → events/sends/revenue for this merchant
```

---

## Observability

Prometheus scrapes:

**Kafka:** consumer lag per topic/partition, produce rate, consumer rate

**Flink Job 1 (customer):**
- events_ingested_per_second
- fan_out_ratio (writes_per_event — should be ~50–200x)
- checkpoint_duration_ms
- rocksdb_state_size_bytes

**Flink Job 2 (account):**
- account_events_per_second
- checkpoint_duration_ms

**ClickHouse:**
- query_latency_ms (segment evaluation queries)
- insert_rate (writes from Flink)
- segment_evaluation_duration_ms

**Feature store:**
- redis_write_rate
- feature_staleness_seconds (age of oldest feature vector)

**Decision engine:**
- decisions_per_second
- arm_selection_distribution (% of decisions per arm — watch for collapse)
- reward_rate_per_arm (actual outcomes per arm)
- policy_update_delta (weight change magnitude on each retrain)

Grafana has pre-built dashboards showing:
1. Pipeline health (end-to-end latency from event to segment membership)
2. Fan-out ratio (the key Flink metric)
3. Bandit arm distribution over time (do we see exploration?)
4. Reward rates per arm (is the policy learning?)

---

## Build order (Phase 1)

Build one step at a time. Smoke test each before moving to the next.

1.  Scaffold repo structure (empty placeholder files matching layout above)
    → Run: `kind create cluster --name klaflow-us` if not already created
2.  `terraform/providers.tf` + `main.tf` — connect to kind cluster `klaflow-us`
3.  Kafka + Schema Registry — Bitnami Helm chart, namespace `streaming`
4.  ClickHouse — Altinity Helm chart, namespace `analytics`, with `schema.sql`
5.  Redis — Bitnami Helm chart, namespace `features`
6.  Flink cluster — official Helm chart, namespace `processing`
7.  Prometheus + Grafana — kube-prometheus-stack, namespace `monitoring`
8.  Avro schema — `customer_event.avsc` (must include `account_id` field)
9.  Python producer — synthetic events for N accounts × M customers each,
    Avro-validated via Schema Registry, realistic event distribution
10. Flink Job 1 — customer aggregation with fan-out to dimension counters
    → Smoke test: verify 1 event produces multiple ClickHouse rows
11. Flink Job 2 — account aggregation, isolated from Job 1
    → Smoke test: verify account metrics updating independently
12. Segment evaluation worker — SIP two-phase pattern (scan + evaluate)
    → Smoke test: verify segment_membership table populating
13. ML scoring service — CLV tier, churn risk, discount sensitivity
    → Smoke test: verify scores appearing as feature store entries
14. Feature writer service — ClickHouse → compute → Redis pipeline
    → Smoke test: verify Redis hash populated with full feature vector
15. Decision engine — bandit with Thompson Sampling, decision logging
    → Smoke test: POST /decide returns arm + score, decision logged
16. Reward closure — background job joining decisions with outcomes
    → Smoke test: verify reward_log entries after simulated purchases
17. FastAPI query layer — all endpoints listed above
18. Dockerfiles for all services
19. pytest tests for all layers
20. `setup.sh`, `deploy.sh`, `teardown.sh`

After each step:
- Run smoke test and show output
- Wait for explicit go-ahead before the next step

---

## Phase 2: Multi-region (do not start until Phase 1 is fully working)

Phase 2 splits into two kind clusters to emulate multi-region deployment.

### Clusters
- `klaflow-us` — us-east-1 simulation
- `klaflow-eu` — eu-west-1 simulation

### New components

**Kafka MirrorMaker 2**
Replicates `customer-events` topic between clusters asynchronously.
This is the source of the core Phase 2 problem: replication lag.

**Global event router**
Routes incoming events by `account_id` hash to the owning region.
Each account has a home region. Events for that account normally go to the
home cluster; routing to the non-home cluster creates a cross-region write
that must be replicated.

**The core Phase 2 problem to demonstrate:**
Due to async replication lag (MirrorMaker has non-zero latency), the same
customer can appear in different segments depending on which cluster you query.
Example: customer makes a purchase, event arrives in US cluster, Flink updates
their counters, they exit `at_risk` segment. EU cluster hasn't received the
replicated event yet. Query EU → customer is still `at_risk`. Query US → customer
is `recent_purchaser`. This is segment divergence.

**Demonstrate the problem explicitly:**
Build a divergence detector:
```
GET /divergence/report → show customers whose segment membership
                         differs between klaflow-us and klaflow-eu
```

**Mitigation approaches to explore (one at a time):**

1. **Read-your-writes routing:**
   After a customer event, route all subsequent reads for that customer
   to their home region for a TTL (e.g. 60 seconds). Eliminates the
   "just purchased but still at_risk" problem for the writing client.
   Cost: requires session affinity tracking.

2. **Conflict resolution by timestamp:**
   When segment membership differs, use `evaluated_at` timestamp to
   pick the more recent evaluation as truth. Simple but loses information
   when both regions have received different events.

3. **Vector clock on segment membership:**
   Each segment membership record carries a vector clock
   `{us: seq, eu: seq}`. Conflict resolution can detect divergence
   vs. true causality. Correct but operationally complex.

For the POC, implement option 1 (read-your-writes) as the pragmatic solution
and demonstrate that it resolves the most common divergence case.

### Phase 2 repo additions

```
terraform/
└── envs/
    ├── us/
    │   ├── main.tf        ← same modules, us-specific tfvars
    │   └── terraform.tfvars
    └── eu/
        ├── main.tf        ← same modules, eu-specific tfvars
        └── terraform.tfvars

src/
├── mirror_maker/
│   └── mm2_config.properties   ← MirrorMaker 2 config
├── global_router/
│   ├── router.py               ← routes by account_id hash to home region
│   └── requirements.txt
└── divergence_detector/
    ├── detector.py             ← queries both clusters, diffs segment membership
    └── requirements.txt
```

### Phase 2 build order

1. Provision `klaflow-eu` kind cluster
2. Terraform envs split — same modules applied to both clusters
3. MirrorMaker 2 — connect clusters, replicate `customer-events` topic
4. Global router — account_id hash → region routing
5. Divergence detector — query both clusters, surface disagreements
6. **Demonstrate the problem:** run producer, show divergence report
7. Read-your-writes routing — implement, re-run, show divergence drops
8. Compare before/after divergence rates in Grafana

---

## Synthetic data design

The producer should generate realistic data distributions, not uniform random.

**Accounts:** 5 synthetic merchants (account_id: merchant_001 through merchant_005)
Each merchant has different customer counts to simulate SMB vs. mid-market:
- merchant_001: 1,000 customers (small)
- merchant_002: 5,000 customers (mid)
- merchant_003: 500 customers (niche)
- merchant_004: 10,000 customers (large)
- merchant_005: 2,000 customers (mid)

**Event distribution (per customer, per day, approximate):**
- page_viewed: Poisson(λ=3) — most common
- email_opened: Poisson(λ=0.4) — less frequent
- link_clicked: Poisson(λ=0.1)
- cart_abandoned: Poisson(λ=0.15)
- purchase_made: Poisson(λ=0.05) — rarest

**Customer lifecycle simulation:**
Assign each customer a lifecycle state at creation. Transition probabilities:
- new → active (0.3/week)
- active → at_risk (0.1/week if no purchase)
- at_risk → lapsed (0.2/week)
- lapsed → churned (0.1/week)
- active → at_risk reversed by purchase event

This means the producer generates more realistic behavior clusters —
some customers consistently active, some drifting toward churn — which
makes the segmentation and decision outputs more interesting to observe.

---

## Hard constraints

- **No docker-compose** — everything on k8s via Terraform + Helm
- **No hardcoded secrets** — use k8s Secrets managed by Terraform
- **No manual kubectl applies** — all resources go through Terraform
- **Schema-first** — all events must pass Avro validation before entering Kafka
- **Idempotent writes** — ClickHouse uses ReplacingMergeTree throughout
- **Two separate Flink jobs** — customer and account aggregation must be isolated
- **ClickHouse is query layer, not event sink** — Flink writes aggregated counters,
  not raw events, to ClickHouse
- **Feature vectors in Redis, not ClickHouse** — decision engine reads Redis only
- **Arms are pre-defined** — bandit action space is a static list, never dynamically
  generated at runtime
- **One step at a time** — never start a step before the previous one is verified

## What NOT to do

- Do not use docker-compose as a shortcut for any component
- Do not write raw events directly to ClickHouse — Flink writes aggregated counters
- Do not use a single Flink job for both customer and account aggregation
- Do not query ClickHouse from the decision engine at decision time — use Redis
- Do not skip Schema Registry validation in the producer
- Do not make the bandit dynamically define new arms at runtime
- Do not start Phase 2 until Phase 1 is fully working end-to-end
- Do not over-engineer the ML models — they are toy implementations for
  architectural demonstration, not production ML