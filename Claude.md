# Klaflow — Klaviyo Architecture Emulation

## What this project is

Klaflow is a hands-on emulation of Klaviyo's actual production architecture,
built to understand how a real-time segmentation and per-customer decisioning
platform works at scale. It emulates the specific architectural choices Klaviyo
made — not a generic event pipeline.

Built in four phases, in this order:

- **Phase 1**: Single-region pipeline — Klaviyo's core event → segment → decision loop ✓ complete
- **Phase 2a**: AI marketing agent — LLM + tool calls over the existing pipeline
- **Phase 2b**: Ontology layer — typed knowledge graph (Neo4j) powering agent reasoning
- **Phase 3**: Multi-region split — data residency, async replication, segment divergence

Phase 3 is last intentionally. Phases 2a and 2b build directly on Phase 1 with
no new infrastructure requirements and are more relevant to the AI agent and
ontology learning goals. Phase 3 is operationally heavy and saved for last.

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
| Query API              | FastAPI (17 endpoints)                  | api          |
| Frontend UI            | React 18 + TypeScript + Tailwind        | ui           |
| AI marketing agent     | Python + Anthropic API (tool_use)       | agent        |
| Knowledge graph        | Neo4j Community Edition                 | ontology     |
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
│       ├── api/                         ← FastAPI query layer
│       ├── ui/                          ← React frontend (Deployment + Service)
│       ├── agent/                       ← AI marketing agent service (Phase 2a)
│       └── neo4j/                       ← Neo4j Community Edition (Phase 2b)
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
│   │   ├── main.py                      ← FastAPI: 17 endpoints (query + decision + UI)
│   │   └── requirements.txt
│   ├── ui/                              ← React frontend (Vite + Tailwind + Recharts)
│   │   ├── src/
│   │   │   ├── api/client.ts            ← TanStack Query API client
│   │   │   ├── components/              ← Layout, Badge, Skeleton, ErrorState
│   │   │   └── pages/                   ← Dashboard, Profiles, CustomerProfile,
│   │   │                                   Segments, SegmentDetail, Decisions,
│   │   │                                   Flows (stub), Campaigns (stub)
│   │   ├── vite.config.ts              ← dev server + /api proxy to localhost:8000
│   │   └── package.json
│   └── clickhouse/
│       └── schema.sql                   ← ClickHouse table definitions
│   ├── agent/                           ← Phase 2a: AI marketing agent
│   │   ├── agent.py                     ← LLM orchestration loop
│   │   ├── tools.py                     ← tool wrappers over FastAPI ✓ done
│   │   ├── tool_schemas.py              ← JSON schemas for Anthropic tool_use API
│   │   ├── goal_parser.py               ← structured intent from natural language
│   │   ├── campaign_monitor.py          ← outcome monitoring + report generation
│   │   └── requirements.txt             ← anthropic package ✓ done
│   └── ontology/                        ← Phase 2b: knowledge graph layer
│       ├── schema.py                    ← object type and link type definitions
│       ├── graph_writer.py              ← populates Neo4j from ClickHouse + Redis
│       ├── graph_query.py               ← Cypher execution + result hydration
│       ├── nl_to_cypher.py              ← LLM translates NL → Cypher (logged)
│       ├── graphrag.py                  ← hybrid vector + graph retrieval
│       └── requirements.txt
├── docker/
│   ├── producer/Dockerfile
│   ├── flink_jobs/Dockerfile
│   ├── feature_store/Dockerfile
│   ├── ml_models/Dockerfile
│   ├── decision_engine/Dockerfile
│   ├── api/Dockerfile
│   └── ui/                              ← multi-stage: node build → nginx serve
│       ├── Dockerfile
│       └── nginx.conf                   ← SPA routing + /api/ proxy to API service
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

## Phase 3: Multi-region (do not start until Phases 1, 2a, and 2b are complete)

Phase 3 splits into two kind clusters to emulate multi-region deployment.

### Clusters
- `klaflow-us` — us-east-1 simulation
- `klaflow-eu` — eu-west-1 simulation

### New components

**Kafka MirrorMaker 2**
Replicates `customer-events` topic between clusters asynchronously.
This is the source of the core Phase 3 problem: replication lag.

**Global event router**
Routes incoming events by `account_id` hash to the owning region.
Each account has a home region. Events for that account normally go to the
home cluster; routing to the non-home cluster creates a cross-region write
that must be replicated.

**The core Phase 3 problem to demonstrate:**
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

### Phase 3 repo additions

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

### Phase 3 build order

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
- Do not start Phase 3 until Phases 1, 2a, and 2b are complete
- Do not over-engineer the ML models — they are toy implementations for
  architectural demonstration, not production ML
---

## Demonstration script (CEO conversation target)

The end goal of Phase 2a is a single runnable script that produces a
concrete, side-by-side comparison suitable for showing in a technical
conversation. This is a first-class deliverable, not an afterthought.

**File:** `scripts/demo.sh` (or `src/demo/run_demo.py`)

**What it does, in order:**

1. Seed merchant_001 with 90 days of synthetic data (if not already seeded)
2. Run segment evaluation — confirm at_risk segment is populated
3. Run feature store update — confirm Redis feature vectors are fresh

4. **Approach A — uniform treatment (current Klaviyo behavior):**
   Take all customers in at_risk segment for merchant_001.
   Send everyone the same arm: `email_10pct_discount`.
   Log these as "approach_a" decisions in a separate comparison table.
   Simulate outcomes: apply reward signal based on each customer's
   discount_sensitivity score (high sensitivity → higher purchase probability).
   Compute: conversion rate, total discount cost (count of discounts sent),
   revenue per customer contacted.

5. **Approach B — per-customer bandit (Klaflow):**
   Same at_risk customers, same merchant.
   Run each through the bandit — arm selected based on full feature vector.
   Log as "approach_b" decisions.
   Simulate outcomes using the same reward model.
   Compute: conversion rate, total discount cost, revenue per customer contacted.

6. **Print comparison report:**
```
═══════════════════════════════════════════════════════
  KLAFLOW DEMO — Per-Customer Decisioning vs. Uniform Treatment
  Merchant: merchant_001  |  Segment: at_risk  |  Customers: N
═══════════════════════════════════════════════════════

  APPROACH A — Uniform (everyone gets 10% discount)
  ─────────────────────────────────────────────────
  Customers targeted:     1,247
  Discounts sent:         1,247   (100%)
  Conversions:              87    (6.9%)
  Unnecessary discounts:   891    (VIP + low-sensitivity customers)
  Revenue attributed:    $4,350
  Discount cost:         $1,247   (assumed $1/discount for illustration)

  APPROACH B — Per-Customer Bandit (Klaflow)
  ─────────────────────────────────────────────────
  Customers targeted:     1,247
  Arm breakdown:
    email_no_offer         312   (25%)  — VIP, low discount sensitivity
    email_10pct_discount   389   (31%)  — high discount sensitivity
    email_free_shipping    201   (16%)  — mid-tier, price aware
    sms_nudge              198   (16%)  — low email open rate
    no_send                147   (12%)  — very low intent signals
  Conversions:             119   (9.6%)
  Discount cost:           $590  (47% reduction)
  Revenue attributed:    $5,950

  DELTA
  ─────────────────────────────────────────────────
  Conversion rate:    +2.7pp   (+39%)
  Discount cost:      -$657    (-53%)
  Revenue:            +$1,600  (+37%)
═══════════════════════════════════════════════════════
```

7. Also output the arm distribution as a bar chart to a PNG file
   (`demo_output/arm_distribution.png`) using matplotlib — simple,
   no dependencies beyond what's already in the project.

**This script must be runnable in under 5 minutes on a fresh seed.**
It is the single most important thing to have working before a CEO conversation.
The numbers don't need to be perfectly calibrated — the point is demonstrating
that per-customer treatment produces a measurably different (and better) outcome
than uniform treatment, and that the system can show you why (arm breakdown).

---

## Phase 2 agent additions
(append to existing Phase 2 section when built)

Phase 2a adds one more demo capability on top of the comparison script:

**Agent demo extension:**
After the comparison report, show the agent accepting a natural language goal
and producing the Approach B execution plan automatically:

```
INPUT:  "Re-engage at-risk customers for merchant_001.
         Don't discount VIP customers. Skip anyone contacted this week."

AGENT TOOL CALL SEQUENCE:
  1. get_segments() → found: at_risk (1,247 customers)
  2. check_recent_contact(days=7) → excluded 89 customers
  3. get_customer_features() × 1,158 → feature vectors retrieved
  4. request_decision() × 1,158 → bandit arms selected
  5. schedule_action() × 1,011 → actions scheduled (147 no_send excluded)

OUTPUT: Campaign #c_001 created. 1,011 actions scheduled.
        Monitoring for outcomes over next 72 hours.
```

This sequence printed to console is the "show your work" moment —
it makes the agent's reasoning visible and auditable without needing
a polished UI.

---

## Phase 2: AI Marketing Agent (do not start until Phase 1 is fully working)

Phase 2 adds an AI marketing agent layer on top of the existing pipeline.
Nothing in Phase 1 is replaced. The agent layer sits above it.

Phase 2 is split into two sub-phases:

- **Phase 2a**: Agent without ontology — LLM + tool calls over existing APIs
- **Phase 2b**: Ontology layer — typed knowledge graph powering agent reasoning

Build 2a first. The contrast between 2a and 2b is the learning.

---

### What the agent actually does

A merchant expresses a goal in natural language:
> "Re-engage customers who haven't purchased in 60 days but were previously
> high-value. Don't send to anyone who got a message in the last 7 days."

The agent:
1. Parses the goal into structured intent
2. Queries the existing API to find qualifying customers
3. Applies exclusion rules automatically
4. Selects appropriate action per customer via the existing bandit
5. Schedules execution
6. Monitors outcomes and generates a report

The agent does NOT replace the bandit. It decides whether and when to engage.
The bandit decides which action to take. Different decisions, different levels.

---

### Phase 2a: Agent without ontology

**Tool set:**

```python
get_segments()
get_segment_members(segment_name, limit)
get_customer_segments(customer_id)
get_customer_features(customer_id)
get_customer_decision_history(customer_id)
check_recent_contact(customer_id, days)
get_suppression_list(account_id)
request_decision(customer_id, context)
schedule_action(customer_id, arm, send_at)
get_campaign_outcomes(campaign_id, hours_elapsed)
get_arm_performance(arm_name, days)
```

**What the LLM does vs. tools:**
- LLM: intent parsing, tool sequence decisions, report generation
- Tools: all data access, all writes, all arithmetic

**New files:**
```
src/agent/
  agent.py              ← LLM orchestration loop
  tools.py              ← FastAPI wrappers ✓ done
  tool_schemas.py       ← Anthropic tool_use JSON schemas
  goal_parser.py        ← NL → structured intent
  campaign_monitor.py   ← outcome monitoring + report
  requirements.txt      ← anthropic package ✓ done

src/api/agent_endpoints.py   ← /agent/* routes added to FastAPI
terraform/modules/agent/
docker/agent/Dockerfile
```

**New API endpoints:**
```
POST /agent/campaign        → { campaign_id, status }
GET  /agent/campaign/{id}   → { plan, targeted_count, sent_count, outcomes, report }
GET  /agent/campaigns       → list all campaigns
POST /agent/campaign/{id}/pause
POST /agent/campaign/{id}/cancel
```

**LLM:** claude-sonnet-4-20250514 via Anthropic API tool_use. No frameworks.

**Smoke test:**
```
POST /agent/campaign
{
  "account_id": "merchant_001",
  "goal": "Send a win-back offer to customers who haven't purchased
           in 45 days. Skip anyone contacted in the last 14 days."
}
```
Expected tool call sequence in logs:
get_segments → get_segment_members → check_recent_contact ×N →
get_customer_features ×N → request_decision ×N → schedule_action ×N

**UI:** Wire the Campaigns page (currently stub) to /agent/* endpoints.
Show: goal text, status, tool call log, arm breakdown, conversion rate.

---

### Phase 2b: Ontology layer

**Why 2a falls short:**

The LLM in 2a reasons over flat API responses. It cannot answer multi-hop
questions:
- "Which customers received a win-back last month, purchased, then lapsed again?"
- "What products do at-risk VIP customers browse before churning?"
- "Which campaigns worked for this segment before?"

These require graph traversal. Neo4j provides it.

**Object types:**
```
Profile     customer_id, account_id, lifecycle_state, clv_tier
Order       order_id, customer_id, amount, timestamp
Product     product_id, name, category, price
Segment     name, condition_definition, member_count
Campaign    campaign_id, goal_text, status, targeted_count
Decision    decision_id, customer_id, arm, score, timestamp
Outcome     outcome_id, decision_id, reward, observed_at
```

**Link types:**
```
Profile  —[PLACED]→       Order
Profile  —[BROWSED]→      Product
Profile  —[MEMBER_OF]→    Segment
Profile  —[RECEIVED]→     Decision
Decision —[RESULTED_IN]→  Outcome
Campaign —[TARGETED]→     Profile
Campaign —[USED_SEGMENT]→ Segment
Order    —[CONTAINS]→     Product
```

**Two new agent tools:**
```python
graph_query(cypher_query)        # run Cypher, return typed objects
explain_query(natural_language)  # NL → Cypher → run → return results + query
```

**The contrast that matters:**

Goal: "Re-engage VIP customers who bought electronics in Q4 but haven't purchased since."

Phase 2a: calls get_segments(), finds vip_lapsed, returns all of them.
Misses the electronics filter, Q4 timing, customers who bought in January.

Phase 2b generates and runs:
```cypher
MATCH (p:Profile)-[:PLACED]->(o:Order)-[:CONTAINS]->(prod:Product)
WHERE p.clv_tier = 'vip'
  AND prod.category = 'electronics'
  AND o.timestamp >= datetime('2024-10-01')
  AND o.timestamp <  datetime('2025-01-01')
  AND NOT EXISTS {
    MATCH (p)-[:PLACED]->(o2:Order)
    WHERE o2.timestamp >= datetime('2025-01-01')
  }
RETURN p.customer_id, max(o.timestamp) as last_purchase
ORDER BY last_purchase DESC
```

The Cypher is the reasoning made explicit — auditable, readable, correctable.

**New files:**
```
src/ontology/
  schema.py         ← object + link type definitions
  graph_writer.py   ← populates Neo4j from ClickHouse + Redis (15min poll)
  graph_query.py    ← Cypher execution + result hydration
  nl_to_cypher.py   ← LLM → Cypher with query_log audit trail
  graphrag.py       ← vector similarity over past campaigns + graph traversal
  requirements.txt

terraform/modules/neo4j/
docker/neo4j/Dockerfile
```

---

### Phase 2 build order

**Phase 2a:**
1. `requirements.txt` — anthropic package
2. `tools.py` — FastAPI wrappers
3. `tool_schemas.py` — Anthropic tool_use JSON schemas
4. `agent.py` — LLM loop
   → Smoke test: tool call sequence appears in logs for test goal
5. `campaign_monitor.py` — reward polling + report generation
6. `/agent/*` FastAPI endpoints
7. Dockerfile + Terraform module
8. Wire Campaigns UI page to /agent/* endpoints
9. **`scripts/demo.py` — side-by-side comparison script** (see Demonstration script section)
   → This is the most important deliverable for the CEO conversation

**Phase 2b:**
10. Neo4j — Terraform module
    → Smoke test: Cypher query runs
11. `schema.py` — object and link type definitions
12. `graph_writer.py` — populate nodes from ClickHouse + Redis
    → Smoke test: MATCH (p:Profile) RETURN count(p) matches customer count
13. Link population — PLACED, BROWSED, MEMBER_OF, RECEIVED
    → Smoke test: multi-hop query returns results
14. `nl_to_cypher.py` — NL → Cypher with logging
    → Smoke test: natural language → valid Cypher → non-empty results
15. Add graph_query + explain_query tools to agent
16. `graphrag.py` — vector similarity + graph traversal
    → Smoke test: complex goal produces more precise cohort than Phase 2a

---

### Hard constraints for Phase 2

- **No LangChain or agent frameworks** — Anthropic API tool_use directly
- **LLM never touches raw data** — all data access through tool functions
- **Cypher queries are logged** — every nl_to_cypher translation stored in query_log
- **Neo4j is reasoning layer only** — no event-level data, graph structure only
- **Do not modify existing Phase 1 code** — agent calls the API, not internals
- **Arms remain pre-defined** — agent requests bandit decisions, cannot create arms
- **Do not start Phase 2b until Phase 2a smoke tests pass**

### What NOT to do in Phase 2

- Do not use LangChain, LlamaIndex, or any agent framework
- Do not let the LLM generate SQL or ClickHouse queries
- Do not store event-level data in Neo4j
- Do not replace the bandit with the agent
- Do not modify existing endpoints in src/api/main.py — add agent_endpoints.py separately