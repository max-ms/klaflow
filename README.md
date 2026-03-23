# Klaflow

A hands-on emulation of Klaviyo's production architecture: real-time customer segmentation, per-customer feature vectors, a contextual bandit decision engine, and an AI marketing agent that orchestrates campaigns via natural language.

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
                        │  AI Agent: NL goal ──► tool calls ──► bandit ──► actions│
                        │                                                         │
                        │  API: 22 FastAPI endpoints over all of the above        │
                        │  UI:  React dashboard (profiles, segments, campaigns)   │
                        └─────────────────────────────────────────────────────────┘
```

---

## Table of Contents

- [Architecture](#architecture)
  - [Track 1 — Event Aggregation](#track-1--event-aggregation)
  - [Track 2 — Segment Evaluation](#track-2--segment-evaluation-sip-pattern)
  - [Decision Layer](#decision-layer)
  - [AI Agent Layer (Phase 2a)](#ai-agent-layer-phase-2a)
- [Why Each Architectural Choice](#why-each-architectural-choice)
- [Stack](#stack)
- [Quick Start](#quick-start)
- [Running the Demo](#running-the-demo)
- [AI Agent](#ai-agent)
- [Frontend UI](#frontend-ui)
- [Data Model](#data-model)
- [Inspecting Internals](#inspecting-internals)
- [Repo Structure](#repo-structure)
- [Running Tests](#running-tests)

---

## Architecture

Klaflow has **two parallel processing tracks**, a **decision layer**, and an **AI agent layer** that orchestrates campaigns using the decision layer.

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

### AI Agent Layer (Phase 2a)

Sits above the decision layer. Does NOT replace the bandit — it operates at a different level of abstraction:

- **Agent decides**: *whether* and *when* to engage a customer (targeting, exclusions, timing)
- **Bandit decides**: *which action* to take once engagement is triggered (arm selection)

A merchant expresses a goal in natural language:

> "Send a win-back offer to at-risk customers. Skip anyone contacted in the last 14 days."

The agent:
1. Parses intent (target segment, exclusion rules, objective)
2. Queries segments and customer data via tool calls
3. Applies suppression list and frequency caps
4. Calls the bandit for per-customer arm selection
5. Schedules actions and generates a campaign report

The LLM never touches raw data directly — all data access goes through 11 tool functions that call the existing API. The tool call sequence is logged and visible in the UI.

**Implementation**: Direct Anthropic API `tool_use` — no LangChain or agent frameworks. The tool loop is ~100 lines of explicit Python in `agent.py`.

---

## Why Each Architectural Choice

These are the specific decisions worth understanding — not just *what* is built, but *why* this way and not the obvious alternative.

### Why two separate Flink jobs instead of one?

Customer aggregation and account aggregation run on different keys (`customer_id` vs. `account_id`). Combining them means a single large merchant generating an event spike degrades counter accuracy for all customers across all merchants. Separate jobs = separate resource pools = separate failure domains. Klaviyo made this split explicitly after experiencing cross-contamination issues.

### Why ClickHouse receives aggregated counters, not raw events?

A naive architecture would stream raw events to ClickHouse and aggregate at query time. This fails at Klaviyo's scale: 100K events/second x 50+ dimension combinations = millions of writes per second. Instead, Flink pre-aggregates and writes the results. ClickHouse is the **query layer**, not the **aggregation layer**. This is the single most important architectural distinction.

### Why Redis AND ClickHouse (not just one)?

ClickHouse is optimized for analytical queries across many rows. The decision engine needs to read one customer's feature vector in <1ms. ClickHouse can't do that — even a fast point query takes 5-50ms. Redis gives <1ms hash reads. So: ClickHouse for segment evaluation (scan millions of rows), Redis for decision-time lookups (one customer, one hash).

### Why the feature writer writes BACK to ClickHouse?

The feature writer computes derived features (open rates, lifecycle state) and ML scores (CLV tier, churn risk). These are written to Redis for the bandit. But they're ALSO written back to `customer_counters` in ClickHouse — because segment conditions like `vip_lapsed` need `clv_tier`, and `at_risk` needs `days_since_last_purchase`. Without this write-back, ML-dependent segments would have zero members.

### Why Thompson Sampling and not epsilon-greedy?

Epsilon-greedy explores uniformly — it sends random actions e% of the time. Thompson Sampling explores *intelligently* — it explores more where uncertainty is highest. For a marketing system, this matters: you don't want to randomly send SMS to a customer who always opens email. Thompson Sampling concentrates exploration on arms the system hasn't learned enough about yet.

### Why pre-defined arms and not dynamic ones?

The action space is static: 5 arms. The bandit does NOT generate new actions. This is deliberate: dynamic arm generation creates a combinatorial explosion that makes learning intractable. Each new arm needs its own exploration budget. Keeping arms fixed means the bandit can converge on a good policy with finite data.

### Why the SIP two-phase scan-then-evaluate pattern?

Evaluating all 18,500 customers against all 7 segments on every cycle is wasteful — most customers haven't changed. Phase 1 scans for profiles with recent counter updates (cheap query, returns ~hundreds of customer_ids). Phase 2 evaluates only those profiles against all segment conditions. This reduces the evaluation workload by 10-100x.

### Why the agent uses tool calls instead of direct DB access?

The agent could query ClickHouse and Redis directly — it would be faster. But tool calls through the API provide: (1) audit trail — every query is logged, (2) access control — the agent can only see what the API exposes, (3) the same data contract as the UI and external integrations. This is the production-correct pattern for an AI agent layer.

### Why no LangChain?

The tool loop is simple: send messages -> check for tool_use blocks -> execute tools -> send results back. It's ~100 lines. LangChain adds a dependency, abstracts away the mechanics, and makes debugging harder. For learning purposes and for production reliability, the explicit loop is better.

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
| AI agent             | Anthropic API (tool_use, no frameworks) | `agent`    |
| Query API            | FastAPI (22 endpoints)               | `api`         |
| Frontend UI          | React 18 + TypeScript + Tailwind     | `ui`          |
| Observability        | Prometheus + Grafana                 | `monitoring`  |
| Infrastructure       | Terraform (kubernetes + helm)        | ---           |
| Kubernetes           | kind                                 | ---           |

---

## Quick Start

Three scripts control the entire deployment. All Docker image builds, Terraform applies, Kafka topic creation, ClickHouse schema setup, and pipeline data seeding are handled by these scripts — no manual `docker build` or `kubectl` commands needed.

### Prerequisites

```bash
brew install kind kubectl terraform
# Docker Desktop must be running
python3 --version  # 3.9+
```

### Deploy

```bash
# 1. Create kind cluster
./scripts/setup.sh

# 2. Build all images, deploy infrastructure, apply schemas
./scripts/deploy.sh

# 3. Seed the pipeline: events → aggregation → features → segments
./scripts/seed.sh

# 4. Access the API
kubectl port-forward svc/klaflow-api 8000:80 -n api &
curl -s http://localhost:8000/segments | python3 -m json.tool
```

### Teardown

```bash
./scripts/teardown.sh
```

### Configuration

All service connection strings are defined in `scripts/seed.sh` and the Terraform modules. The single source of truth for each:

| Config | Where defined | Value |
|--------|--------------|-------|
| Kafka bootstrap | `scripts/seed.sh`, Terraform env vars | `kafka.streaming.svc.cluster.local:9092` |
| Schema Registry | `scripts/seed.sh`, Terraform env vars | `http://schema-registry.streaming.svc.cluster.local:8081` |
| ClickHouse host | `scripts/seed.sh`, Terraform env vars | `clickhouse.analytics.svc.cluster.local` |
| ClickHouse auth | `scripts/seed.sh`, Terraform env vars | `klaflow` / `klaflow-pass` |
| Redis host | `scripts/seed.sh`, Terraform env vars | `redis-master.features.svc.cluster.local` |
| ML scoring URL | `scripts/seed.sh`, Terraform env vars | `http://ml-scoring.decisions.svc.cluster.local:8000` |
| Anthropic API key | `.env` file (sourced by `deploy.sh`) | `ANTHROPIC_API_KEY=sk-ant-...` |

Python services read these from environment variables with in-cluster defaults, so they work both in k8s (via Terraform-managed env vars) and locally (via port-forwards + exports).

### What the scripts do

**`setup.sh`** — checks prerequisites (docker, kind, kubectl, terraform, python3), creates the `klaflow-us` kind cluster.

**`deploy.sh`** — sources `.env`, builds the 5 service images that Terraform deploys as long-running k8s Deployments (`api`, `ml-models`, `decision-engine`, `agent`, `ui`), loads them into the kind cluster, runs `terraform apply`, waits for Kafka/ClickHouse/Schema Registry, creates the `__consumer_offsets` topic (required for KRaft mode), applies the ClickHouse schema.

**`seed.sh`** — builds the 3 pipeline job images (`producer`, `feature-store`, `segment-worker`), then runs each as a one-off k8s pod in order, waiting for each to complete:
1. **Producer seed**: 90 days of synthetic events for 18,500 customers across 5 merchants (~5M events)
2. **Flink aggregation**: real PyFlink jobs (running as FlinkDeployments) consume from Kafka and write fan-out counters to ClickHouse — seed.sh waits 60s for them to process
3. **Feature writer**: ClickHouse counters -> derived features + ML scores -> Redis + ClickHouse write-back
4. **Segment evaluation**: SIP two-phase scan + evaluate -> 7 segments populated

**`teardown.sh`** — `terraform destroy` + `kind delete cluster`.

---

## Running the Demo

The demo script compares **uniform treatment** (everyone gets 10% discount) vs. **per-customer bandit** (each customer gets the action most likely to convert them).

```bash
# Port-forward ClickHouse and Redis for local access
kubectl port-forward -n analytics svc/clickhouse 8123:8123 &
kubectl port-forward -n features svc/redis-master 6379:6379 &

pip3 install numpy redis requests matplotlib
python3 src/demo/run_demo.py
```

### Sample output

```
=========================================================
  KLAFLOW DEMO — Per-Customer Decisioning vs. Uniform Treatment
  Merchant: merchant_001  |  Segment: at_risk  |  Customers: 327
=========================================================

  APPROACH A — Uniform (everyone gets 10% discount)
  -----------------------------------------------------
  Customers targeted:      327
  Discounts sent:          327   (100%)
  Conversions:              22   (6.7%)
  Revenue attributed:   $    1,387
  Discount cost:        $      327   ($1/discount)

  APPROACH B — Per-Customer Bandit (Klaflow)
  -----------------------------------------------------
  Customers targeted:      327
  Arm breakdown:
    email_no_offer             137   (  42%)  -- engaged, just need a nudge
    email_free_shipping        124   (  38%)  -- cart abandoners, shipping barrier
    email_10pct_discount        57   (  17%)  -- price-sensitive
    sms_nudge                    7   (   2%)  -- don't open email
    no_send                      2   (   1%)  -- very low intent
  Conversions:              32   (9.8%)
  Discount cost:        $      181   (-45% vs A)
  Revenue attributed:   $    2,029

  DELTA
  -----------------------------------------------------
  Conversion rate:    +3.1pp   (+45%)
  Discount cost:      $    -146    (-45%)
  Revenue:            $    +642    (+46%)
=========================================================
```

### Why the bandit wins

1. **Customers who don't open email** (low `email_open_rate_7d`) never see the discount. The bandit routes these to SMS — a channel that actually reaches them.
2. **Engaged customers who just need a nudge** (high open rate, prior purchases) would convert without a discount. The bandit sends `email_no_offer`, saving the discount cost.
3. **Cart abandoners** (high `cart_abandon_rate_30d`) abandoned because of shipping cost, not product price. The bandit sends `email_free_shipping` — addressing the actual barrier.

### How the reward simulation works

Each customer gets a deterministic random seed (based on customer_id), so the same customer gets the same random draw regardless of which arm they receive. What changes is the **conversion probability** — which depends on the interaction between the arm and the customer's feature profile. This ensures the comparison is fair: same customers, same randomness, different treatments, different outcomes.

---

## AI Agent

### How it works

The agent is a loop over the Anthropic `messages` API with `tool_use`:

```
Merchant goal (natural language)
        |
   Agent service (Claude + 11 tools)
        | tool calls
   Existing FastAPI endpoints
   (segments, customers, features, bandit)
        |
   Scheduled actions per customer
        |
   Campaign report (tool call log + arm breakdown)
```

Source: `src/agent/agent.py` (~200 lines, no frameworks)

### Tool set

The LLM can call these 11 functions. Each wraps an existing API endpoint — the agent never touches the database directly.

| Category | Tool | What it does |
|----------|------|-------------|
| Segment | `get_segments()` | List all segments with member counts |
| Segment | `get_segment_members(segment_name, limit)` | Get customers in a segment |
| Segment | `get_customer_segments(customer_id)` | Which segments a customer belongs to |
| Customer | `get_customer_features(customer_id)` | Full feature vector from Redis |
| Customer | `get_customer_decision_history(customer_id)` | Past decisions + outcomes |
| Exclusion | `check_recent_contact(customer_id, days)` | Was this customer contacted recently? |
| Exclusion | `get_suppression_list(account_id)` | Globally suppressed (churned) customers |
| Decision | `request_decision(customer_id, account_id, context)` | Call the bandit for an arm |
| Decision | `schedule_action(customer_id, account_id, arm, campaign_id)` | Schedule the action |
| Measurement | `get_campaign_outcomes(campaign_id)` | Conversion metrics for a campaign |
| Measurement | `get_arm_performance(arm_name)` | Historical arm stats |

### Running the agent

```bash
# Set your Anthropic API key in .env
echo "ANTHROPIC_API_KEY=sk-ant-..." > .env

# Ensure API is accessible
kubectl port-forward svc/klaflow-api 8000:80 -n api &

# Run a campaign via the API
curl -s -X POST http://localhost:8000/agent/campaign \
  -H "Content-Type: application/json" \
  -d '{"account_id":"merchant_001","goal":"Win back at-risk customers. Skip recently contacted."}' \
  | python3 -m json.tool

# Or via Python directly
ANTHROPIC_API_KEY=$(grep ANTHROPIC_API_KEY .env | cut -d= -f2) \
python3 -c "
import sys; sys.path.insert(0, 'src/agent')
from agent import run_campaign, campaign_summary
import json
c = run_campaign('merchant_001', 'Send a win-back offer to at-risk customers. Skip anyone contacted in the last 14 days.')
print(json.dumps(campaign_summary(c), indent=2, default=str))
"
```

### Agent API endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/agent/campaign` | POST | Create + execute a campaign from NL goal |
| `/agent/campaign/{id}` | GET | Campaign status, tool call log, report |
| `/agent/campaigns` | GET | List all campaigns |
| `/agent/campaign/{id}/pause` | POST | Pause a running campaign |
| `/agent/campaign/{id}/cancel` | POST | Cancel a campaign |

### What the LLM does vs. what the tools do

- **LLM**: natural language parsing, deciding which tools to call and in what order, generating the human-readable campaign report
- **Tools**: all data access, all writes, all arithmetic — the LLM never computes counts, rates, or scores itself

### Goal parser

`src/agent/goal_parser.py` extracts structured metadata from the natural language goal before the LLM loop starts. It identifies: target segments, exclusion windows, VIP handling, and campaign objective (win_back, retention, upsell, cart_recovery). This is used for logging and validation — the LLM handles the actual execution planning.

---

## Frontend UI

A React dashboard that connects to the live FastAPI backend.

**Stack:** React 18, TypeScript, Tailwind CSS, React Router v6, TanStack Query, Recharts, Vite.

### Running locally

```bash
# Ensure the API is accessible
kubectl port-forward svc/klaflow-api 8000:80 -n api &

cd src/ui
npm install
npm run dev
# -> http://localhost:3001 (proxies /api to localhost:8000)
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
| **Flows** | `/flows` | Stub — placeholder cards for Welcome Series, Abandoned Cart, Win-Back flows. |
| **Campaigns** | `/campaigns` | Create campaigns via NL goal, see status, arm breakdown, tool call sequence (terminal-style log), and agent report. |

### Design

- **Sidebar:** Deep navy (`#1B2559`) with white text, persistent navigation
- **Accent:** Klaviyo green (`#27AE60`)
- **Font:** Inter (Google Fonts)
- **Tables:** Zebra striping, hover highlight, sticky headers
- **Loading:** Skeleton screens (shimmer animation), not spinners
- **Empty states:** Descriptive message, not blank
- **Errors:** Inline error with retry button

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

All tables use `ReplacingMergeTree` — duplicate writes are deduplicated by the version column (`computed_at` or `evaluated_at`). Use `FINAL` in queries to get deduplicated results.

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

Event rates per customer per day (Poisson): page_viewed (l=3), email_opened (l=0.4), cart_abandoned (l=0.15), link_clicked (l=0.1), purchase_made (l=0.05).

Customer lifecycle: `new -> active -> at_risk -> lapsed -> churned` with weekly transition probabilities. Lifecycle state affects event generation rates (churned customers produce ~0 events).

### Feature Vector (Redis)

Pre-computed per customer, stored as a Redis hash at `customer:{account_id}:{customer_id}`:

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
| `churn_risk_score` | float | 0.0-1.0 |
| `discount_sensitivity` | float | 0.0-1.0 |
| `updated_at` | epoch_ms | last update timestamp |

---

## Inspecting Internals

These commands are for debugging and exploring the system after deployment. They are not required for normal operation.

### API

```bash
kubectl port-forward svc/klaflow-api 8000:80 -n api &

curl -s http://localhost:8000/segments | python3 -m json.tool
curl -s "http://localhost:8000/segments/high_engagers/customers?limit=10" | python3 -m json.tool
curl -s http://localhost:8000/customers/merchant_001:cust_0001/segments | python3 -m json.tool
curl -s "http://localhost:8000/customers/merchant_001:cust_0001/features?account_id=merchant_001" | python3 -m json.tool
curl -s -X POST http://localhost:8000/decide \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"merchant_001:cust_0001","account_id":"merchant_001"}' | python3 -m json.tool
```

### ClickHouse

```bash
kubectl -n analytics exec -it svc/clickhouse -- \
  clickhouse-client --user klaflow --password klaflow-pass

-- Fan-out counters for a customer
SELECT metric_name, metric_value
FROM customer_counters FINAL
WHERE customer_id = 'merchant_001:cust_0001'
ORDER BY metric_name;

-- Segment membership summary
SELECT segment_name,
       countIf(in_segment = 1) AS members,
       count() AS evaluated
FROM segment_membership FINAL
GROUP BY segment_name
ORDER BY members DESC;
```

### Kafka

```bash
kubectl -n streaming exec -it kafka-controller-0 -- bash

/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
/opt/bitnami/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

### Redis

```bash
kubectl -n features exec -it svc/redis-master -- redis-cli

HGETALL customer:merchant_001:merchant_001:cust_0001
DBSIZE
```

### Prometheus + Grafana

```bash
kubectl port-forward svc/kube-prometheus-stack-grafana 3000:80 -n monitoring &
# Login: admin / klaflow
```

### Terraform

```bash
cd terraform
terraform state list
terraform plan
```

---

## Repo Structure

```
klaflow/
├── terraform/
│   ├── providers.tf                        # K8s + Helm providers -> kind-klaflow-us
│   ├── main.tf                             # Wires all modules together
│   └── modules/
│       ├── kafka/                           # Kafka + Schema Registry
│       ├── clickhouse/                      # ClickHouse + schema ConfigMap
│       ├── flink/                           # Flink Kubernetes Operator
│       ├── redis/                           # Redis standalone
│       ├── observability/                   # Prometheus + Grafana
│       ├── decisions/                       # ML scoring + reward logger
│       ├── api/                             # FastAPI deployment
│       ├── ui/                              # React frontend (Deployment + Service)
│       └── agent/                           # AI agent service (Phase 2a)
├── src/
│   ├── producer/
│   │   ├── producer.py                      # Synthetic events: seed (90 days) or continuous
│   │   └── schemas/customer_event.avsc      # Avro schema (event contract)
│   ├── flink_jobs/
│   │   ├── customer_aggregation_job.py      # Job 1: per-customer fan-out counters (PyFlink)
│   │   ├── account_aggregation_job.py       # Job 2: per-account metrics (PyFlink)
│   │   └── requirements.txt
│   ├── segment_worker/
│   │   └── segment_evaluator.py             # SIP two-phase: scan changed -> evaluate rules
│   ├── feature_store/
│   │   ├── feature_writer.py                # ClickHouse -> compute features -> Redis
│   │   └── feature_schema.py                # CustomerFeatureVector dataclass
│   ├── ml_models/
│   │   └── score_customers.py               # CLV tier, churn risk, discount sensitivity
│   ├── decision_engine/
│   │   ├── bandit.py                        # Thompson Sampling contextual bandit
│   │   ├── arms.py                          # 5 arms + reward constants
│   │   └── reward_logger.py                 # Background reward closure job
│   ├── api/
│   │   ├── main.py                          # 17 original + 5 agent endpoints
│   │   └── agent_endpoints.py               # /agent/* campaign CRUD routes
│   ├── agent/                               # Phase 2a: AI marketing agent
│   │   ├── agent.py                         # LLM orchestration loop (Anthropic tool_use)
│   │   ├── tools.py                         # 11 tool wrappers over FastAPI
│   │   ├── tool_schemas.py                  # JSON schemas for each tool
│   │   ├── goal_parser.py                   # NL -> structured CampaignIntent
│   │   ├── campaign_monitor.py              # Outcome metrics + report formatting
│   │   └── requirements.txt                 # anthropic + httpx
│   ├── demo/
│   │   └── run_demo.py                      # Side-by-side comparison script
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
│   ├── deploy.sh                            # Build all images + terraform apply + schema setup
│   ├── seed.sh                              # Run pipeline steps in order (events -> counters -> features -> segments)
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
      1 event -> N counters (event_type x window x campaign x category)
      Only windows the event falls within are updated (7-day-old event -> 30d only, not 7d)
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
      segment management, campaigns, and a bandit operator console
11. AI agent receives NL goal -> calls tools (segments, features, bandit) ->
      schedules per-customer actions -> generates campaign report with tool call log
```

The key ordering dependency: **feature writer must run before segment evaluation** for ML-dependent
segments (`at_risk`, `vip_lapsed`, `discount_sensitive`) to have non-zero members. The feature
writer writes `clv_tier`, `days_since_last_purchase`, `churn_risk_score`, and `discount_sensitivity`
back to `customer_counters` in ClickHouse, which the segment evaluator then queries.

## Project Phases

| Phase | Status | What it adds |
|-------|--------|-------------|
| **Phase 1** | Complete | Core pipeline: Kafka -> aggregation -> ClickHouse -> segments -> Redis -> bandit -> API -> UI |
| **Phase 2a** | Complete | AI agent: NL goals -> tool calls -> bandit decisions -> campaign reports |
| **Phase 2b** | Not started | Ontology layer: Neo4j knowledge graph for multi-hop reasoning |
| **Phase 3** | Not started | Multi-region: two kind clusters, MirrorMaker, segment divergence detection |
