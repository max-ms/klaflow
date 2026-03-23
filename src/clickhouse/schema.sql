-- Drop old tables from previous architecture
DROP TABLE IF EXISTS customer_events;
DROP TABLE IF EXISTS customer_segments;

-- Aggregated customer counters (written by Flink Job 1: customer aggregation)
CREATE TABLE IF NOT EXISTS customer_counters (
    account_id       String,
    customer_id      String,
    metric_name      String,
    metric_value     Float64,
    computed_at      DateTime
) ENGINE = ReplacingMergeTree(computed_at)
  PARTITION BY toYYYYMM(computed_at)
  ORDER BY (account_id, customer_id, metric_name);

-- Segment membership (written by segment evaluation workers)
CREATE TABLE IF NOT EXISTS segment_membership (
    account_id       String,
    customer_id      String,
    segment_name     String,
    in_segment       UInt8,
    evaluated_at     DateTime
) ENGINE = ReplacingMergeTree(evaluated_at)
  ORDER BY (account_id, customer_id, segment_name);

-- Account-level metrics (written by Flink Job 2: account aggregation)
CREATE TABLE IF NOT EXISTS account_metrics (
    account_id       String,
    metric_name      String,
    metric_value     Float64,
    computed_at      DateTime
) ENGINE = ReplacingMergeTree(computed_at)
  ORDER BY (account_id, metric_name);

-- Decision log (written by decision engine)
CREATE TABLE IF NOT EXISTS decision_log (
    account_id       String,
    customer_id      String,
    arm_chosen       String,
    feature_snapshot String,
    created_at       DateTime DEFAULT now()
) ENGINE = MergeTree()
  PARTITION BY toYYYYMM(created_at)
  ORDER BY (account_id, customer_id, created_at);

-- Reward log (written by reward closure job)
CREATE TABLE IF NOT EXISTS reward_log (
    account_id       String,
    customer_id      String,
    arm_chosen       String,
    reward           Float64,
    created_at       DateTime DEFAULT now()
) ENGINE = MergeTree()
  PARTITION BY toYYYYMM(created_at)
  ORDER BY (account_id, customer_id, created_at);
