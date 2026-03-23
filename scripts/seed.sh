#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

MODE="${1:-seed}"

# Shared connection config — single source of truth
KAFKA_BOOTSTRAP="kafka.streaming.svc.cluster.local:9092"
SCHEMA_REGISTRY_URL="http://schema-registry.streaming.svc.cluster.local:8081"
CLICKHOUSE_HOST="clickhouse.analytics.svc.cluster.local"
CLICKHOUSE_PORT="8123"
CLICKHOUSE_USER="klaflow"
CLICKHOUSE_PASSWORD="klaflow-pass"
REDIS_HOST="redis-master.features.svc.cluster.local"
REDIS_PORT="6379"
ML_SCORING_URL="http://ml-scoring.decisions.svc.cluster.local:8000"

# ---------- Build producer image ----------
echo "--- Building producer image ---"
docker build -q -t klaflow-producer:latest -f "$PROJECT_DIR/docker/producer/Dockerfile" "$PROJECT_DIR"
kind load docker-image klaflow-producer:latest --name klaflow-us 2>/dev/null || true
echo ""

# Helper: run a k8s job-style pod and wait for completion
run_step() {
    local NAME="$1"
    local IMAGE="$2"
    local COMMAND="$3"
    shift 3
    local ENV_JSON="$*"

    echo "--- Step: $NAME ---"

    # Clean up any previous run
    kubectl -n processing delete pod "$NAME" --ignore-not-found=true 2>/dev/null || true
    sleep 1

    kubectl -n processing run "$NAME" \
        --image="$IMAGE" \
        --image-pull-policy=Never \
        --restart=Never \
        --overrides="{
            \"spec\":{\"containers\":[{
                \"name\":\"$NAME\",
                \"image\":\"$IMAGE\",
                \"imagePullPolicy\":\"Never\",
                \"env\":[$ENV_JSON],
                \"command\":$COMMAND
            }]}
        }"

    echo "  Waiting for $NAME to complete..."
    kubectl -n processing wait --for=condition=Ready pod/"$NAME" --timeout=60s 2>/dev/null || true
    kubectl -n processing logs -f "$NAME" || true
    kubectl -n processing wait --for=jsonpath='{.status.phase}'=Succeeded pod/"$NAME" --timeout=1800s 2>/dev/null || true
    echo ""
}

if [ "$MODE" = "single" ]; then
    echo "=== Klaflow — Single Event Injection ==="
    echo ""
    run_step "producer-single" "klaflow-producer:latest" \
        '["python","-u","producer.py","--mode","single"]' \
        "{\"name\":\"KAFKA_BOOTSTRAP\",\"value\":\"$KAFKA_BOOTSTRAP\"},{\"name\":\"SCHEMA_REGISTRY_URL\",\"value\":\"$SCHEMA_REGISTRY_URL\"}"

elif [ "$MODE" = "continuous" ]; then
    echo "=== Klaflow — Continuous Producer ==="
    echo ""
    # Clean up any previous run
    kubectl -n processing delete pod "producer-continuous" --ignore-not-found=true 2>/dev/null || true
    sleep 1
    kubectl -n processing run producer-continuous \
        --image=klaflow-producer:latest \
        --image-pull-policy=Never \
        --restart=Never \
        --overrides="{
            \"spec\":{\"containers\":[{
                \"name\":\"producer-continuous\",
                \"image\":\"klaflow-producer:latest\",
                \"imagePullPolicy\":\"Never\",
                \"env\":[{\"name\":\"KAFKA_BOOTSTRAP\",\"value\":\"$KAFKA_BOOTSTRAP\"},{\"name\":\"SCHEMA_REGISTRY_URL\",\"value\":\"$SCHEMA_REGISTRY_URL\"}],
                \"command\":[\"python\",\"-u\",\"producer.py\",\"--mode\",\"continuous\"]
            }]}
        }"
    echo "Producer running. Follow logs with:"
    echo "  kubectl -n processing logs -f producer-continuous"

elif [ "$MODE" = "seed" ]; then
    echo "=== Klaflow Pipeline Seed ==="
    echo "Loads 90 days of historical events and runs one-shot aggregation."
    echo ""

    # Step 1: Seed events
    run_step "producer-seed" "klaflow-producer:latest" \
        '["python","-u","producer.py","--mode","seed"]' \
        "{\"name\":\"KAFKA_BOOTSTRAP\",\"value\":\"$KAFKA_BOOTSTRAP\"},{\"name\":\"SCHEMA_REGISTRY_URL\",\"value\":\"$SCHEMA_REGISTRY_URL\"}"

    # Step 2: Wait for Flink jobs to process historical data
    echo "--- Waiting for Flink jobs to process historical data ---"
    echo "  Jobs flush every 10s. Waiting 60s for processing to complete..."
    sleep 60
    echo ""

    # Step 3: One-shot feature writer
    run_step "feature-writer-seed" "klaflow-feature-store:latest" \
        '["python","feature_writer.py","--full"]' \
        "{\"name\":\"CLICKHOUSE_HOST\",\"value\":\"$CLICKHOUSE_HOST\"},{\"name\":\"CLICKHOUSE_PORT\",\"value\":\"$CLICKHOUSE_PORT\"},{\"name\":\"CLICKHOUSE_USER\",\"value\":\"$CLICKHOUSE_USER\"},{\"name\":\"CLICKHOUSE_PASSWORD\",\"value\":\"$CLICKHOUSE_PASSWORD\"},{\"name\":\"REDIS_HOST\",\"value\":\"$REDIS_HOST\"},{\"name\":\"REDIS_PORT\",\"value\":\"$REDIS_PORT\"},{\"name\":\"ML_SCORING_URL\",\"value\":\"$ML_SCORING_URL\"}"

    # Step 4: One-shot segment evaluation
    run_step "segment-eval-seed" "klaflow-segment-worker:latest" \
        '["python","segment_evaluator.py","--full"]' \
        "{\"name\":\"CLICKHOUSE_HOST\",\"value\":\"$CLICKHOUSE_HOST\"},{\"name\":\"CLICKHOUSE_PORT\",\"value\":\"$CLICKHOUSE_PORT\"},{\"name\":\"CLICKHOUSE_USER\",\"value\":\"$CLICKHOUSE_USER\"},{\"name\":\"CLICKHOUSE_PASSWORD\",\"value\":\"$CLICKHOUSE_PASSWORD\"}"

    echo "=== Pipeline seed complete ==="
    echo ""
    echo "Historical data is loaded. The streaming pipeline continues running."
    echo "Verify results:"
    echo "  kubectl port-forward svc/klaflow-api 8000:80 -n api &"
    echo "  curl -s http://localhost:8000/segments | python3 -m json.tool"

else
    echo "Usage: seed.sh [seed|single|continuous]"
    echo "  seed       — load 90 days of historical data (default)"
    echo "  single     — inject one event for e2e tracing"
    echo "  continuous — start a long-running producer pod"
    exit 1
fi
