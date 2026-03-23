#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TF_DIR="$PROJECT_DIR/terraform"

# Load .env if it exists (for ANTHROPIC_API_KEY etc.)
if [ -f "$PROJECT_DIR/.env" ]; then
    export $(grep -v '^#' "$PROJECT_DIR/.env" | grep -v '^\s*$' | xargs)
fi

echo "=== Klaflow Deploy ==="

# ---------- Verify cluster ----------
if ! kubectl cluster-info --context kind-klaflow-us >/dev/null 2>&1; then
    echo "ERROR: Cluster klaflow-us is not running. Run ./scripts/setup.sh first."
    exit 1
fi

# ---------- Build images for Terraform-managed deployments ----------
# All long-running services deployed as k8s Deployments by Terraform,
# including the streaming pipeline (consumers, feature writer, segment worker, producer).
echo ""
echo "--- Building Docker images ---"

IMAGES=(
    "klaflow-api:docker/api/Dockerfile"
    "klaflow-ml-models:docker/ml_models/Dockerfile"
    "klaflow-decision-engine:docker/decision_engine/Dockerfile"
    "klaflow-agent:docker/agent/Dockerfile"
    "klaflow-ui:docker/ui/Dockerfile"
    "klaflow-flink-jobs:docker/flink_jobs/Dockerfile"
    "klaflow-feature-store:docker/feature_store/Dockerfile"
    "klaflow-segment-worker:docker/segment_worker/Dockerfile"
)

for entry in "${IMAGES[@]}"; do
    IMAGE_NAME="${entry%%:*}"
    DOCKERFILE="${entry#*:}"
    if [ -f "$PROJECT_DIR/$DOCKERFILE" ]; then
        echo "  Building $IMAGE_NAME..."
        # Flink image must use legacy builder for kind compatibility (OCI index format issue)
        if [ "$IMAGE_NAME" = "klaflow-flink-jobs" ]; then
            DOCKER_BUILDKIT=0 docker build -q -t "$IMAGE_NAME:latest" -f "$PROJECT_DIR/$DOCKERFILE" "$PROJECT_DIR"
        else
            docker build -q -t "$IMAGE_NAME:latest" -f "$PROJECT_DIR/$DOCKERFILE" "$PROJECT_DIR"
        fi
    else
        echo "  Skipping $IMAGE_NAME (no Dockerfile at $DOCKERFILE)"
    fi
done

echo ""
echo "--- Loading images into kind cluster ---"
for entry in "${IMAGES[@]}"; do
    IMAGE_NAME="${entry%%:*}"
    DOCKERFILE="${entry#*:}"
    if [ -f "$PROJECT_DIR/$DOCKERFILE" ]; then
        kind load docker-image "$IMAGE_NAME:latest" --name klaflow-us 2>/dev/null || true
    fi
done

# ---------- Terraform ----------
echo ""
echo "--- Running Terraform ---"
cd "$TF_DIR"
terraform init -input=false

TF_VARS=""
if [ -n "${ANTHROPIC_API_KEY:-}" ] && [ "$ANTHROPIC_API_KEY" != "your-key-here" ]; then
    TF_VARS="-var anthropic_api_key=$ANTHROPIC_API_KEY"
fi
terraform apply -auto-approve -input=false $TF_VARS

# ---------- Post-deploy setup ----------
echo ""
echo "--- Post-deploy setup ---"

# Wait for core pods
echo "Waiting for Kafka..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=kafka -n streaming --timeout=300s 2>/dev/null || true

echo "Waiting for ClickHouse..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=clickhouse -n analytics --timeout=300s 2>/dev/null || true

# Create __consumer_offsets topic (required for Schema Registry with KRaft)
KAFKA_POD=$(kubectl get pod -n streaming -l app.kubernetes.io/name=kafka -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [ -n "$KAFKA_POD" ]; then
    echo "Ensuring __consumer_offsets topic exists..."
    kubectl exec -n streaming "$KAFKA_POD" -- /opt/bitnami/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --create --if-not-exists \
        --topic __consumer_offsets --partitions 50 --replication-factor 1 \
        --config cleanup.policy=compact 2>/dev/null || true
fi

# Wait for Schema Registry
echo "Waiting for Schema Registry..."
kubectl wait --for=condition=available deployment/schema-registry -n streaming --timeout=300s 2>/dev/null || true

# Apply ClickHouse schema
CH_POD=$(kubectl get pod -n analytics -l app.kubernetes.io/component=clickhouse -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [ -n "$CH_POD" ]; then
    echo "Applying ClickHouse schema..."
    kubectl cp "$PROJECT_DIR/src/clickhouse/schema.sql" "analytics/$CH_POD:/tmp/schema.sql"
    kubectl exec -n analytics "$CH_POD" -- bash -c "clickhouse-client --user klaflow --password klaflow-pass < /tmp/schema.sql" 2>/dev/null || \
    echo "Warning: Could not apply schema automatically."
fi

echo ""
echo "=== Deploy complete ==="
echo ""
echo "Streaming pipeline is running (producer, consumers, feature writer, segment worker)."
echo "To load 90 days of historical data, run: ./scripts/seed.sh"
