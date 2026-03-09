#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TF_DIR="$PROJECT_DIR/terraform"

echo "=== Klaflow Deploy ==="

# Verify cluster is running
if ! kubectl cluster-info --context kind-klaflow-us >/dev/null 2>&1; then
    echo "ERROR: Cluster klaflow-us is not running. Run ./scripts/setup.sh first."
    exit 1
fi

# Build Docker images and load into kind
echo "Building Docker images..."
docker build -t klaflow-producer:latest -f "$PROJECT_DIR/docker/producer/Dockerfile" "$PROJECT_DIR"
docker build -t klaflow-flink-job:latest -f "$PROJECT_DIR/docker/flink_job/Dockerfile" "$PROJECT_DIR"
docker build -t klaflow-api:latest -f "$PROJECT_DIR/docker/api/Dockerfile" "$PROJECT_DIR"

echo "Loading images into kind cluster..."
kind load docker-image klaflow-producer:latest --name klaflow-us
kind load docker-image klaflow-flink-job:latest --name klaflow-us
kind load docker-image klaflow-api:latest --name klaflow-us

# Terraform init and apply
echo "Running Terraform..."
cd "$TF_DIR"
terraform init
terraform apply -auto-approve

# Wait for pods to be ready
echo "Waiting for pods to be ready..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=kafka -n streaming --timeout=300s 2>/dev/null || true
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=clickhouse -n analytics --timeout=300s 2>/dev/null || true

# Create __consumer_offsets topic if it doesn't exist (required for Schema Registry)
echo "Ensuring __consumer_offsets topic exists..."
KAFKA_POD=$(kubectl get pod -n streaming -l app.kubernetes.io/name=kafka -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [ -n "$KAFKA_POD" ]; then
    kubectl exec -n streaming "$KAFKA_POD" -- /opt/bitnami/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --create --if-not-exists \
        --topic __consumer_offsets --partitions 50 --replication-factor 1 \
        --config cleanup.policy=compact 2>/dev/null || true
fi

# Wait for Schema Registry to be ready
echo "Waiting for Schema Registry..."
kubectl wait --for=condition=available deployment/schema-registry -n streaming --timeout=300s 2>/dev/null || true

# Apply ClickHouse schema
echo "Applying ClickHouse schema..."
CH_POD=$(kubectl get pod -n analytics -l app.kubernetes.io/name=clickhouse -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [ -n "$CH_POD" ]; then
    kubectl cp "$PROJECT_DIR/src/clickhouse/schema.sql" "analytics/$CH_POD:/tmp/schema.sql"
    kubectl exec -n analytics "$CH_POD" -- clickhouse-client --user klaflow --password klaflow-pass --query "$(cat "$PROJECT_DIR/src/clickhouse/schema.sql")" 2>/dev/null || \
    kubectl exec -n analytics "$CH_POD" -- bash -c "clickhouse-client --user klaflow --password klaflow-pass < /tmp/schema.sql" 2>/dev/null || \
    echo "Warning: Could not apply schema automatically. Apply manually."
fi

echo ""
echo "=== Deploy complete ==="
echo "Check status: kubectl get all -A"
