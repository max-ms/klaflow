#!/usr/bin/env bash
set -euo pipefail

echo "=== Klaflow Reset ==="
echo "Clears all event-derived data for a clean single-event trace."
echo ""

# ---------- Step 1: Suspend Flink jobs so they stop consuming ----------
echo "--- Suspending Flink jobs ---"
for job in customer-aggregation account-aggregation; do
    kubectl patch flinkdeployment "$job" -n processing --type=merge \
        -p '{"spec":{"job":{"state":"suspended"}}}' 2>/dev/null || true
done
# Wait for TaskManager pods to terminate
echo "  Waiting for job pods to stop..."
for job in customer-aggregation account-aggregation; do
    kubectl wait --for=delete pod -l app="$job" -n processing --timeout=60s 2>/dev/null || true
done
echo "  Flink jobs suspended."

# ---------- Step 2: Reset Kafka consumer group offsets to end ----------
echo ""
echo "--- Resetting Kafka consumer offsets ---"
KAFKA_POD=$(kubectl get pod -n streaming -l app.kubernetes.io/name=kafka -o jsonpath='{.items[0].metadata.name}')
for group in klaflow-customer-aggregation klaflow-account-aggregation; do
    echo "  Resetting $group to latest offset..."
    kubectl exec -n streaming "$KAFKA_POD" -c kafka -- /opt/bitnami/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server localhost:9092 --group "$group" \
        --topic customer-events --reset-offsets --to-latest --execute 2>/dev/null || true
done
echo "  Offsets reset."

# ---------- Step 3: Truncate ClickHouse tables ----------
echo ""
echo "--- Truncating ClickHouse tables ---"
CH_POD=$(kubectl get pod -n analytics -l app.kubernetes.io/component=clickhouse -o jsonpath='{.items[0].metadata.name}')
for table in customer_counters account_metrics segment_membership decision_log reward_log; do
    echo "  Truncating $table..."
    kubectl exec -n analytics "$CH_POD" -- clickhouse-client \
        --user klaflow --password klaflow-pass \
        --query "TRUNCATE TABLE IF EXISTS $table" 2>/dev/null || true
done

# ---------- Step 4: Clear Redis feature keys ----------
echo ""
echo "--- Clearing Redis feature keys ---"
REDIS_POD=$(kubectl get pod -n features -l app.kubernetes.io/name=redis -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n features "$REDIS_POD" -- redis-cli --scan --pattern 'customer:*' \
    | xargs -r -n 100 kubectl exec -n features "$REDIS_POD" -- redis-cli UNLINK 2>/dev/null || true
echo "  Done."

# ---------- Step 5: Resume Flink jobs ----------
echo ""
echo "--- Resuming Flink jobs ---"
for job in customer-aggregation account-aggregation; do
    kubectl patch flinkdeployment "$job" -n processing --type=merge \
        -p '{"spec":{"job":{"state":"running"}}}' 2>/dev/null || true
done
echo "  Waiting for jobs to start..."
sleep 10
# Wait for TaskManager pods to be ready
for job in customer-aggregation account-aggregation; do
    kubectl wait --for=condition=ready pod -l app="$job" -n processing --timeout=120s 2>/dev/null || true
done
echo "  Flink jobs running."

echo ""
echo "=== Reset complete ==="
echo "System is clean. Inject a single event with:"
echo "  ./scripts/seed.sh single"
