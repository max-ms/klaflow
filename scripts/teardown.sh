#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TF_DIR="$PROJECT_DIR/terraform"

echo "=== Klaflow Teardown ==="

# Terraform destroy
if [ -d "$TF_DIR/.terraform" ]; then
    echo "Running Terraform destroy..."
    cd "$TF_DIR"
    terraform destroy -auto-approve || true
fi

# Delete kind cluster
if kind get clusters 2>/dev/null | grep -q "klaflow-us"; then
    echo "Deleting kind cluster: klaflow-us..."
    kind delete cluster --name klaflow-us
else
    echo "Cluster klaflow-us does not exist."
fi

echo ""
echo "=== Teardown complete ==="
