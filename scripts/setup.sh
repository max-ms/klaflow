#!/usr/bin/env bash
set -euo pipefail

echo "=== Klaflow Setup ==="

# Check prerequisites
MISSING=()
command -v docker >/dev/null 2>&1 || MISSING+=("docker")
command -v kind >/dev/null 2>&1 || MISSING+=("kind")
command -v kubectl >/dev/null 2>&1 || MISSING+=("kubectl")
command -v terraform >/dev/null 2>&1 || MISSING+=("terraform")
command -v python3 >/dev/null 2>&1 || MISSING+=("python3")

if [ ${#MISSING[@]} -gt 0 ]; then
    echo "ERROR: Missing prerequisites: ${MISSING[*]}"
    echo "Install with: brew install ${MISSING[*]}"
    exit 1
fi

# Check Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker is not running. Start Docker Desktop first."
    exit 1
fi

echo "All prerequisites found."

# Check Python version
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
echo "Python version: $PYTHON_VERSION"

# Create kind cluster if not exists
if kind get clusters 2>/dev/null | grep -q "klaflow-us"; then
    echo "Cluster klaflow-us already exists."
else
    echo "Creating kind cluster: klaflow-us..."
    kind create cluster --name klaflow-us
fi

# Verify cluster
kubectl cluster-info --context kind-klaflow-us

echo ""
echo "=== Setup complete ==="
echo "Run ./scripts/deploy.sh to deploy infrastructure."
