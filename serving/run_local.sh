#!/usr/bin/env bash
# Runs the ops API (HTTP + MCP) on this laptop against the cluster's Redis.
# Needs: the cluster running (nodes up), kubectl pointed at it, AWS
# credentials for S3/Glue reads. Ctrl+C stops both the tunnel and the API.
#
#   serving/run_local.sh            # API on http://localhost:8000, MCP at /mcp
#
# MCP clients that read a project .mcp.json pick the server up in any new session
# opened in this project (.mcp.json stays out of git: it points at localhost).
set -euo pipefail
cd "$(dirname "$0")"

if ! kubectl get nodes --no-headers 2>/dev/null | grep -q " Ready"; then
  echo "No Ready nodes: start the cluster first (scale the node group to 2)." >&2
  exit 1
fi

# Tunnel to Redis; stopped again when this script exits.
kubectl -n redis port-forward svc/redis 16379:6379 >/dev/null &
TUNNEL=$!
trap 'kill $TUNNEL 2>/dev/null' EXIT
sleep 2

# The API logs in as the read-only Redis user; the password stays in the
# cluster's Secret and only lives in this process's environment.
REDIS_PASSWORD=$(kubectl -n serving get secret redis-api -o jsonpath='{.data.password}' | base64 -d)
export REDIS_PASSWORD

echo "API: http://localhost:8000  (MCP: http://localhost:8000/mcp, docs: http://localhost:8000/docs)"
.venv/bin/uvicorn app.main:app --port 8000
