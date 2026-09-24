#!/usr/bin/env bash
# Deploy the infrastructure and start one run.
#   deploy/k8s/up.sh [run-file] [task-file]
# Images must already be available to the cluster (deploy/k8s/build.sh; for kind/k3d, load them).
set -euo pipefail
cd "$(dirname "$0")/../.."
NS=scaling-agent
RUN_FILE=${1:-deploy/k8s/run.scripted.yaml}
TASK_FILE=${2:-examples/tasks/example_task.md}

kubectl apply -f deploy/k8s/infra.yaml

if ! kubectl -n "$NS" get secret sa-secrets >/dev/null 2>&1; then
  args=(--from-literal=coord-admin-token="$(openssl rand -hex 24)"
        --from-literal=gitea-admin-user=root
        --from-literal=gitea-admin-password="$(openssl rand -hex 16)")
  if [ -n "${ANTHROPIC_API_KEY:-}" ]; then args+=(--from-literal=anthropic-api-key="$ANTHROPIC_API_KEY"); fi
  kubectl -n "$NS" create secret generic sa-secrets "${args[@]}"
fi

kubectl -n "$NS" rollout status deploy/gitea --timeout=300s
kubectl -n "$NS" rollout status deploy/coord --timeout=300s

# Runs against the local mock AGS: deploy it with freshly generated signing keys. (For Tencent
# Cloud, create sa-tencentcloud yourself from the real AK/SK and point the run file at AGS.)
if grep -Eq '^\s*endpoint:\s*https?://mock-ags' "$RUN_FILE"; then
  if ! kubectl -n "$NS" get secret sa-tencentcloud >/dev/null 2>&1; then
    kubectl -n "$NS" create secret generic sa-tencentcloud \
      --from-literal=secret-id="AKIDmock$(openssl rand -hex 12)" --from-literal=secret-key="$(openssl rand -hex 20)"
  fi
  kubectl apply -f deploy/k8s/mock-ags.yaml
  kubectl -n "$NS" rollout status deploy/mock-ags --timeout=300s
fi

kubectl -n "$NS" create configmap sa-run \
  --from-file=run.yaml="$RUN_FILE" --from-file=task.md="$TASK_FILE" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" delete job sa-launch --ignore-not-found --wait=true
kubectl apply -f deploy/k8s/launcher-job.yaml

echo
echo "watch:   kubectl -n $NS get pods -w"
echo "logs:    kubectl -n $NS logs -f job/sa-launch"
echo "status:  kubectl -n $NS exec deploy/coord -- scaling-agent status"
