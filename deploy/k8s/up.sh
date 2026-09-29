#!/usr/bin/env bash
# Deploy the infrastructure and start one run.
#   deploy/k8s/up.sh [run-file] [task-file]
# Images must already be available to the cluster (deploy/k8s/build.sh; for kind, KIND_CLUSTER=...).
# Targets the current kubectl context: point KUBECONFIG at the right cluster first.
#
# Model access for claude_code runs (non-secret settings such as ANTHROPIC_BASE_URL and
# ANTHROPIC_MODEL go into the run file's worker_env):
#   ANTHROPIC_AUTH_TOKEN / ANTHROPIC_API_KEY   stored in the sa-model Secret for the launcher, which
#                                               forwards the names listed in the run file's forward_env
#   MODEL_EGRESS=<cidr>:<port>[,...]           extra worker egress, e.g. a model gateway that is not
#                                               on port 443 or sits in a private range
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
  kubectl -n "$NS" create secret generic sa-secrets "${args[@]}"
fi

# Model credentials: refreshed on every call that provides them. printf is a builtin, so the values
# never appear on a command line.
if [ -n "${ANTHROPIC_AUTH_TOKEN:-}${ANTHROPIC_API_KEY:-}" ]; then
  kubectl -n "$NS" create secret generic sa-model --from-env-file=<(
    [ -z "${ANTHROPIC_AUTH_TOKEN:-}" ] || printf 'anthropic-auth-token=%s\n' "$ANTHROPIC_AUTH_TOKEN"
    [ -z "${ANTHROPIC_API_KEY:-}" ] || printf 'anthropic-api-key=%s\n' "$ANTHROPIC_API_KEY"
  ) --dry-run=client -o yaml | kubectl apply -f -
fi

if [ -n "${MODEL_EGRESS:-}" ]; then
  rules=""
  IFS=, read -ra targets <<<"$MODEL_EGRESS"
  for target in "${targets[@]}"; do
    rules+="    - to: [{ipBlock: {cidr: ${target%:*}}}]
      ports: [{port: ${target##*:}, protocol: TCP}]
"
  done
  kubectl apply -f - <<EOF
# Workers may also reach the model endpoints named in MODEL_EGRESS (deploy/k8s/up.sh).
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: {name: sa-worker-model, namespace: $NS}
spec:
  podSelector: {matchLabels: {app: sa-worker}}
  policyTypes: [Egress]
  egress:
$rules
EOF
else
  kubectl -n "$NS" delete networkpolicy sa-worker-model --ignore-not-found
fi

kubectl -n "$NS" rollout status deploy/gitea --timeout=300s
kubectl -n "$NS" rollout status deploy/coord --timeout=300s

kubectl -n "$NS" create configmap sa-run \
  --from-file=run.yaml="$RUN_FILE" --from-file=task.md="$TASK_FILE" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" delete job sa-launch --ignore-not-found --wait=true
kubectl apply -f deploy/k8s/launcher-job.yaml

echo
echo "watch:   kubectl -n $NS get pods -w"
echo "logs:    kubectl -n $NS logs -f job/sa-launch"
echo "status:  kubectl -n $NS exec deploy/coord -- scaling-agent status"
