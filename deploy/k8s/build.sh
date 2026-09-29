#!/usr/bin/env bash
# Build the four images offline: dependencies come from a wheelhouse and Gitea from its release
# binary, so the image builds need neither package mirrors nor Docker Hub beyond the base image.
#   KIND_CLUSTER=<name> deploy/k8s/build.sh    # also load the images into that kind cluster
set -euo pipefail
cd "$(dirname "$0")/../.."

BASE=${PYTHON_IMAGE:-python:3.11-bookworm}
GITEA_VERSION=${GITEA_VERSION:-1.27.3}

docker image inspect "$BASE" >/dev/null 2>&1 || docker pull "$BASE"
# A local-only tag: the builder cannot mistake it for something to re-check on Docker Hub.
docker tag "$BASE" sa-base:dev
LOCAL_BASE=sa-base:dev

if [ ! -x deploy/gitea/gitea ]; then
  curl -fsSL -o deploy/gitea/gitea \
    "https://github.com/go-gitea/gitea/releases/download/v${GITEA_VERSION}/gitea-${GITEA_VERSION}-linux-amd64"
  chmod +x deploy/gitea/gitea
fi

mkdir -p deploy/wheels
# Build the wheelhouse with the base image's interpreter: wheels built by the host's Python (another
# minor version) do not install in the image, and PIP_NO_INDEX leaves no fallback.
docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp -v "$PWD:/src" -w /src "$LOCAL_BASE" \
  python -m pip wheel -q --wheel-dir deploy/wheels ".[ags,k8s,worker]" hatchling
rm -f deploy/wheels/scaling_agent-*.whl

# The classic builder uses the local base image without asking the registry (no Hub rate limits).
export DOCKER_BUILDKIT=0
for target in coord launcher worker; do
  docker build -q --build-arg PYTHON_IMAGE="$LOCAL_BASE" --build-arg PIP_NO_INDEX=1 \
    -f deploy/Dockerfile --target "$target" -t "sa-$target:dev" .
done
docker build -q --build-arg BASE_IMAGE="$LOCAL_BASE" -f deploy/gitea/Dockerfile -t sa-gitea:dev deploy/gitea
docker images --format '{{.Repository}}:{{.Tag}}  {{.Size}}' | grep -E '^sa-(coord|launcher|worker|gitea):dev '

# kind nodes have their own containerd and cannot see this Docker daemon's images.
if [ -n "${KIND_CLUSTER:-}" ]; then
  kind load docker-image --name "$KIND_CLUSTER" sa-coord:dev sa-launcher:dev sa-worker:dev sa-gitea:dev
fi
