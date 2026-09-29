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
# minor version) do not install in the image, and PIP_NO_INDEX leaves no fallback. The host's proxy,
# pip settings and CA bundle go into the container, so it reaches the index wherever the host does
# (a proxy-only or TLS-intercepting egress is unreachable for a bare container). Assumes a rootful Docker.
wheel_args=(--network host)
for var in HTTP_PROXY HTTPS_PROXY NO_PROXY http_proxy https_proxy no_proxy PIP_INDEX_URL PIP_EXTRA_INDEX_URL PIP_TRUSTED_HOST PIP_CERT; do
  [ -z "${!var:-}" ] || wheel_args+=(-e "$var")
done
for conf in "$HOME/.pip/pip.conf" "$HOME/.config/pip/pip.conf" /etc/pip.conf; do
  [ ! -f "$conf" ] || { wheel_args+=(-v "$conf:/etc/pip.conf:ro"); break; }
done
[ ! -f /etc/ssl/certs/ca-certificates.crt ] ||
  wheel_args+=(-v /etc/ssl/certs/ca-certificates.crt:/etc/ssl/certs/ca-certificates.crt:ro)
docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp "${wheel_args[@]}" -v "$PWD:/src" -w /src "$LOCAL_BASE" \
  python -m pip wheel -q --wheel-dir deploy/wheels ".[ags,k8s,worker]" hatchling
rm -f deploy/wheels/scaling_agent-*.whl

# The pi harness: a Node program, fetched with the host's npm (deploy/fetch-pi.sh). WITH_PI=0 leaves
# it out (workers then cannot run harness: pi); the default takes it when npm is available.
WITH_PI=${WITH_PI:-auto}
if [ "$WITH_PI" = 1 ] || { [ "$WITH_PI" = auto ] && command -v npm >/dev/null 2>&1; }; then
  deploy/fetch-pi.sh
else
  echo "pi harness not included (WITH_PI=$WITH_PI, npm: $(command -v npm || echo missing))" >&2
fi
docker image inspect "${NODE_IMAGE:-node:24-bookworm-slim}" >/dev/null 2>&1 || docker pull "${NODE_IMAGE:-node:24-bookworm-slim}"

# The classic builder uses the local base image without asking the registry (no Hub rate limits).
export DOCKER_BUILDKIT=0
for target in coord launcher worker; do
  docker build -q --build-arg PYTHON_IMAGE="$LOCAL_BASE" --build-arg PIP_NO_INDEX=1 \
    --build-arg NODE_IMAGE="${NODE_IMAGE:-node:24-bookworm-slim}" \
    -f deploy/Dockerfile --target "$target" -t "sa-$target:dev" .
done
docker build -q --build-arg BASE_IMAGE="$LOCAL_BASE" -f deploy/gitea/Dockerfile -t sa-gitea:dev deploy/gitea
docker images --format '{{.Repository}}:{{.Tag}}  {{.Size}}' | grep -E '^sa-(coord|launcher|worker|gitea):dev '

# kind nodes have their own containerd and cannot see this Docker daemon's images.
if [ -n "${KIND_CLUSTER:-}" ]; then
  kind load docker-image --name "$KIND_CLUSTER" sa-coord:dev sa-launcher:dev sa-worker:dev sa-gitea:dev
fi
