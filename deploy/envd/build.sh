#!/usr/bin/env bash
# Build E2B's envd (the in-sandbox daemon AGS speaks to) from e2b-dev/infra, for running AGS-style
# sandboxes locally behind the mock AGS. Outside Firecracker it needs `-isnotfc -no-cgroups`
# (set provider.envd_flags). Produces deploy/envd/envd and the local image sa-envd:dev.
#   ENVD_REF=<git ref> (default main)   ENVD_REBUILD=1 to rebuild an existing binary
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -x envd ] || [ -n "${ENVD_REBUILD:-}" ]; then
  REF=${ENVD_REF:-main}
  SRC=${ENVD_SRC:-$PWD/.src}
  if [ ! -d "$SRC/.git" ]; then
    git clone -q --filter=blob:none --no-checkout https://github.com/e2b-dev/infra "$SRC"
    git -C "$SRC" sparse-checkout set packages/envd packages/shared  # envd's go.mod replaces ../shared
  fi
  git -C "$SRC" fetch -q origin "$REF" && git -C "$SRC" checkout -q FETCH_HEAD
  # envd pins a newer Go than distros ship; GOTOOLCHAIN=auto fetches it. GOWORK=off builds the
  # module on its own instead of through the monorepo workspace.
  (cd "$SRC/packages/envd" && GOWORK=off GOTOOLCHAIN=auto CGO_ENABLED=0 go build -o "$OLDPWD/envd" .)
fi
echo "envd $(./envd -version)"

if command -v docker >/dev/null; then
  printf 'FROM scratch\nCOPY envd /usr/bin/envd\n' | DOCKER_BUILDKIT=0 docker build -q -t sa-envd:dev -f - .
fi
