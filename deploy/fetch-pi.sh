#!/usr/bin/env bash
# Put the pi coding agent (npm @earendil-works/pi-coding-agent) into deploy/pi/, which the worker
# image copies. Run by deploy/k8s/build.sh; run it yourself before a plain `docker build`.
# Uses the host's npm, so its registry and proxy settings apply; the tree is plain JavaScript.
#   PI_VERSION=0.87.1 deploy/fetch-pi.sh
set -euo pipefail
cd "$(dirname "$0")/pi"
VERSION=${PI_VERSION:-0.87.1}
if [ "$(cat .version 2>/dev/null)" = "$VERSION" ] && [ -e node_modules/.bin/pi ]; then
  exit 0
fi
rm -rf node_modules package.json package-lock.json .version
# No lockfile: a lockfile written through a mirror pins the mirror's URLs.
npm install --prefix . --omit=dev --no-audit --no-fund --no-package-lock --ignore-scripts \
  --os linux --cpu x64 --libc glibc "@earendil-works/pi-coding-agent@${VERSION}"
# pi's shrinkwrap pins esbuild's binaries for every platform (~285 MB); the image only runs linux-x64.
find node_modules -type d -path '*/@esbuild/*' -maxdepth 6 -mindepth 4 ! -name linux-x64 -prune -exec rm -rf {} +
echo "$VERSION" > .version
