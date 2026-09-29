# syntax=docker/dockerfile:1
# One Dockerfile, one image per infrastructure role:
#   docker build -f deploy/Dockerfile --target coord      -t sa-coord:dev .
#   docker build -f deploy/Dockerfile --target launcher   -t sa-launcher:dev .
#   docker build -f deploy/Dockerfile --target worker     -t sa-worker:dev .
#   docker buildx build -f deploy/Dockerfile --target worker-ags --platform linux/amd64 \
#     -t sgccr.ccs.tencentyun.com/<ns>/sa-worker:v1 --push .     # AGS: adds envd
# Gitea uses the upstream gitea/gitea image.

# The full (non-slim) Python image already ships git, curl and a C toolchain, which the worker
# and the merge-queue verifier need; no package-manager step, so builds work behind strict egress.
ARG PYTHON_IMAGE=python:3.11-bookworm
ARG GITEA_IMAGE=gitea/gitea:1.27.3

FROM ${PYTHON_IMAGE} AS base
RUN git --version
WORKDIR /opt/scaling-agent
# Optional offline wheelhouse (deploy/k8s/up.sh fills it from the host). With PIP_NO_INDEX=1
# installs use only these wheels, so builds need no network access at all.
ARG PIP_NO_INDEX=0
ENV PIP_NO_INDEX=${PIP_NO_INDEX} PIP_FIND_LINKS=/wheels PIP_NO_CACHE_DIR=1
COPY deploy/wheels/ /wheels/
COPY pyproject.toml README.md ./
COPY src ./src

# Coordination server: board, claims, messages, event delivery, merge queue.
FROM base AS coord
RUN pip install .
ENV SA_COORD_DB_PATH=/data/coord.sqlite3
EXPOSE 8700
CMD ["scaling-agent", "coord", "serve"]

# Launcher: provisions worker sandboxes (AGS or Kubernetes pods) and supervises the run.
FROM base AS launcher
RUN pip install ".[ags,k8s]"
CMD ["scaling-agent", "--help"]

# Worker: the runtime plus the harness (claude-agent-sdk bundles the Claude Code CLI).
# Extend this stage with the toolchain your task needs.
FROM base AS worker
RUN pip install ".[worker]"
RUN mkdir -p /workspace
WORKDIR /workspace
CMD ["scaling-agent", "worker", "run"]

# Worker for Tencent Cloud AGS: AGS needs envd in the image and ignores CMD/ENTRYPOINT
# (the sandbox tool's Command/Args start envd; the launcher starts the runtime through envd).
FROM ccr.ccs.tencentyun.com/ags-image/envd:v0.5.14 AS envd
FROM worker AS worker-ags
COPY --from=envd /usr/bin/envd /usr/bin/envd
RUN chmod 0755 /usr/bin/envd
CMD ["/usr/bin/envd", "-port", "49983"]

# Organization infrastructure (Gitea + coordination server) in one AGS sandbox, for when worker
# sandboxes cannot reach any host of yours. scripts/ags_infra.py starts it and exposes both ports
# through AGS port forwarding. The Gitea binary in the upstream image is statically linked.
#   docker build -f deploy/Dockerfile --target infra-ags -t sa-infra-ags:dev .
FROM ${GITEA_IMAGE} AS gitea
FROM coord AS infra-ags
COPY --from=gitea /app/gitea/gitea /usr/local/bin/gitea
COPY deploy/gitea/entrypoint.sh /usr/local/bin/sa-gitea-entrypoint
COPY --from=envd /usr/bin/envd /usr/bin/envd
RUN chmod 0755 /usr/local/bin/gitea /usr/local/bin/sa-gitea-entrypoint /usr/bin/envd \
 && useradd -m -u 1000 -s /bin/sh git \
 && mkdir -p /data && chown git:git /data
CMD ["/usr/bin/envd", "-port", "49983"]
