# Gitea for the shared workspace, built from the upstream release binary.
# Why not gitea/gitea from Docker Hub: behind strict egress (or Hub rate limits) the release binary
# from GitHub is often the only reachable artifact. deploy/k8s/up.sh downloads it next to this file.
#   curl -L -o deploy/gitea/gitea https://github.com/go-gitea/gitea/releases/download/v1.27.3/gitea-1.27.3-linux-amd64
#   docker build -f deploy/gitea/Dockerfile -t sa-gitea:dev deploy/gitea
# The base image only needs git; python:3.11-bookworm is reused because the other images build on it.
ARG BASE_IMAGE=python:3.11-bookworm
FROM ${BASE_IMAGE}
COPY gitea /usr/local/bin/gitea
COPY entrypoint.sh /usr/local/bin/sa-gitea-entrypoint
RUN chmod 0755 /usr/local/bin/gitea /usr/local/bin/sa-gitea-entrypoint \
 && useradd -m -u 1000 -s /bin/sh git \
 && mkdir -p /data && chown git:git /data
USER git
ENV GITEA_WORK_DIR=/data HOME=/home/git
VOLUME /data
EXPOSE 3000
ENTRYPOINT ["/usr/local/bin/sa-gitea-entrypoint"]
