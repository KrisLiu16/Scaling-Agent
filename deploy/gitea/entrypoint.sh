#!/bin/sh
# Headless Gitea for the shared workspace: config from env, DB migration, first admin, then serve.
set -eu

CONF=/data/app.ini
: "${GITEA_ROOT_URL:=http://gitea:3000/}"
: "${GITEA_ADMIN_USER:=root}"

if [ ! -f "$CONF" ]; then
  mkdir -p /data/repos /data/log
  cat > "$CONF" <<EOF
APP_NAME = scaling-agent workspace
RUN_MODE = prod
WORK_PATH = /data

[server]
HTTP_PORT = 3000
ROOT_URL = ${GITEA_ROOT_URL}
DISABLE_SSH = true
LFS_START_SERVER = false
OFFLINE_MODE = true

[database]
DB_TYPE = sqlite3
PATH = /data/gitea.db
SQLITE_JOURNAL_MODE = WAL

[repository]
ROOT = /data/repos
DEFAULT_BRANCH = main

[security]
INSTALL_LOCK = true
SECRET_KEY = $(gitea generate secret SECRET_KEY)
INTERNAL_TOKEN = $(gitea generate secret INTERNAL_TOKEN)
; webhooks go to the coordination server on the private network
ALLOWED_HOST_LIST = *

[service]
DISABLE_REGISTRATION = true
REQUIRE_SIGNIN_VIEW = true
AUTO_WATCH_NEW_REPOS = false

[oauth2]
ENABLE = false

[picture]
DISABLE_GRAVATAR = true

[cron.update_checker]
ENABLED = false

[api]
MAX_RESPONSE_ITEMS = 100

[log]
MODE = console
LEVEL = warn
EOF
fi

gitea --config "$CONF" migrate >/dev/null
if [ -n "${GITEA_ADMIN_PASSWORD:-}" ]; then
  if ! gitea --config "$CONF" admin user list --admin 2>/dev/null | awk '{print $2}' | grep -qx "$GITEA_ADMIN_USER"; then
    gitea --config "$CONF" admin user create --admin --username "$GITEA_ADMIN_USER" \
      --password "$GITEA_ADMIN_PASSWORD" --email "$GITEA_ADMIN_USER@agents.invalid" --must-change-password=false
  fi
fi
exec gitea --config "$CONF" web
