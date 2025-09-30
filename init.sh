# bij wijziging volgende commando uitvoeren: chmod +x init.sh

#!/usr/bin/env bash
set -euo pipefail

# Zorg dat de airflow CLI van de airflow-user ook voor root vindbaar is
export PATH="/home/airflow/.local/bin:${PATH}"

# ──────────────────────────────────────────────────────────────────────────────
# Config / defaults
# ──────────────────────────────────────────────────────────────────────────────
AIRFLOW_HOME="${AIRFLOW_HOME:-/opt/airflow}"
LOG_DIR="${AIRFLOW_HOME}/logs"
DAGS_DIR="${AIRFLOW_HOME}/dags"
PLUGINS_DIR="${AIRFLOW_HOME}/plugins"

# Gebruik dezelfde UID als de runtime containers (scheduler/webserver/worker)
TARGET_UID="${AIRFLOW_UID:-}"
if [[ -z "${TARGET_UID}" ]]; then
  # fallback: probeer de airflow user, anders 50000 (Airflow default)
  TARGET_UID="$(id -u airflow 2>/dev/null || echo 50000)"
fi
TARGET_GID="0"   # root group, zodat setgid werkt en alle containers kunnen schrijven

# Admin user (optioneel/idempotent)
AF_ADMIN_USERNAME="${AF_ADMIN_USERNAME:-admin}"
AF_ADMIN_PASSWORD="${AF_ADMIN_PASSWORD:-admin}"
AF_ADMIN_FIRSTNAME="${AF_ADMIN_FIRSTNAME:-Airflow}"
AF_ADMIN_LASTNAME="${AF_ADMIN_LASTNAME:-Admin}"
AF_ADMIN_EMAIL="${AF_ADMIN_EMAIL:-admin@example.com}"

SQL_CONN="${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:-${AIRFLOW__CORE__SQL_ALCHEMY_CONN:-}}"

# ──────────────────────────────────────────────────────────────────────────────
log() { printf '%s %s\n' "$(date +'%Y-%m-%d %H:%M:%S')" "$*"; }
die() { echo "FATAL: $*" >&2; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Command not found: $1"
}

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
perm_fix_dir_tree() {
  # Zorg voor setgid op directories zodat nieuwe subdirs de groep en perms erven
  install -d -m 2775 -o "${TARGET_UID}" -g "${TARGET_GID}" "${LOG_DIR}" "${DAGS_DIR}" "${PLUGINS_DIR}"

  # Forceer owner:group en permissies
  chown -R "${TARGET_UID}:${TARGET_GID}" "${LOG_DIR}" "${DAGS_DIR}" "${PLUGINS_DIR}" || true

  # Dirs 2775 (rwx voor user/group + setgid), files 664
  find "${LOG_DIR}"     -type d -exec chmod 2775 {} \; 2>/dev/null || true
  find "${DAGS_DIR}"    -type d -exec chmod 2775 {} \; 2>/dev/null || true
  find "${PLUGINS_DIR}" -type d -exec chmod 2775 {} \; 2>/dev/null || true

  find "${LOG_DIR}"     -type f -exec chmod 664 {} \; 2>/dev/null || true
  find "${DAGS_DIR}"    -type f -exec chmod 664 {} \; 2>/dev/null || true
  find "${PLUGINS_DIR}" -type f -exec chmod 664 {} \; 2>/dev/null || true

  # Nieuwe bestanden krijgen group-writable perms
  umask 002
}

wait_for_postgres() {
  # Wacht op zowel DNS als TCP door 'airflow db check' te herhalen
  local retries="${1:-60}"
  local sleep_s="${2:-2}"

  [[ -z "${SQL_CONN}" ]] && die "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN (of CORE) is niet gezet."

  log "==> Wait for Postgres (airflow db check)"
  local i
  for i in $(seq 1 "${retries}"); do
    if gosu "${TARGET_UID}:${TARGET_GID}" airflow db check >/dev/null 2>&1; then
      log "   Postgres reachable"
      return 0
    fi
    sleep "${sleep_s}"
  done
  die "Database niet bereikbaar na $((retries*sleep_s))s"
}

db_init_or_upgrade() {
  log "==> Database check/init/upgrade"
  if gosu "${TARGET_UID}:${TARGET_GID}" airflow db check >/dev/null 2>&1; then
    # Probeer migraties (idempotent)
    if gosu "${TARGET_UID}:${TARGET_GID}" airflow db check-migrations >/dev/null 2>&1; then
      log "   migrations up-to-date"
    else
      log "   applying upgrades…"
      gosu "${TARGET_UID}:${TARGET_GID}" airflow db upgrade
    fi
  else
    # Eerste keer init
    log "   running db init…"
    gosu "${TARGET_UID}:${TARGET_GID}" airflow db init || true
    log "   applying upgrades…"
    gosu "${TARGET_UID}:${TARGET_GID}" airflow db upgrade || true
  fi
}

ensure_admin_user() {
  log "==> Ensure admin user"
  # Idempotent: 'users create' faalt als user bestaat → negeren we.
  # Gebruik ROLE Admin, zodat je meteen toegang hebt tot UI.
  if ! timeout 60s gosu "${TARGET_UID}:${TARGET_GID}" airflow users create \
      --username "${AF_ADMIN_USERNAME}" \
      --password "${AF_ADMIN_PASSWORD}" \
      --firstname "${AF_ADMIN_FIRSTNAME}" \
      --lastname "${AF_ADMIN_LASTNAME}" \
      --role Admin \
      --email "${AF_ADMIN_EMAIL}"; then
    log "   WARN: create skip/timeout"
  fi
}

show_env_summary() {
  log "==> Environment summary"
  log "   AIRFLOW_HOME=${AIRFLOW_HOME}"
  log "   SQL_CONN=$( [[ -n "${SQL_CONN}" ]] && echo OK || echo MISSING )"
  log "   Will enforce permissions for uid=${TARGET_UID}:gid=${TARGET_GID}"
}

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
require_cmd gosu
# Check airflow-CLI als target user, met expliciete PATH fallback
gosu "${TARGET_UID:-50000}:${TARGET_GID:-0}" env PATH="/home/airflow/.local/bin:${PATH}" \
  bash -lc 'command -v airflow >/dev/null' \
  || die "airflow CLI niet gevonden in PATH voor uid=${TARGET_UID:-50000}"

require_cmd install

log "==> Start airflow-init"
show_env_summary

log "==> Enforce permissions for logs/dags/plugins"
perm_fix_dir_tree

wait_for_postgres

db_init_or_upgrade

ensure_admin_user

# Nogmaals permissies zetten (dag-submap kan tijdens init zijn aangemaakt)
perm_fix_dir_tree

log "==> Init done"


