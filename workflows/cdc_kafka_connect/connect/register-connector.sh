#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKFLOW_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${WORKFLOW_DIR}/../.." && pwd)"

ENV_FILE="${REPO_ROOT}/.env"
MAP_FILE="${SCRIPT_DIR}/topic-table-map.env"
TEMPLATE="${SCRIPT_DIR}/snowflake-sink-template.json"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing .env at ${ENV_FILE}"
  exit 1
fi

if [[ ! -f "${MAP_FILE}" ]]; then
  echo "Missing topic-table-map.env at ${MAP_FILE}"
  exit 1
fi

set -a
source "${ENV_FILE}"
source "${MAP_FILE}"
set +a

required=(
  SNOWFLAKE_ACCOUNT
  SNOWFLAKE_USER
  SNOWFLAKE_ROLE
  SNOWFLAKE_DATABASE
  SNOWFLAKE_PRIVATE_KEY_PATH
)
for k in "${required[@]}"; do
  if [[ -z "${!k:-}" ]]; then
    echo "Missing required env var: $k"
    exit 1
  fi
done

SNOWFLAKE_RAW_SCHEMA="${SNOWFLAKE_RAW_SCHEMA:-${SNOWFLAKE_SCHEMA}}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"

if [[ ! -f "${SNOWFLAKE_PRIVATE_KEY_PATH}" ]]; then
  echo "Private key not found at ${SNOWFLAKE_PRIVATE_KEY_PATH}"
  exit 1
fi

private_key="$(
  sed -e '/-----BEGIN/d' -e '/-----END/d' "${SNOWFLAKE_PRIVATE_KEY_PATH}" | tr -d '\n\r'
)"

payload="$(mktemp)"

sed \
  -e "s|__TOPICS__|${TOPICS}|g" \
  -e "s|__TOPIC_TABLE_MAP__|${TOPIC_TABLE_MAP}|g" \
  -e "s|__SNOWFLAKE_ACCOUNT__|${SNOWFLAKE_ACCOUNT}|g" \
  -e "s|__SNOWFLAKE_USER__|${SNOWFLAKE_USER}|g" \
  -e "s|__SNOWFLAKE_ROLE__|${SNOWFLAKE_ROLE}|g" \
  -e "s|__SNOWFLAKE_DATABASE__|${SNOWFLAKE_DATABASE}|g" \
  -e "s|__SNOWFLAKE_RAW_SCHEMA__|${SNOWFLAKE_RAW_SCHEMA}|g" \
  -e "s|__SNOWFLAKE_PRIVATE_KEY__|${private_key}|g" \
  "${TEMPLATE}" > "${payload}"

echo "Registering connector at ${CONNECT_URL} (schema=${SNOWFLAKE_RAW_SCHEMA})..."
response_file="$(mktemp)"
http_code="$(
  curl -sS -o "${response_file}" -w "%{http_code}" -X PUT \
    -H "Content-Type: application/json" \
    --data @"${payload}" \
    "${CONNECT_URL}/connectors/snowflake-cdc-sink/config"
)"

if [[ "${http_code}" =~ ^2 ]]; then
  echo "Connector upserted: snowflake-cdc-sink"
else
  echo "Connector registration failed (HTTP ${http_code}):"
  cat "${response_file}"
  echo
  exit 1
fi
