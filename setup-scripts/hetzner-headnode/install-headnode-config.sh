#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE_DIR="${SCRIPT_DIR}/templates"

require_var() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "missing required env var: ${name}" >&2
    exit 1
  fi
}

render_template() {
  local source="$1"
  local target="$2"
  install -m 0644 /dev/null "${target}"
  sed \
    -e "s|__HEAD_TAILNET_IP__|${HEAD_TAILNET_IP}|g" \
    -e "s|__ODYN_SYNC_TOKEN__|${ODYN_SYNC_TOKEN}|g" \
    -e "s|__RAY_HEAD_IMAGE__|${RAY_HEAD_IMAGE}|g" \
    "${source}" > "${target}"
}

main() {
  require_var HEAD_TAILNET_IP
  require_var ODYN_SYNC_TOKEN
  require_var RAY_HEAD_IMAGE
  install -d /opt/odyn-cp /opt/odyn/ray-head/env /opt/odyn/nginx
  render_template "${TEMPLATE_DIR}/opt/odyn-cp/odyn-cp.env" "/opt/odyn-cp/odyn-cp.env"
  render_template "${TEMPLATE_DIR}/opt/odyn/ray-head/env/ray-head.env" "/opt/odyn/ray-head/env/ray-head.env"
  install -m 0644 "${TEMPLATE_DIR}/opt/odyn/nginx/nginx-proxy.conf" "/opt/odyn/nginx/nginx-proxy.conf"
  install -m 0644 "${TEMPLATE_DIR}/etc/systemd/system/odyn-cp.service" "/etc/systemd/system/odyn-cp.service"
}

main
