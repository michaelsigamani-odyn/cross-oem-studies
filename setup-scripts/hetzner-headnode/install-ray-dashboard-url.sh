#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE_DIR="${SCRIPT_DIR}/templates/etc/nginx/sites-available"

require_var() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "missing required env var: ${name}" >&2
    exit 1
  fi
}

select_template() {
  if [[ "${RAY_DASHBOARD_AUTH_MODE:-basic}" == "public" ]]; then
    echo "${TEMPLATE_DIR}/ray-dashboard.public.conf"
    return
  fi
  echo "${TEMPLATE_DIR}/ray-dashboard.auth.conf"
}

render_nginx_config() {
  local source="$1"
  local target="$2"
  sed \
    -e "s|__RAY_DASHBOARD_SERVER_NAME__|${RAY_DASHBOARD_SERVER_NAME}|g" \
    -e "s|__RAY_DASHBOARD_UPSTREAM__|${RAY_DASHBOARD_UPSTREAM}|g" \
    "${source}" > "${target}"
}

configure_basic_auth() {
  if [[ "${RAY_DASHBOARD_AUTH_MODE:-basic}" != "basic" ]]; then
    return
  fi
  require_var RAY_DASHBOARD_BASIC_AUTH_USER
  require_var RAY_DASHBOARD_BASIC_AUTH_PASSWORD
  htpasswd -bc /etc/nginx/.ray-dashboard.htpasswd "${RAY_DASHBOARD_BASIC_AUTH_USER}" "${RAY_DASHBOARD_BASIC_AUTH_PASSWORD}"
}

enable_site() {
  ln -sf /etc/nginx/sites-available/ray-dashboard.conf /etc/nginx/sites-enabled/ray-dashboard.conf
  rm -f /etc/nginx/sites-enabled/default
}

provision_tls() {
  certbot --nginx \
    --non-interactive \
    --agree-tos \
    --email "${LETSENCRYPT_EMAIL}" \
    -d "${RAY_DASHBOARD_SERVER_NAME}"
}

main() {
  require_var RAY_DASHBOARD_SERVER_NAME
  require_var RAY_DASHBOARD_UPSTREAM
  require_var LETSENCRYPT_EMAIL
  apt-get update
  apt-get install -y nginx certbot python3-certbot-nginx apache2-utils
  configure_basic_auth
  render_nginx_config "$(select_template)" "/etc/nginx/sites-available/ray-dashboard.conf"
  enable_site
  nginx -t
  systemctl enable nginx
  systemctl restart nginx
  provision_tls
  systemctl reload nginx
}

main
