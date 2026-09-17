#!/bin/sh
# Writes the browser runtime config from environment variables at container start. Contains URLs only, never secrets.
set -eu
js_str() { printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'; }
cat > /usr/share/nginx/html/config.js <<EOF
window.__FRAUDWATCH_CONFIG__ = {
  apiBaseUrl: "$(js_str "${API_BASE_URL:-}")",
  liveUrl: "$(js_str "${LIVE_APP_URL:-}")",
  labUrl: "$(js_str "${LAB_APP_URL:-}")",
  controlUrl: "$(js_str "${CONTROL_URL:-}")"
};
EOF
