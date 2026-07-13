#!/bin/bash
set -x

RAY_BIN="${RAY_BIN:-ray}"
RAY_NODE_NAME="${RAY_NODE_NAME:-radeon-runpod}"

# 1. Start SSH daemon
/usr/sbin/sshd

# 2. Start Tailscale daemon
mkdir -p /var/run/tailscale /var/lib/tailscale
tailscaled --state=/var/lib/tailscale/tailscaled.state --socket=/var/run/tailscale/tailscaled.sock &
sleep 2

# 3. Join Tailscale if AuthKey is provided, or wait for manual register
if [ -n "$TAILSCALE_AUTHKEY" ]; then
    tailscale up --authkey="$TAILSCALE_AUTHKEY" --accept-dns=false
else
    echo "=========================================================="
    echo "Tailscale AuthKey not set. To register this pod manually:"
    echo "1. Retrieve the login link from 'tailscale up' below"
    echo "2. Or run: tailscale up --accept-dns=false"
    echo "=========================================================="
    tailscale up --accept-dns=false &
fi

# 4. Patch python version match level in ray
python3 -c "
from pathlib import Path
import ray._private.utils as utils
path = Path(utils.__file__)
source = path.read_text()
path.write_text(source.replace('python_version_match_level=\"patch\"', 'python_version_match_level=\"minor\"'))
"

# 5. Start Ray worker if GCS address is provided
if [ -n "$RAY_HEAD_ADDRESS" ]; then
    if [[ "$RAY_HEAD_ADDRESS" == *:* ]]; then
        RAY_ADDRESS="$RAY_HEAD_ADDRESS"
    else
        RAY_ADDRESS="${RAY_HEAD_ADDRESS}:6379"
    fi

    RAY_NODE_IP="${RADEON_NODE_IP:-$(tailscale ip -4 | awk 'NR==1 {print $1}') }"
    if [ -z "$RAY_NODE_IP" ]; then
        RAY_NODE_IP="127.0.0.1"
    fi

    if [ "${RAY_USE_PROXYCHAINS:-0}" = "1" ] && command -v proxychains4 >/dev/null 2>&1; then
        proxychains4 -q "$RAY_BIN" start \
            --address="$RAY_ADDRESS" \
            --resources='{"AMD_GPU":1.0}' \
            --num-gpus=1 \
            --node-name="$RAY_NODE_NAME" \
            --node-ip-address="$RAY_NODE_IP" \
            --dashboard-agent-listen-port="52375" \
            --dashboard-agent-grpc-port="65428" \
            --runtime-env-agent-port="59527" \
            --metrics-export-port="8081" \
            --block
    else
        "$RAY_BIN" start \
            --address="$RAY_ADDRESS" \
            --resources='{"AMD_GPU":1.0}' \
            --num-gpus=1 \
            --node-name="$RAY_NODE_NAME" \
            --node-ip-address="$RAY_NODE_IP" \
            --dashboard-agent-listen-port="52375" \
            --dashboard-agent-grpc-port="65428" \
            --runtime-env-agent-port="59527" \
            --metrics-export-port="8081" \
            --block
    fi
else
    echo "No RAY_HEAD_ADDRESS provided. Keeping container alive for manual configuration."
    sleep infinity
fi
