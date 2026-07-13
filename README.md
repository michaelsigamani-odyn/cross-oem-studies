# Odyn Network — Hetzner Head Node Setup

The Hetzner CX43 acts as the public-facing nginx reverse proxy and control plane for the Odyn inference cluster. It routes incoming requests across all GPU worker nodes via Tailscale.

> **Replicability note**: Sections 1-12 + worker addition describe production head node bootstrap. They require a specific Hetzner server, Tailscale auth keys, pre-existing control-plane source (sibling repo), and hardcoded IPs/tokens. They are not runnable from this `cross-oem` checkout alone without the full phase1 tree and live infrastructure.

---

## Cross-OEM Workflow (replicable with Python + Docker)

This subdirectory (`cross-oem`) contains the Python toolkit and Dockerfiles for heterogeneous (AMD + NVIDIA) Ray Serve workers.

### Local dev / test prerequisites
- Python 3.11+
- Docker (for building worker images)
- `python3 -m pip install -e .[test]` or `python3 -m pip install -e . pyyaml pytest` (deps declared in pyproject + pyyaml)

### Replicable commands
```bash
# 1. Install / test the toolkit
python3 -m pip install -e .

# 2. Run unit + workflow E2E tests (fully mocked, no cluster required)
make check
make e2e
make safe-refactor

# 3. Build worker images
make build-radeon
make build-dgx
make build-rtx5090

# 4. (Production) Start a worker locally (requires Ray cluster + env vars)
PYTHONPATH=src python3 -c "import sys,main as m;sys.argv=['','start-radeon'];m.main()"
PYTHONPATH=src python3 -c "import sys,main as m;sys.argv=['','start-dgx'];m.main()"
PYTHONPATH=src python3 -c "import sys,main as m;sys.argv=['','start-rtx5090'];m.main()"

# 5. Deploy Ray Serve app (requires live cluster + SSH access configured)
PYTHONPATH=src python3 -c "import sys,main as m;sys.argv=['','deploy'];m.main()"

# 6. Run traffic split E2E against live cluster
PYTHONPATH=src python3 -c "import sys,main as m;sys.argv=['','e2e'];m.main()"
```

All core logic paths (deploy, load, split validation, safe-refactor with rollback) have coverage in `test/test_workflow_e2e.py`. The `test/test_workflow_e2e.py` runs fully mocked.

---

## Prerequisites (Head Node)

- Hetzner Cloud CX43 server (Ubuntu 24.04)
- Tailscale account with access to the Odyn tailnet
- Port `8088` open in the Hetzner Cloud firewall (inbound TCP)

---

## 1. Install Docker

```bash
apt-get update
apt-get install -y ca-certificates curl gnupg
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
  | tee /etc/apt/sources.list.d/docker.list
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io
```

---

## 2. Install Go 1.25.1

```bash
wget https://go.dev/dl/go1.25.1.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.25.1.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> /etc/profile
source /etc/profile
go version
```

---

## 3. Install Tailscale

```bash
curl -fsSL https://tailscale.com/install.sh | sh
systemctl start tailscaled
systemctl enable tailscaled
tailscale up --authkey=YOUR_TAILSCALE_AUTH_KEY --hostname=odyn-hetzner
tailscale ip -4
```

Generate an auth key at https://login.tailscale.com/admin/settings/keys

---

## 4. Set Up PostgreSQL

```bash
apt-get install -y postgresql
systemctl start postgresql
systemctl enable postgresql

sudo -u postgres psql << 'SQL'
CREATE USER marketplace WITH PASSWORD 'marketplace';
CREATE DATABASE marketplace OWNER marketplace;
GRANT ALL PRIVILEGES ON DATABASE marketplace TO marketplace;
SQL
```

---

## 5. Clone the Repo

```bash
cd /root
git clone https://github.com/Odyn-Network/phase1.git
cd phase1
```

---

## 6. Build and Install odyn-cp

```bash
cd /root/phase1/control-plane
go build -o odyn-cp .
```

Create `/root/phase1/control-plane/.env`:

```env
DATABASE_URL=postgres://marketplace:marketplace@localhost:5432/marketplace?sslmode=disable
PORT=8081
GRPC_PORT=50051
ODYN_RAY_HEAD_SYNC_TOKEN=09ad5ee348cdeda4bec87f42c47aaf8891b4fa65c0a33d6f277186eeedf6af76
RAY_HEAD_GCS_PORT=6379
RAY_HEAD_DASHBOARD_PORT=8265
RAY_HEAD_SERVE_PORT=8000
RAY_HEAD_HEALTH_PORT=8002
LOG_LEVEL=info
ENV=production
FRP_BIND_PORT=7002
FRP_VHOST_HTTP_PORT=8082
FRP_SSH_PORT_START=10000
FRP_SSH_PORT_END=20000
FRP_RAY_PORT_START=20001
FRP_RAY_PORT_END=30000
ODYN_RAY_HEAD_HOST=100.72.227.97
ODYN_RAY_HEAD_HOSTS=100.72.227.97
```

Create the systemd service:

```bash
cat > /etc/systemd/system/odyn-cp.service << 'SERVICE'
[Unit]
Description=Odyn Control Plane
After=network.target postgresql.service

[Service]
Type=simple
WorkingDirectory=/root/phase1/control-plane
EnvironmentFile=/root/phase1/control-plane/.env
ExecStart=/root/phase1/control-plane/odyn-cp
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
SERVICE

systemctl daemon-reload
systemctl enable odyn-cp
systemctl start odyn-cp
systemctl status odyn-cp
```

---

## 7. Set Up Ray Head Env

```bash
mkdir -p /opt/odyn/ray-head/env
```

Create `/opt/odyn/ray-head/env/ray-head.env`:

```env
ODYN_CONTROL_PLANE_URL=http://100.72.227.97:8081
ODYN_RAY_HEAD_SYNC_TOKEN=09ad5ee348cdeda4bec87f42c47aaf8891b4fa65c0a33d6f277186eeedf6af76
RAY_HEAD_NODE_IP=100.72.227.97
ODYN_POLL_ASSIGNMENT=true
RAY_HEAD_PORT=6379
RAY_DASHBOARD_PORT=8265
RAY_SERVE_PORT=8000
RAY_HEALTH_PORT=8002
RAY_METRICS_EXPORT_PORT=9091
REDIS_URL=redis://100.72.227.97:6380
MODEL_CACHE_DIR=/opt/models
RAY_HEAD_IMAGE=michaelsigamaniodyn/ray-head:ray249-py312
RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S=1.0
VLLM_EXTRA_ARGS=--enforce-eager --max-num-seqs 128
```

---

## 8. Start the Ray Head

```bash
cd /root/phase1/ray-head/deploy

RAY_HEAD_IMAGE=michaelsigamaniodyn/ray-head:ray249-py312 \
RAY_HEAD_ENV_FILE=/opt/odyn/ray-head/env/ray-head.env \
docker compose up -d

sleep 15
docker logs odyn-ray-head --tail 10
```

---

## 9. Open Firewall Port

In the Hetzner Cloud Console, add an inbound firewall rule: Protocol TCP, Port 8088, Source Any.

---

## 10. Write the nginx Config

Create `/root/nginx-proxy.conf`:

```nginx
upstream vllm_cluster {
    server 100.108.245.77:9000;   # odyn-radeon
    server 100.92.148.18:9000;    # odyn-dgx1
    server 100.112.76.83:9000;    # odyn-dgx2
    server 100.111.244.85:8080;   # odyn-vast-baremetal
}

server {
    listen 8088;
    location / {
        proxy_pass http://vllm_cluster;
        proxy_read_timeout 120s;
        proxy_connect_timeout 10s;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        add_header X-Served-By $upstream_addr always;
    }
}
```

---

## 11. Start the nginx Proxy

```bash
docker run -d --restart unless-stopped \
  --network host \
  --name odyn-proxy \
  -v /root/nginx-proxy.conf:/etc/nginx/conf.d/default.conf \
  nginx:alpine
```

---

## 12. Verify

```bash
systemctl status odyn-cp
docker ps

# Burst test from anywhere on the internet (replace HOST with your public IP)
  HOST=${PUBLIC_HOST:-178.104.165.93}
  for i in 1 2 3 4 5 6 7 8 9; do
    curl -s -D - http://$HOST:8088/v1/chat/completions \
      -H "Content-Type: application/json" \
      -d '{"model":"qwen2.5-7b","messages":[{"role":"user","content":"hello"}],"max_tokens":10}' \
      -o /dev/null | grep "X-Served-By" &
  done
  wait
```

---

## SSH Access

All nodes are reachable via their Tailscale IP using the shared `odyn` SSH key.

```bash
# Hetzner head node (public IP, root user)
ssh root@178.104.165.93 -i ~/.ssh/odyn

# Radeon (ROCm worker)
ssh michael@100.108.245.77 -i ~/.ssh/odyn

# DGX1 (CUDA worker)
ssh michael@100.92.148.18 -i ~/.ssh/odyn

# DGX2 (CUDA worker)
ssh michael@100.112.76.83 -i ~/.ssh/odyn
```

Optional `~/.ssh/config` entries for shorthand access:

```sshconfig
# ─── Odyn GPU cluster ─────────────────────────────────────────────────────────
Host odyn-hetzner
    HostName 178.104.165.93
    User root
    IdentityFile ~/.ssh/odyn

Host odyn-radeon
    HostName 100.108.245.77
    User michael
    IdentityFile ~/.ssh/odyn

Host odyn-dgx1
    HostName 100.92.148.18
    User michael
    IdentityFile ~/.ssh/odyn

Host odyn-dgx2
    HostName 100.112.76.83
    User michael
    IdentityFile ~/.ssh/odyn
```

With this config in place, connect with `ssh odyn-hetzner`, `ssh odyn-radeon`, `ssh odyn-dgx1`, or `ssh odyn-dgx2`.

---



1. Install Tailscale on the new machine and join the tailnet
2. Pull and start the correct vLLM Docker image for the hardware type:

| Hardware | Image | Registry |
|---|---|---|
| NVIDIA DGX Spark (ARM64, CUDA) | `michaelsigamaniodyn/runtime-vllm-dgx-spark:ray249-py312` | [Docker Hub](https://hub.docker.com/repositories/michaelsigamaniodyn) |
| AMD Radeon (ROCm) | `michaelsigamaniodyn/runtime-vllm-radeon:python3.12` | [Docker Hub](https://hub.docker.com/repositories/michaelsigamaniodyn) |
| NVIDIA RTX / Blackwell (x86_64, sm_120+) | `michaelsigamaniodyn/runtime-vllm-rtx5090:latest` | [Docker Hub](https://hub.docker.com/repositories/michaelsigamaniodyn) |

For autoscaling, always pull the pre-built image from Docker Hub rather than building locally. If a new image is needed, build from the relevant Dockerfile in the repo (`cross-oem/dgx-spark/`, `cross-oem/radeon/`, or `cross-oem/rtx-5090/`) and push to `michaelsigamaniodyn` before deploying.

3. Add the Tailscale IP and port to `/root/nginx-proxy.conf` under `upstream vllm_cluster`
4. Restart the proxy:

```bash
docker restart odyn-proxy
```

---

## Architecture

```
Public Internet
      |
      v
178.104.165.93:8088  (Hetzner nginx proxy)
      |
      |-- 100.108.245.77:9000  (Radeon - ROCm)
      |-- 100.92.148.18:9000   (DGX1 - CUDA)
      |-- 100.112.76.83:9000   (DGX2 - CUDA)
      +-- 100.111.244.85:8080  (Vast baremetal - 4x GPU TP4)

All worker connections via Tailscale VPN

odyn-cp  (systemd) on :8081  -- cluster control plane
Ray head (Docker)  on :8000  -- Ray Serve proxy (legacy)
```

---

## Key Paths

| Path | Description |
|---|---|
| `/root/phase1/control-plane/.env` | odyn-cp environment variables |
| `/root/phase1/control-plane/odyn-cp` | compiled Go binary |
| `/opt/odyn/ray-head/env/ray-head.env` | Ray head environment variables |
| `/root/phase1/ray-head/deploy/docker-compose.yml` | Ray head Docker Compose |
| `/root/nginx-proxy.conf` | nginx upstream config |

---

## Notes

- `ODYN_RAY_HEAD_SYNC_TOKEN` must match in both `.env` files
- The `X-Served-By` response header identifies which node handled each request
- Worker nodes do not need public IPs -- Tailscale handles all internal routing
- To debug nginx: `docker logs odyn-proxy`
- To debug odyn-cp: `journalctl -u odyn-cp -f`

---

## Endpoints

| | Endpoint | Script |
|--|----------|--------|
| 1 | Real-time chat completions | [submit_chat_completions.py](https://github.com/Odyn-Network/phase1/blob/feat/cross-oem-chat-inference/cross-oem/submit_chat_completions.py) |
| 2 | Offline batch chat completions | [submit_chat_completions_offline.py](https://github.com/Odyn-Network/phase1/blob/feat/cross-oem-chat-inference/cross-oem/submit_chat_completions_offline.py) |
| 3 | Offline data-parallel via Ray Data | [submit_ray_job.py](https://github.com/Odyn-Network/phase1/blob/feat/cross-oem-chat-inference/cross-oem/submit_ray_job.py) |

---

## Observability

Use both dashboards — they serve different purposes.

### Ray Dashboard — system & model serving health
**URL:** http://127.0.0.1:8265/#/serve (override with `RAY_DASHBOARD_URL`)

- Replica lifecycle (active, starting, pending) for Ray Serve deployments
- CPU, RAM, and GPU/VRAM utilisation in real time via the cluster view
- Live stdout/stderr logs from model actors on all GPU workers

### Prometheus & Grafana
- `prometheus.yml` scrapes the router metrics endpoint (default `http://127.0.0.1:9309/metrics`).
- Grafana dashboard panels track node status timeline, throughput, failover events, and batch progress.
- Import `grafana/dashboards/cross_oem.json` to provision an opinionated dashboard.
- Import alert rule `grafana/alerts/node_failure.json` to trigger a critical notification when `router_status` drops to zero for 30s.

- The Web dashboard (`dashboard/`) consumes `/v1/nodes` for live node status without refresh.

---

## SLA-aware routing & workload queueing

The failover router prefers nodes whose observed p95 latency honours the SLA
target and deprioritises breaching nodes; if every routable node breaches, the
router still serves traffic on the least-loaded rotation. When no node is
routable at all (e.g. mid-failover), incoming workloads are held in a bounded
FIFO admission queue until a node recovers or a reserve is promoted, instead
of being rejected immediately.

| Env var | Default | Meaning |
|---|---|---|
| `ROUTER_SLA_P95_MS` | `30000` | p95 latency SLA target per node (ms); `0` disables SLA-aware selection |
| `ROUTER_QUEUE_MAX` | `64` | max workloads queued while no node is routable |
| `ROUTER_QUEUE_TIMEOUT_S` | `15` | max seconds a queued workload waits before rejection; `0` disables queueing |

Per-node `sla_ok`, latency percentiles, and the shared `queue_depth` are
exposed via `/v1/nodes`, the router snapshot, and Prometheus gauges
(`router_sla_ok`, `router_queue_depth`, `router_sla_p95_target_ms`). Chat
completions additionally return the routing decision in the `x-served-by`
response header and the `served_by` field of non-streaming responses; batch
items carry `_odyn_served_by` per item result.

---

## Infrastructure Note

The Ray head has been moved to AWS with an API Gateway layer added for out-of-the-box CloudWatch logging and rate limiting.

Terraform setup: [gateway.tf](https://github.com/Odyn-Network/phase1/blob/feat/cross-oem-chat-inference/cross-oem/gateway.tf)

```bash
../.venv/bin/python src/main.py deploy
```

### Run All Tests
 
 ```bash
 pytest
 ```

### Installation & Packaging

You can install `odyn.network.toolkit` as a standard developer package in two ways:

#### Option A: Direct Local or Wheel Install
```bash
# Build and install from compiled wheel
python -m build
pip install dist/odyn_network_toolkit-1.0.0-py3-none-any.whl
```

This registers the global `odyn-toolkit` executable command-line entrypoint.

#### Developer SDK (CLI) Usage

Once installed, use the newly registered `odyn-toolkit` CLI to trigger and test each of our three core flows:

##### Retrieving your API Key

Retrieve your active `ODYN_API_KEY` programmatically or from the AWS Console:

* **Via Terraform**:
  ```bash
  terraform output -raw consumer_api_key
  ```
* **Via AWS CLI**:
  ```bash
  aws apigateway get-api-keys --include-values --query "items[?name=='odyn-consumer-key'].value" --output text --region eu-central-1
  ```
* **Via AWS Console**:
  Navigate to **API Gateway** -> **API Keys** in the **Frankfurt (`eu-central-1`)** region, click on **`odyn-consumer-key`**, and click **Show** to view the key.

##### Running CLI Commands

```bash
# Set your API key in the environment first, or pass it directly via the -k flag
export ODYN_API_KEY="your-prod-api-key"

# 1. Run Real-Time Chat completions with the default prompt
odyn-toolkit chat

# 1a. Override prompt inline
odyn-toolkit chat --prompt "Summarize Kubernetes in one sentence."

# 1b. Load prompt from a workload file
mkdir -p user-workloads
printf "%s\n" "Explain failover and recovery in 2 lines." > user-workloads/chat_prompt.txt
odyn-toolkit chat --prompt-file user-workloads/chat_prompt.txt

# 2. Run Offline Batch Chat completions
odyn-toolkit batch

# 3. Submit a Parallel Data Job directly to our Ray Head node
odyn-toolkit ray-job

# 4. Verify all required Ray nodes are visible and in IDLE/ACTIVE state
python src/main.py verify-node-visibility
```

*Note: The SDK commands automatically handle route queries, programmatic error tracking, and fallbacks behind the scenes without polluting your shell environment.*

For node visibility verification, configure expected node identity values before running the command:

```bash
export RAY_DASHBOARD_URL="http://100.123.244.93:28265"
export EXPECTED_DGX_IP="<dgx-ip>"
export EXPECTED_RADEON_IP="<london-radeon-ip>"
export EXPECTED_HEAD_IP="<aws-head-ip>"
export EXPECTED_RUNPOD_IP="<runpod-ip>"
export EXPECTED_RUNPOD_RESOURCE_KEY="acceleratorType:Amd-Instinct-Mi300X-Oam"
```

If you prefer dynamic RunPod lookup, omit `EXPECTED_RUNPOD_IP` and provide a CLI command that returns JSON containing one of `ssh_host`, `host`, or `ip`:

```bash
export RUNPOD_LOOKUP_CMD="runpodctl get pods --output json"
```

If RunPod joins Ray through a private/Tailscale address that differs from the public SSH endpoint, keep `EXPECTED_RUNPOD_RESOURCE_KEY` set so verification can match the RunPod node by resource marker.

The verifier queries `/api/cluster_status`, asserts all four required nodes are present, requires each node to be `IDLE` or `ACTIVE`, and prints a pass/fail report with live node state.

### Reproducible failover benchmark
 
 Run the deterministic benchmark to confirm failover completion time stays within baseline:
 
 ```bash
 python benchmark/run_benchmark.py
 ```

### Full demo smoke verification (4 phases)

Run the end-to-end demo verifier that performs pre-checks, live heterogeneous load validation,
failover tracking, recovery tracking, report generation, and Slack posting:

```bash
export RAY_DASHBOARD_URL="http://100.123.244.93:28265"
export DEMO_DGX_IP="100.108.245.77"
export DEMO_RADEON1_IP="104.15.30.249"
export DEMO_RADEON2_IP="100.88.171.72"
export DEMO_ENDPOINT_URLS="https://<gateway>/v1/chat/completions,http://<dgx>/infer/v1/chat/completions,http://<radeon1>/infer/v1/chat/completions"
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."
python src/main.py demo-verify
```

Report output defaults to `build/demo-verification-report.md` and can be overridden with `DEMO_REPORT_PATH`.

### Recovery verification (Phases 1-3)

Run the reboot/rejoin verification flow aligned to the Phase 1-3 acceptance criteria:

```bash
# Optional: install and start ray-worker.service on both Radeon workers
chmod +x scripts/install_ray_worker_service.sh
RAY_HEAD_ADDRESS="100.123.244.93:6379" WORKER_SERVICE_HOSTS="odyn-radeon,a6000-london" scripts/install_ray_worker_service.sh

# Recovery verification report
export RAY_DASHBOARD_URL="http://100.123.244.93:28265"
export EXPECTED_HEAD_IP="<aws-head-ip>"
export EXPECTED_DGX_IP="<dgx-ip>"
export EXPECTED_RADEON_IP="<london-radeon-ip>"
export EXPECTED_RUNPOD_IP="<runpod-radeon-ip>"
python src/main.py recovery-verify
```

By default the verifier waits for an operator-triggered reboot of the primary Radeon worker. To let the verifier issue reboot directly, set `RECOVERY_TRIGGER_REBOOT=1` and provide `RECOVERY_REBOOT_CMD`.

Report output defaults to `build/recovery-verification-report.md` and includes per-phase PASS/FAIL and node states post-recovery.

---

## Service Level Objectives (SLOs)

We enforce three critical SLOs to track availability, reliability, and cluster failover speed:

| Objective | SLI Metric | SLO Target | Monitoring Source |
|---|---|---|---|
| **Gateway Availability** | `availability` | $\ge 99.9\%$ (5m window) | CloudWatch Custom Metrics |
| **Batch Job Reliability**| `batch_reliability` | $\ge 99.5\%$ (1h window) | CloudWatch Custom Metrics |
| **Failover RTO** | `failover_rto_seconds` | $\le 30\text{s}$ (99th pct) | CloudWatch Logs Insights |

### Querying RTO via CloudWatch Logs Insights

To track cluster failover RTO programmatically from structured logs in `/aws/lambda/odyn-failover` or `odyn-failover` log group:

```query
fields @timestamp, failed_node_id, standby_node_id, elapsed_seconds
| filter event = "RETRY"
| stats max(elapsed_seconds) as MaxRTO, percentiles(elapsed_seconds, 99) as p99RTO by bin(1h)
```
