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

All core logic paths (deploy, load, split validation, safe-refactor with rollback) are covered in pytest (`test/test_e2e_regression.py` plus workflow/unit tests) and run fully mocked.

---

## Prerequisites (Head Node)

- Hetzner Cloud CX43 server (Ubuntu 24.04)
- Tailscale account with access to the Odyn tailnet
- Root shell access on the head node
- Existing `phase1` repository access

---

## 0. Define Deployment Variables (do this first)

Set these once and reuse them in every later step.

```bash
export HEAD_PUBLIC_IP="178.104.165.93"
export HEAD_TAILNET_IP="100.72.227.97"
export TAILSCALE_AUTH_KEY="tskey-REPLACE_ME"
export ODYN_SYNC_TOKEN="09ad5ee348cdeda4bec87f42c47aaf8891b4fa65c0a33d6f277186eeedf6af76"
export RAY_HEAD_IMAGE="michaelsigamaniodyn/ray-head:ray249-py312"
```

Generate the Tailscale auth key at https://login.tailscale.com/admin/settings/keys.

---

## 1. Install Base System Dependencies

```bash
apt-get update
apt-get install -y ca-certificates curl gnupg wget postgresql
```

---

## 2. Install Docker Engine

```bash
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
  | tee /etc/apt/sources.list.d/docker.list
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io
systemctl enable docker
systemctl start docker
```

---

## 3. Install Go 1.25.1

```bash
wget https://go.dev/dl/go1.25.1.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.25.1.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> /etc/profile
source /etc/profile
go version
```

---

## 4. Install and Join Tailscale

```bash
curl -fsSL https://tailscale.com/install.sh | sh
systemctl enable tailscaled
systemctl start tailscaled
tailscale up --authkey="${TAILSCALE_AUTH_KEY}" --hostname=odyn-hetzner
tailscale ip -4
```

Confirm the returned Tailscale IPv4 matches `${HEAD_TAILNET_IP}`.

---

## 5. Configure PostgreSQL

```bash
systemctl enable postgresql
systemctl start postgresql
sudo -u postgres psql << 'SQL'
CREATE USER marketplace WITH PASSWORD 'marketplace';
CREATE DATABASE marketplace OWNER marketplace;
GRANT ALL PRIVILEGES ON DATABASE marketplace TO marketplace;
SQL
```

---

## 6. Clone `phase1`

```bash
cd /root
git clone https://github.com/Odyn-Network/phase1.git
cd /root/phase1
```

If this `cross-oem` directory is not already present under `phase1`, clone it now so setup assets are available:

```bash
git clone https://github.com/Odyn-Network/cross-oem.git cross-oem
```

---

## 7. Build and Configure `odyn-cp`

```bash
cd /root/phase1/control-plane
go build -o odyn-cp .
```

Render and install all required config/service files from repo-managed templates:

```bash
cd /root/phase1/cross-oem
HEAD_TAILNET_IP="${HEAD_TAILNET_IP}" \
ODYN_SYNC_TOKEN="${ODYN_SYNC_TOKEN}" \
RAY_HEAD_IMAGE="${RAY_HEAD_IMAGE}" \
./setup-scripts/hetzner-headnode/install-headnode-config.sh

systemctl daemon-reload
systemctl enable odyn-cp
systemctl start odyn-cp
systemctl status odyn-cp
```

Files installed by this step:

- `/opt/odyn-cp/odyn-cp.env`
- `/opt/odyn/ray-head/env/ray-head.env`
- `/opt/odyn/nginx/nginx-proxy.conf`
- `/etc/systemd/system/odyn-cp.service`

Template sources in repo:

- `setup-scripts/hetzner-headnode/templates/opt/odyn-cp/odyn-cp.env`
- `setup-scripts/hetzner-headnode/templates/opt/odyn/ray-head/env/ray-head.env`
- `setup-scripts/hetzner-headnode/templates/opt/odyn/nginx/nginx-proxy.conf`
- `setup-scripts/hetzner-headnode/templates/etc/systemd/system/odyn-cp.service`

---

## 8. Create Ray Head Environment File

This file is already rendered to `/opt/odyn/ray-head/env/ray-head.env` by Step 7.

Validate content quickly:

```bash
grep -E "ODYN_CONTROL_PLANE_URL|RAY_HEAD_NODE_IP|RAY_HEAD_IMAGE" /opt/odyn/ray-head/env/ray-head.env
```

---

## 9. Start Ray Head Services

```bash
cd /root/phase1/ray-head/deploy
RAY_HEAD_IMAGE="${RAY_HEAD_IMAGE}" \
RAY_HEAD_ENV_FILE=/opt/odyn/ray-head/env/ray-head.env \
docker compose up -d
sleep 15
docker logs odyn-ray-head --tail 20
```

---

## 10. Open Public Firewall Rule

In the Hetzner Cloud Console, add inbound TCP `8088` from `0.0.0.0/0`.

---

## 11. Write nginx Upstream Config

The upstream config is managed in repo and installed to `/opt/odyn/nginx/nginx-proxy.conf` by Step 7.

To customize workers, edit the repo template and reinstall:

```bash
cd /root/phase1/cross-oem
$EDITOR setup-scripts/hetzner-headnode/templates/opt/odyn/nginx/nginx-proxy.conf
HEAD_TAILNET_IP="${HEAD_TAILNET_IP}" \
ODYN_SYNC_TOKEN="${ODYN_SYNC_TOKEN}" \
RAY_HEAD_IMAGE="${RAY_HEAD_IMAGE}" \
./setup-scripts/hetzner-headnode/install-headnode-config.sh
```

---

## 12. Start nginx Proxy Container

```bash
docker run -d --restart unless-stopped \
  --network host \
  --name odyn-proxy \
  -v /opt/odyn/nginx/nginx-proxy.conf:/etc/nginx/conf.d/default.conf \
  nginx:alpine
```

---

## 13. Verify End-to-End Routing

```bash
systemctl status odyn-cp
docker ps

HOST="${PUBLIC_HOST:-${HEAD_PUBLIC_IP}}"
for i in 1 2 3 4 5 6 7 8 9; do
  curl -s -D - "http://${HOST}:8088/v1/chat/completions" \
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
## Add a Worker Node

1. Install Tailscale on the new machine and join the same tailnet.
2. Pull and start the correct vLLM Docker image for the hardware type:

| Hardware | Image | Registry |
|---|---|---|
| NVIDIA DGX Spark (ARM64, CUDA) | `michaelsigamaniodyn/runtime-vllm-dgx-spark:ray249-py312` | [Docker Hub](https://hub.docker.com/repositories/michaelsigamaniodyn) |
| AMD Radeon (ROCm) | `michaelsigamaniodyn/runtime-vllm-radeon:python3.12` | [Docker Hub](https://hub.docker.com/repositories/michaelsigamaniodyn) |
| NVIDIA RTX / Blackwell (x86_64, sm_120+) | `michaelsigamaniodyn/runtime-vllm-rtx5090:latest` | [Docker Hub](https://hub.docker.com/repositories/michaelsigamaniodyn) |

For autoscaling, pull pre-built images from Docker Hub rather than building locally. If a new image is required, build from `cross-oem/dgx-spark/`, `cross-oem/radeon/`, or `cross-oem/rtx-5090/`, then push to `michaelsigamaniodyn` before deploy.

3. Add the worker Tailscale IP and inference port to `setup-scripts/hetzner-headnode/templates/opt/odyn/nginx/nginx-proxy.conf` under `upstream vllm_cluster`, then rerun Step 11.
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
| `/opt/odyn-cp/odyn-cp.env` | odyn-cp environment variables |
| `/root/phase1/control-plane/odyn-cp` | compiled Go binary |
| `/opt/odyn/ray-head/env/ray-head.env` | Ray head environment variables |
| `/root/phase1/ray-head/deploy/docker-compose.yml` | Ray head Docker Compose |
| `/opt/odyn/nginx/nginx-proxy.conf` | nginx upstream config |
| `/etc/nginx/sites-available/ray-dashboard.conf` | public Ray dashboard reverse proxy config |

---

## Notes

- `ODYN_RAY_HEAD_SYNC_TOKEN` must match in both control-plane and ray-head env files
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

### Public Ray Dashboard URL (repo-managed setup)

Do not publish the dashboard without auth unless you explicitly accept exposure risk.

1. Create DNS `A` record for your dashboard host (example `raydash.example.com`) to point at the Hetzner public IP.
2. Run the repo-managed installer on the Hetzner head node:

```bash
cd /root/phase1/cross-oem
RAY_DASHBOARD_SERVER_NAME="raydash.example.com" \
RAY_DASHBOARD_UPSTREAM="100.123.244.93:28265" \
LETSENCRYPT_EMAIL="ops@example.com" \
RAY_DASHBOARD_AUTH_MODE="basic" \
RAY_DASHBOARD_BASIC_AUTH_USER="rayviewer" \
RAY_DASHBOARD_BASIC_AUTH_PASSWORD="replace-me" \
./setup-scripts/hetzner-headnode/install-ray-dashboard-url.sh
```

If the Ray head runs on the same host, set `RAY_DASHBOARD_UPSTREAM="127.0.0.1:8265"`.

To make it accessible to anyone with the link (no auth), set:

```bash
RAY_DASHBOARD_AUTH_MODE="public"
```

Installer script and templates:

- `setup-scripts/hetzner-headnode/install-ray-dashboard-url.sh`
- `setup-scripts/hetzner-headnode/templates/etc/nginx/sites-available/ray-dashboard.auth.conf`
- `setup-scripts/hetzner-headnode/templates/etc/nginx/sites-available/ray-dashboard.public.conf`

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
