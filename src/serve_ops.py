import concurrent.futures
import json
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
import yaml
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from config import RuntimeConfig


@dataclass
class Replica:
    replica_id: str
    node_id: str
    node_ip: str
    log_file_path: str


@dataclass
class SplitResult:
    deltas: Dict[str, int]
    split_pct: Dict[str, float]
    max_error_pct: float


def _json_get(url: str, timeout: int = 30) -> Dict:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


def _is_loopback(url: str) -> bool:
    host = urllib.parse.urlparse(url).hostname or ""
    return host in {"localhost", "127.0.0.1", "0.0.0.0"}


def check_metric_health(cfg: RuntimeConfig) -> None:
    for name in ("grafana", "prometheus"):
        _assert_backend_health(cfg.ray_dashboard_url, name)


def _assert_backend_health(dashboard_url: str, name: str) -> None:
    data = _json_get(f"{dashboard_url}/api/{name}_health")
    if not data.get("result"):
        raise RuntimeError(f"{name} health check failed: {data}")


def ensure_remote_grafana_host(cfg: RuntimeConfig) -> None:
    if _grafana_ok(cfg):
        return
    if not cfg.auto_fix_grafana:
        raise RuntimeError("grafana iframe host is loopback and AUTO_FIX_GRAFANA=0")
    from grafana_fixer import GrafanaFixer
    GrafanaFixer(cfg).fix()


def _grafana_ok(cfg: RuntimeConfig) -> bool:
    data = _json_get(f"{cfg.ray_dashboard_url}/api/grafana_health")
    host = data.get("data", {}).get("grafanaHost", "")
    return _is_loopback(cfg.ray_dashboard_url) or not _is_loopback(host)


def list_running_replicas(cfg: RuntimeConfig) -> List[Replica]:
    data = _json_get(f"{cfg.ray_dashboard_url}/api/serve/applications/")
    rows = data["applications"][cfg.app_name]["deployments"][cfg.deployment_name]["replicas"]
    return [Replica(r["replica_id"], r["node_id"], r["node_ip"], r["log_file_path"]) for r in rows if r.get("state") == "RUNNING"]


def assert_replica_spread(cfg: RuntimeConfig, replicas: List[Replica]) -> None:
    ips = [r.node_ip for r in replicas]
    _assert_distinct_replicas(ips)
    _assert_expected_nodes(cfg, ips)


def assert_cluster_nodes_present(cfg: RuntimeConfig) -> None:
    usage = _json_get(f"{cfg.ray_dashboard_url}/api/cluster_status")["data"]["clusterStatus"]["loadMetricsReport"]["usage"]
    keys = [f"node:{cfg.expected_head_ip}", f"node:{cfg.expected_dgx_ip}", f"node:{cfg.expected_radeon_ip}", "node:InternalHead"]
    if cfg.expected_standby_ip and cfg.expected_standby_ip != "127.0.0.1":
        keys.append(f"node:{cfg.expected_standby_ip}")
    missing = [key for key in keys if key not in usage]
    if missing:
        raise RuntimeError(f"missing expected node resources: {missing}")


def _assert_distinct_replicas(node_ips: List[str]) -> None:
    if len(node_ips) < 2:
        raise RuntimeError(f"expected >=2 running replicas, got {len(node_ips)}")
    if len(set(node_ips)) < 2:
        raise RuntimeError(f"replicas are not on distinct nodes: {node_ips}")


def _assert_expected_nodes(cfg: RuntimeConfig, node_ips: List[str]) -> None:
    if cfg.expected_dgx_ip not in node_ips or cfg.expected_radeon_ip not in node_ips:
        raise RuntimeError(f"replicas do not include expected DGX/Radeon nodes: {node_ips}")


def _read_log(cfg: RuntimeConfig, replica: Replica) -> str:
    base = cfg.ray_dashboard_url.rstrip("/")
    filename = _dashboard_log_filename(replica.log_file_path)
    payload = _fetch_log_payload(cfg, replica, filename, f"{base}/api/v0/logs/file")
    if payload:
        return payload
    return _fetch_log_payload(cfg, replica, filename, f"{base}/api/logs/file")


def _dashboard_log_filename(path: str) -> str:
    if "/logs/" in path:
        return path.split("/logs/", 1)[1]
    return path.lstrip("/")


def _fetch_log_payload(cfg: RuntimeConfig, replica: Replica, filename: str, endpoint: str) -> str:
    query = urllib.parse.urlencode({"node_id": replica.node_id, "filename": filename, "lines": 50000})
    url = f"{endpoint}?{query}"
    cmd = ["ssh", "-i", cfg.aws_ssh_key, "-o", "IdentitiesOnly=yes", cfg.aws_ssh_host, f"curl -s '{url}'"]
    raw = subprocess.run(cmd, capture_output=True, text=True, timeout=60).stdout
    return _extract_log_text(raw)


def _extract_log_text(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        return ""
    if not text.startswith("{"):
        return text
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return text
    data = payload.get("data") or {}
    for key in ("logs", "contents", "content"):
        if isinstance(data, dict) and key in data and isinstance(data[key], str):
            return data[key]
        if key in payload and isinstance(payload[key], str):
            return payload[key]
    return ""


def post_count(cfg: RuntimeConfig, replica: Replica) -> int:
    return _read_log(cfg, replica).count("v1/chat/completions")


def _request_once(cfg: RuntimeConfig) -> int:
    payload = {"model": cfg.model_name, "messages": [{"role": "user", "content": "Reply one word: ready"}], "max_tokens": 5}
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(cfg.infer_url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=90) as response:
        return response.status


def run_load(cfg: RuntimeConfig) -> Dict[int, int]:
    counts: Dict[int, int] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=cfg.concurrency) as pool:
        futures = [pool.submit(_request_once, cfg) for _ in range(cfg.requests)]
        for future in concurrent.futures.as_completed(futures):
            code = future.result()
            counts[code] = counts.get(code, 0) + 1
    return counts


def compute_split(cfg: RuntimeConfig, before: Dict[str, int], after: Dict[str, int]) -> SplitResult:
    deltas = {rid: after[rid] - before[rid] for rid in before}
    _assert_positive_deltas(deltas)
    split_pct = _to_split_percentage(deltas)
    return SplitResult(deltas, split_pct, _max_error(split_pct, cfg.split_target_pct))


def _assert_positive_deltas(deltas: Dict[str, int]) -> None:
    if any(delta <= 0 for delta in deltas.values()):
        raise RuntimeError(f"cross-routing failed, at least one replica saw no traffic: {deltas}")


def _to_split_percentage(deltas: Dict[str, int]) -> Dict[str, float]:
    total = sum(deltas.values())
    return {rid: (count * 100.0 / total) for rid, count in deltas.items()}


def _max_error(split_pct: Dict[str, float], target_pct: float) -> float:
    return max(abs(value - target_pct) for value in split_pct.values())


def assert_split_tolerance(cfg: RuntimeConfig, split: SplitResult) -> None:
    if split.max_error_pct <= cfg.split_tolerance_pct:
        return
    raise RuntimeError(
        f"split outside tolerance: target={cfg.split_target_pct}% tolerance={cfg.split_tolerance_pct}% split={split.split_pct}"
    )


def _copy_to_head(cfg: RuntimeConfig) -> None:
    src_dir = Path(__file__).resolve().parent
    files = [path for path in src_dir.glob("*.py") if path.is_file()]
    cmd = ["scp", "-i", cfg.aws_ssh_key, "-o", "IdentitiesOnly=yes"] + [str(f) for f in files] + [f"{cfg.aws_ssh_host}:/home/ubuntu/"]
    subprocess.run(cmd, check=True)


def _copy_config_to_head(cfg: RuntimeConfig) -> None:
    cmd = ["scp", "-i", cfg.aws_ssh_key, "-o", "IdentitiesOnly=yes", str(cfg.serve_config_path), f"{cfg.aws_ssh_host}:/home/ubuntu/serve_config.yaml"]
    subprocess.run(cmd, check=True)


def run_deploy(cfg: RuntimeConfig) -> None:
    _copy_to_head(cfg)
    _copy_config_to_head(cfg)
    cmd = ["ssh", "-i", cfg.aws_ssh_key, "-o", "IdentitiesOnly=yes", cfg.aws_ssh_host, f"serve deploy -a {cfg.serve_address} /home/ubuntu/serve_config.yaml"]
    subprocess.run(cmd, check=True)


def wait_healthy(cfg: RuntimeConfig, max_checks: int = 60) -> None:
    for _ in range(max_checks):
        if _serve_status(cfg).startswith("HEALTHY"):
            return
    raise RuntimeError("serve deployment did not become HEALTHY")


def _app_and_dep_statuses(data: dict, app_name: str) -> list[str]:
    app = data.get("applications", {}).get(app_name, {})
    deps = app.get("deployments", {}).values()
    return [app.get("status", "")] + [d.get("status", "") for d in deps]


def _evaluate_statuses(statuses: list[str]) -> str:
    if any(s == "UNHEALTHY" for s in statuses): return "UNHEALTHY"
    if any(s in {"DEPLOYING", "UPDATING", "RESTARTING"} for s in statuses): return "UPDATING"
    ok = all(s in {"RUNNING", "HEALTHY"} for s in statuses)
    return "HEALTHY" if ok and statuses else "pending"


def _serve_status(cfg: RuntimeConfig) -> str:
    data = _json_get(f"{cfg.ray_dashboard_url}/api/serve/applications/")
    combined = _app_and_dep_statuses(data, "gateway") + _app_and_dep_statuses(data, "qwen7b")
    status = _evaluate_statuses(combined)
    time.sleep(10)
    return status


def _extract_app_configs(apps_dict: dict) -> list:
    return [
        d["deployed_app_config"]
        for d in apps_dict.values()
        if d.get("deployed_app_config")
    ]


def _build_snapshot_dict(data: dict) -> dict:
    cfg = {k: data[k] for k in ("proxy_location", "http_options", "grpc_options") if data.get(k)}
    cfg["applications"] = _extract_app_configs(data.get("applications", {}))
    return cfg


def snapshot_serve_config(cfg: RuntimeConfig) -> str:
    data = _json_get(f"{cfg.ray_dashboard_url}/api/serve/applications/")
    return yaml.safe_dump(_build_snapshot_dict(data), sort_keys=False)


def rollback_to_snapshot(cfg: RuntimeConfig, snapshot: str) -> None:
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as handle:
        handle.write(snapshot)
        temp_path = Path(handle.name)
    subprocess.run(["scp", "-i", cfg.aws_ssh_key, "-o", "IdentitiesOnly=yes", str(temp_path), f"{cfg.aws_ssh_host}:/tmp/rb.yaml"], check=True)
    subprocess.run(["ssh", "-i", cfg.aws_ssh_key, "-o", "IdentitiesOnly=yes", cfg.aws_ssh_host, f"serve deploy -a {cfg.serve_address} /tmp/rb.yaml"], check=True)


def _resolve_executable(exe: str) -> str:
    bin_path = Path(sys.executable).parent / exe
    return str(bin_path) if bin_path.exists() else exe


def _run(command: List[str], check: bool = True) -> str:
    cmd = [_resolve_executable(command[0])] + command[1:]
    res = subprocess.run(cmd, text=True, capture_output=True)
    if res.returncode == 0 or not check:
        return res.stdout
    raise RuntimeError(f"command failed: {' '.join(cmd)}\n{res.stdout}\n{res.stderr}")
