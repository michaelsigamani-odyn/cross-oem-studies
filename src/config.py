from dataclasses import dataclass
import os
from pathlib import Path
from urllib.parse import urlparse


@dataclass
class RuntimeConfig:
    ray_dashboard_url: str
    ray_gcs_address: str
    serve_address: str
    infer_url: str
    dgx_vllm_url: str
    radeon_vllm_url: str
    rtx5090_vllm_url: str
    standby_vllm_url: str
    model_name: str
    requests: int
    concurrency: int
    split_target_pct: float
    split_tolerance_pct: float
    app_name: str
    deployment_name: str
    expected_head_ip: str
    expected_dgx_ip: str
    expected_radeon_ip: str
    expected_rtx5090_ip: str
    expected_runpod_ip: str
    expected_runpod_resource_key: str
    expected_standby_ip: str
    auto_fix_grafana: bool
    serve_config_path: Path
    radeon_host: str
    dgx_host: str
    rtx5090_host: str
    standby_host: str
    standby_node_name: str
    standby_ssh_key: str
    aws_ssh_host: str
    aws_ssh_key: str
    ray_head_dashboard_port: int
    ray_head_metrics_port: int
    radeon_image: str
    rtx5090_image: str
    nvidia_worker_image: str
    standby_image: str
    model_cache_dir: str
    router_health_interval_s: float
    router_failure_threshold: int
    router_max_request_retries: int
    router_actor_name: str
    router_metrics_port: int
    gateway_api_key: str
    router_primary_count: int
    router_sla_p95_ms: float
    router_queue_max: int
    router_queue_timeout_s: float
    runpod_lookup_cmd: str


def _read_bool(name: str, default: str) -> bool:
    return os.getenv(name, default).lower() in {"1", "true", "yes"}


def _read_int(name: str, default: str) -> int:
    return int(os.getenv(name, default))


def _read_float(name: str, default: str) -> float:
    return float(os.getenv(name, default))


def _default_config_path() -> Path:
    return Path(__file__).resolve().parents[1] / "cluster" / "serve_config.yaml"


def _derived_gcs_address() -> str:
    raw = os.getenv("RAY_GCS_ADDRESS")
    if raw:
        return raw
    dashboard = os.getenv("RAY_DASHBOARD_URL", "http://127.0.0.1:8265")
    host = urlparse(dashboard).hostname or "127.0.0.1"
    return "127.0.0.1:6379" if host in {"127.0.0.1", "localhost", "0.0.0.0"} else f"{host}:6379"


def load_runtime_config() -> RuntimeConfig:
    return RuntimeConfig(
        ray_dashboard_url=os.getenv("RAY_DASHBOARD_URL", "http://127.0.0.1:8265"),
        ray_gcs_address=_derived_gcs_address(),
        serve_address=os.getenv("SERVE_ADDRESS", "http://127.0.0.1:8265"),
        infer_url=os.getenv("INFER_URL", "http://127.0.0.1/v1/chat/completions"),
        dgx_vllm_url=os.getenv("DGX_VLLM_URL", "http://127.0.0.1:8001/infer/v1/chat/completions"),
        radeon_vllm_url=os.getenv("RADEON_VLLM_URL", "http://127.0.0.1:8002/infer/v1/chat/completions"),
        rtx5090_vllm_url=os.getenv("RTX5090_VLLM_URL", "http://127.0.0.1:8004/infer/v1/chat/completions"),
        standby_vllm_url=os.getenv("STANDBY_VLLM_URL", "http://127.0.0.1:8003/infer/v1/chat/completions"),
        model_name=os.getenv("MODEL_NAME", "qwen2.5-7b"),
        requests=_read_int("REQUESTS", "200"),
        concurrency=_read_int("CONCURRENCY", "24"),
        split_target_pct=_read_float("SPLIT_TARGET_PCT", "50"),
        split_tolerance_pct=_read_float("SPLIT_TOLERANCE_PCT", "5"),
        app_name=os.getenv("APP_NAME", "qwen7b"),
        deployment_name=os.getenv("DEPLOYMENT_NAME", "VLLMDeployment"),
        expected_head_ip=os.getenv("EXPECTED_HEAD_IP", "127.0.0.1"),
        expected_dgx_ip=os.getenv("EXPECTED_DGX_IP", "127.0.0.1"),
        expected_radeon_ip=os.getenv("EXPECTED_RADEON_IP", "127.0.0.1"),
        expected_rtx5090_ip=os.getenv("EXPECTED_RTX5090_IP", "127.0.0.1"),
        expected_runpod_ip=os.getenv("EXPECTED_RUNPOD_IP", ""),
        expected_runpod_resource_key=os.getenv("EXPECTED_RUNPOD_RESOURCE_KEY", "acceleratorType:Amd-Instinct-Mi300X-Oam"),
        expected_standby_ip=os.getenv("EXPECTED_STANDBY_IP", "100.112.76.83"),
        auto_fix_grafana=_read_bool("AUTO_FIX_GRAFANA", "1"),
        serve_config_path=Path(os.getenv("SERVE_CONFIG_PATH", str(_default_config_path()))),
        radeon_host=os.getenv("RADEON_HOST", "radeon"),
        dgx_host=os.getenv("DGX_HOST", "dgx"),
        rtx5090_host=os.getenv("RTX5090_HOST", "rtx5090"),
        standby_host=os.getenv("STANDBY_HOST", "michael@100.112.76.83"),
        standby_node_name=os.getenv("STANDBY_NODE_NAME", "odyn-dgx2"),
        standby_ssh_key=os.getenv("STANDBY_SSH_KEY", str(Path.home() / ".ssh" / "odyn")),
        aws_ssh_host=os.getenv("AWS_SSH_HOST", "ubuntu@127.0.0.1"),
        aws_ssh_key=os.getenv("AWS_SSH_KEY", str(Path.home() / ".ssh" / "my-key.pem")),
        ray_head_dashboard_port=_read_int("RAY_HEAD_DASHBOARD_PORT", "28265"),
        ray_head_metrics_port=_read_int("RAY_HEAD_METRICS_PORT", "8081"),
        radeon_image=os.getenv("RADEON_IMAGE", "michaelsigamaniodyn/runtime-vllm-radeon:rocm721-gfx1151"),
        rtx5090_image=os.getenv("RTX5090_IMAGE", "michaelsigamaniodyn/runtime-vllm-rtx5090:latest"),
        nvidia_worker_image=os.getenv("NVIDIA_WORKER_IMAGE", "michaelsigamaniodyn/runtime-vllm-dgx-spark:cuda12.9"),
        standby_image=os.getenv("STANDBY_IMAGE", os.getenv("NVIDIA_WORKER_IMAGE", "michaelsigamaniodyn/runtime-vllm-dgx-spark:cuda12.9")),
        model_cache_dir=os.getenv("MODEL_CACHE_DIR", "/home/michael/.cache/huggingface"),
        router_health_interval_s=_read_float("ROUTER_HEALTH_INTERVAL_S", "5"),
        router_failure_threshold=_read_int("ROUTER_FAILURE_THRESHOLD", "3"),
        router_max_request_retries=_read_int("ROUTER_MAX_REQUEST_RETRIES", "3"),
        router_actor_name=os.getenv("ROUTER_ACTOR_NAME", "cross-oem-failover-router"),
        router_metrics_port=_read_int("ROUTER_METRICS_PORT", "9309"),
        gateway_api_key=os.getenv("GATEWAY_API_KEY", ""),
        router_primary_count=_read_int("ROUTER_PRIMARY_COUNT", "2"),
        router_sla_p95_ms=_read_float("ROUTER_SLA_P95_MS", "30000"),
        router_queue_max=_read_int("ROUTER_QUEUE_MAX", "64"),
        router_queue_timeout_s=_read_float("ROUTER_QUEUE_TIMEOUT_S", "15"),
        runpod_lookup_cmd=os.getenv("RUNPOD_LOOKUP_CMD", ""),
    )
