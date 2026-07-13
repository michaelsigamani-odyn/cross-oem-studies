import asyncio
import json
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import subprocess

import pytest
from fastapi.testclient import TestClient

# Ensure src/ is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def _install_stub(module_name: str, builder) -> None:
    if module_name not in sys.modules:
        sys.modules[module_name] = builder()


def _build_boto_stub() -> types.ModuleType:
    stub = types.ModuleType("boto3")

    class _S3Stub:
        def put_object(self, **kwargs):  # pragma: no cover - no-op
            return None

    def client(*args, **kwargs):  # pragma: no cover - no-op
        return _S3Stub()

    stub.client = client  # type: ignore[attr-defined]
    return stub


def _build_pandas_stub() -> types.ModuleType:
    stub = types.ModuleType("pandas")
    stub.__version__ = "0.0.0"
    stub.__dict__["DataFrame"] = object
    return stub


_install_stub("boto3", _build_boto_stub)
_install_stub("pandas", _build_pandas_stub)
def _build_numpy_stub() -> types.ModuleType:
    stub = types.ModuleType("numpy")

    class _NDArray:  # pragma: no cover - placeholder type
        pass

    stub.__version__ = "0.0.0"
    stub.ndarray = _NDArray  # type: ignore[attr-defined]
    stub.array = lambda *args, **kwargs: _NDArray()  # type: ignore[attr-defined]
    stub.asarray = stub.array  # type: ignore[attr-defined]
    stub.float64 = float  # type: ignore[attr-defined]
    stub.int64 = int  # type: ignore[attr-defined]
    stub.generic = object  # type: ignore[attr-defined]
    return stub


_install_stub("numpy", _build_numpy_stub)


def _build_ray_data_stub() -> types.ModuleType:
    stub = types.ModuleType("ray.data")

    def from_items(items):  # pragma: no cover - simple passthrough
        return items

    stub.from_items = from_items  # type: ignore[attr-defined]
    stub.Dataset = object  # type: ignore[attr-defined]
    return stub


_install_stub("ray.data", _build_ray_data_stub)

import app as app_module  # noqa: E402
from app import app  # noqa: E402
from router_dispatch import RouterHandleAdapter, dispatch_batch_item  # noqa: E402
from router import RouterService, _render_metrics  # noqa: E402


class StubRouter:
    def __init__(self, routes: list[dict[str, str]]) -> None:
        self.routes = list(routes)
        self.success_calls: list[tuple[str, str, float]] = []
        self.failure_calls: list[tuple[str, str, str, bool]] = []

    def route(self, job_id: str) -> dict[str, str]:
        if not self.routes:
            raise RuntimeError("no routes available")
        return self.routes.pop(0)

    def success(self, job_id: str, node: str, latency_ms: float) -> None:
        self.success_calls.append((job_id, node, latency_ms))

    def failure(self, job_id: str, node: str, error: str, retry: bool) -> dict[str, str]:
        self.failure_calls.append((job_id, node, error, retry))
        if not self.routes:
            return {"node": node, "url": "", "attempt": 0}
        return self.routes.pop(0)


class StubAsyncResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def json(self) -> dict[str, object]:
        return self._payload

    def raise_for_status(self) -> None:
        return None


class StubAsyncClient:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    async def __aenter__(self) -> "StubAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        return False

    async def post(self, url: str, json: dict[str, object], timeout: float) -> StubAsyncResponse:
        return StubAsyncResponse(self._payload)


def router_payload() -> dict[str, object]:
    return {
        "nodes": [
            {"name": "dgx", "infer_url": "http://dgx", "health_url": "http://dgx/health", "controller": "dgx", "priority": 0},
            {"name": "radeon", "infer_url": "http://radeon", "health_url": "http://radeon/health", "controller": "radeon", "priority": 1},
            {"name": "standby", "infer_url": "http://standby", "health_url": "http://standby/health", "controller": "standby", "priority": 2},
        ],
        "interval_s": 0.1,
        "threshold": 3,
        "max_retries": 3,
        "metrics_port": 9999,
        "actor_name": "router-test",
        "primary_count": 2,
    }


def stub_config(**overrides: object) -> SimpleNamespace:
    defaults = {
        "router_max_request_retries": 3,
        "gateway_api_key": "",
        "serve_address": "http://127.0.0.1:8265",
        "infer_url": "http://127.0.0.1/v1/chat/completions",
        "model_name": "test-model",
        "expected_dgx_ip": "100.108.245.77",
        "expected_radeon_ip": "100.92.148.18",
        "expected_standby_ip": "100.112.76.83",
        "expected_runpod_ip": "100.68.169.47",
        "dgx_vllm_url": "http://100.108.245.77/infer/v1/chat/completions",
        "radeon_vllm_url": "http://100.92.148.18/infer/v1/chat/completions",
        "standby_vllm_url": "http://100.112.76.83/infer/v1/chat/completions",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_happy_path() -> None:
    router = StubRouter([
        {"node": "node-a", "url": "http://node-a", "attempt": 1},
        {"node": "node-b", "url": "http://node-b", "attempt": 1},
    ])
    payload = {"prompt": "hello"}
    responses: list[str] = []

    def request_fn(url: str, body: dict[str, object]) -> dict[str, object]:
        responses.append(url)
        assert body is payload
        return {"ok": True, "url": url}

    result_a = dispatch_batch_item("job-1", payload, router, request_fn, 3)
    result_b = dispatch_batch_item("job-2", payload, router, request_fn, 3)

    assert result_a["error"] is None and result_a["response"]["ok"] is True
    assert result_a["response"]["url"] == "http://node-a"
    assert result_a["response"]["_odyn_served_by"]["node"] == "node-a"
    assert result_b["error"] is None and result_b["response"]["url"] == "http://node-b"
    assert result_b["response"]["_odyn_served_by"]["node"] == "node-b"
    assert {call[1] for call in router.success_calls} == {"node-a", "node-b"}
    assert router.failure_calls == []


def test_chat_completion_routed(monkeypatch: pytest.MonkeyPatch) -> None:
    stub_router_calls: dict[str, list] = {"route": [], "success": []}

    class AsyncRouter:
        async def next(self, job_id: str) -> dict[str, object]:
            stub_router_calls["route"].append(job_id)
            return {"node": "dgx", "url": "http://dgx/run", "attempt": 1}

        async def success(self, job_id: str, node: str, latency_ms: float) -> None:
            stub_router_calls["success"].append((job_id, node))

        async def failure(self, *args, **kwargs):
            raise AssertionError("failure should not be invoked in happy path")

        async def metrics(self) -> dict[str, object]:
            return {}

    stub_router = AsyncRouter()
    client = TestClient(app)

    if hasattr(app_module._router_client, "cache_clear"):
        app_module._router_client.cache_clear()
    monkeypatch.setattr(app_module, "_router_client", lambda: stub_router)
    monkeypatch.setattr(app_module, "load_runtime_config", lambda: stub_config())
    monkeypatch.setattr(app_module.httpx, "AsyncClient", lambda **_: StubAsyncClient({"id": "cmpl-1", "object": "chat.completion", "created": 1, "model": "test-model", "choices": [], "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}}))

    response = client.post("/v1/chat/completions", json={"model": "test-model", "messages": [{"role": "user", "content": "hi"}]})
    assert response.status_code == 200
    assert stub_router_calls["route"]


def test_health_check_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_controller_calls: list[str] = []

    class DummyController:
        def start(self) -> None:
            fake_controller_calls.append("start")

    async def always_unhealthy(node, timeout):
        return False

    monkeypatch.setattr("router._controller", lambda name, cfg: DummyController())
    monkeypatch.setattr("router._load_cfg", lambda: stub_config())
    monkeypatch.setattr("router._log", lambda msg: None)
    monkeypatch.setattr("router._log_retry", lambda *args, **kwargs: None)
    monkeypatch.setattr("router._probe", always_unhealthy)

    async def scenario() -> None:
        loop = asyncio.get_event_loop()
        service = RouterService(router_payload(), start_tasks=False, loop=loop)
        node = service.state.nodes["dgx"]

        for _ in range(6):
            await service._probe_node(node)

        assert fake_controller_calls == ["start"]
        assert service.state.nodes["dgx"].status == "respawning"
        assert service.state.nodes["standby"].status == "primary"

    asyncio.run(scenario())


def test_failover_reroute() -> None:
    router = StubRouter([
        {"node": "node-a", "url": "http://node-a", "attempt": 1},
        {"node": "node-b", "url": "http://node-b", "attempt": 2},
    ])

    def failing_request(url: str, payload: dict[str, object]) -> dict[str, object]:
        if "node-a" in url:
            raise RuntimeError("node failure")
        return {"ok": True}

    result = dispatch_batch_item("job-99", {"value": 1}, router, failing_request, 2)

    assert result["error"] is None and result["response"]["ok"] is True
    assert result["response"]["_odyn_served_by"]["node"] == "node-b"
    assert router.failure_calls and router.failure_calls[0][0] == "job-99"


def test_no_silent_drops() -> None:
    router = StubRouter([
        {"node": "node-a", "url": "http://node-a", "attempt": 1},
    ])

    def failing_request(url: str, payload: dict[str, object]) -> dict[str, object]:
        raise RuntimeError("boom")

    result = dispatch_batch_item("job-drop", {"value": 1}, router, failing_request, 1)
    assert result["error"] == "boom"
    assert router.failure_calls == [("job-drop", "node-a", "boom", False)]


def test_respawn_rejoins_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("router._controller", lambda name, cfg: type("C", (), {"start": lambda self: None})())
    monkeypatch.setattr("router._load_cfg", lambda: stub_config())
    monkeypatch.setattr("router._log", lambda msg: None)
    monkeypatch.setattr("router._log_retry", lambda *args, **kwargs: None)

    async def scenario() -> None:
        loop = asyncio.get_event_loop()
        service = RouterService(router_payload(), start_tasks=False, loop=loop)
        dgx = service.state.nodes["dgx"]

        await service._handle_failure(dgx)
        assert service.state.nodes["standby"].status == "primary"

        await service._handle_recovery(dgx)
        seen_nodes = set()
        for idx in range(3):
            route = await service.route(f"job-{idx}")
            seen_nodes.add(route["node"])
            await service.report_success(f"job-{idx}", route["node"], 10.0)

        assert seen_nodes == {"dgx", "radeon"}

    asyncio.run(scenario())


def test_quick_recovery_cycle(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_controller_calls: list[str] = []
    probe_count = {"dgx": 0}

    class DummyController:
        def start(self) -> None:
            fake_controller_calls.append("start")

    async def probe_with_network_heal(node, timeout):
        if node.spec.name != "dgx":
            return True
        probe_count["dgx"] += 1
        return probe_count["dgx"] >= 4

    monkeypatch.setattr("router._controller", lambda name, cfg: DummyController())
    monkeypatch.setattr("router._load_cfg", lambda: stub_config())
    monkeypatch.setattr("router._log", lambda msg: None)
    monkeypatch.setattr("router._log_retry", lambda *args, **kwargs: None)
    monkeypatch.setattr("router._probe", probe_with_network_heal)

    async def scenario() -> None:
        loop = asyncio.get_event_loop()
        service = RouterService(router_payload(), start_tasks=False, loop=loop)
        dgx = service.state.nodes["dgx"]
        standby = service.state.nodes["standby"]

        for _ in range(3):
            await service._probe_node(dgx)

        assert dgx.status == "respawning"
        assert standby.status == "primary"

        await service._probe_node(dgx)
        assert dgx.status == "primary"
        assert dgx.respawns == 1
        assert fake_controller_calls == ["start"]

        seen_nodes = set()
        for idx in range(6):
            route = await service.route(f"job-recover-{idx}")
            seen_nodes.add(route["node"])
            await service.report_success(f"job-recover-{idx}", route["node"], 8.0)

        assert seen_nodes == {"dgx", "radeon"}

    asyncio.run(scenario())


def test_nodes_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    app_module.REQUEST_COUNTS_RATE.clear()

    if hasattr(app_module._router_client, "cache_clear"):
        app_module._router_client.cache_clear()
    async def metrics_stub() -> dict[str, object]:
        return {
            "dgx": {
                "status": "primary",
                "jobs_total": 5,
                "jobs_failed": 0,
                "respawns": 1,
                "last_latency_ms": 12.5,
                "last_check": 100.0,
                "failures": 0,
                "last_error": None,
                "inflight": 0,
                "latency_p50_ms": 11.0,
                "latency_p95_ms": 20.0,
                "latency_p99_ms": 25.0,
            }
        }

    class MetricsRouter:
        async def metrics(self) -> dict[str, object]:
            return await metrics_stub()

    monkeypatch.setattr(app_module, "_router_client", lambda: MetricsRouter())
    monkeypatch.setattr(app_module, "_build_nodes_view", lambda cfg, metrics, include_offline: [{
        "name": "NVIDIA DGX Spark - n02",
        "node_ip": "100.108.245.77",
        "ray_node_id": "node-1",
        "status": "LIVE",
        "role": "active",
        "serve_replica_id": "SERVE_REPLICA::x",
        "serve_replica_state": "RUNNING",
        "last_check_iso": "1970-01-01T00:01:40Z",
        "last_error": None,
    }])
    monkeypatch.setattr(app_module, "load_runtime_config", lambda: stub_config())

    client = TestClient(app)
    response = client.get("/v1/nodes")
    assert response.status_code == 200
    body = response.json()
    assert body[0]["node_ip"] == "100.108.245.77"
    assert body[0]["status"] == "LIVE"
    assert body[0]["serve_replica_state"] == "RUNNING"


def test_nodes_endpoint_degraded_when_ray_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    class MetricsRouter:
        async def metrics(self) -> dict[str, object]:
            return {}

    monkeypatch.setattr(app_module, "_router_client", lambda: MetricsRouter())
    monkeypatch.setattr(app_module, "_build_nodes_view", lambda cfg, metrics, include_offline: (_ for _ in ()).throw(RuntimeError("dashboard timeout")))
    monkeypatch.setattr(app_module, "load_runtime_config", lambda: stub_config())

    client = TestClient(app)
    response = client.get("/v1/nodes")
    assert response.status_code == 503
    assert "Ray dashboard unavailable" in response.json()["detail"]


def test_node_status_starting_maps_to_warming() -> None:
    payload = app_module._node_status("dgx", {
        "status": "starting",
        "jobs_total": 0,
        "jobs_failed": 0,
        "respawns": 0,
        "last_latency_ms": 0.0,
        "last_check": 100.0,
        "last_error": None,
        "inflight": 0,
        "latency_p50_ms": 0.0,
        "latency_p95_ms": 0.0,
        "latency_p99_ms": 0.0,
    })
    assert payload["status"] == "WARMING"


def test_serve_lifecycle_mapping() -> None:
    assert app_module._resolve_lifecycle(["RUNNING"], True) == "LIVE"
    assert app_module._resolve_lifecycle(["STARTING"], True) == "WARMING"
    assert app_module._resolve_lifecycle(["PENDING"], True) == "WARMING"
    assert app_module._resolve_lifecycle(["RECOVERING"], True) == "WARMING"
    assert app_module._resolve_lifecycle([], True) == "IDLE"
    assert app_module._resolve_lifecycle(["FAILED"], True) == "FAILED"
    assert app_module._resolve_lifecycle([], True, False) == "FAILED"
    assert app_module._resolve_lifecycle([], False) == "OFFLINE"


def test_build_nodes_view_includes_unmanaged_alive_gpu(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = stub_config(expected_dgx_ip="100.108.245.77", expected_radeon_ip="100.92.148.18", expected_standby_ip="100.112.76.83", dgx_vllm_url="http://100.108.245.77", radeon_vllm_url="http://100.92.148.18", standby_vllm_url="http://100.112.76.83")
    metrics = {
        "dgx": {"status": "primary", "readiness_ok": True, "last_check": 100.0, "last_error": None},
        "radeon": {"status": "primary", "readiness_ok": True, "last_check": 100.0, "last_error": None},
    }
    alive = [
        {"node_ip": "100.108.245.77", "node_name": "dgx", "node_id": "ray-1", "state": "ALIVE", "resources_total": {"GPU": 1}},
        {"node_ip": "100.68.169.47", "node_name": "odyn-rtx5060-01", "node_id": "ray-2", "state": "ALIVE", "resources_total": {"GPU": 1}},
    ]
    replicas = {"100.108.245.77": [{"replica_id": "SERVE_REPLICA::dgx", "state": "RUNNING"}]}
    monkeypatch.setattr(app_module, "_alive_gpu_nodes", lambda _: alive)
    monkeypatch.setattr(app_module, "_replicas_by_ip", lambda _: replicas)
    rows = app_module._build_nodes_view(cfg, metrics)
    mapped = {row["node_ip"]: row for row in rows}
    assert mapped["100.108.245.77"]["status"] == "LIVE"
    assert mapped["100.68.169.47"]["status"] == "IDLE"
    assert mapped["100.68.169.47"]["name"] == "odyn-rtx5060-01"


def test_build_nodes_view_marks_missing_router_node_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = stub_config(expected_dgx_ip="100.108.245.77", expected_radeon_ip="100.92.148.18", expected_standby_ip="100.112.76.83", dgx_vllm_url="http://100.108.245.77", radeon_vllm_url="http://100.92.148.18", standby_vllm_url="http://100.112.76.83")
    metrics = {
        "standby": {"status": "reserve", "readiness_ok": False, "last_check": 100.0, "last_error": "heartbeat timeout"},
    }
    monkeypatch.setattr(app_module, "_alive_gpu_nodes", lambda _: [])
    monkeypatch.setattr(app_module, "_replicas_by_ip", lambda _: {})
    rows = app_module._build_nodes_view(cfg, metrics, include_offline=True)
    standby = next(row for row in rows if row["node_ip"] == "100.112.76.83")
    assert standby["status"] == "OFFLINE"
    assert standby["last_error"] == "heartbeat timeout"


def test_build_nodes_view_omits_missing_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = stub_config()
    metrics = {"standby": {"status": "reserve", "readiness_ok": False, "last_check": 100.0, "last_error": "down"}}
    monkeypatch.setattr(app_module, "_alive_gpu_nodes", lambda _: [])
    monkeypatch.setattr(app_module, "_replicas_by_ip", lambda _: {})
    rows = app_module._build_nodes_view(cfg, metrics)
    assert rows == []


def test_build_nodes_view_stable_name_from_ip_map(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = stub_config()
    alive = [{"node_ip": "100.68.169.47", "node_name": "100.68.169.47", "node_id": "ray-2", "state": "ALIVE", "resources_total": {"GPU": 1}}]
    monkeypatch.setattr(app_module, "_alive_gpu_nodes", lambda _: alive)
    monkeypatch.setattr(app_module, "_replicas_by_ip", lambda _: {})
    rows = app_module._build_nodes_view(cfg, {})
    assert rows[0]["name"] == "odyn-rtx5060-01"


def test_nodes_endpoint_include_offline_param(monkeypatch: pytest.MonkeyPatch) -> None:
    class MetricsRouter:
        async def metrics(self) -> dict[str, object]:
            return {}

    monkeypatch.setattr(app_module, "_router_client", lambda: MetricsRouter())
    monkeypatch.setattr(app_module, "load_runtime_config", lambda: stub_config())
    calls = []

    def fake_builder(cfg, metrics, include_offline):
        calls.append(include_offline)
        return []

    monkeypatch.setattr(app_module, "_build_nodes_view", fake_builder)
    client = TestClient(app)
    assert client.get("/v1/nodes").status_code == 200
    assert client.get("/v1/nodes?include_offline=true").status_code == 200
    assert calls == [False, True]


def test_running_chat_route_prefers_running_replica(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = stub_config()
    monkeypatch.setattr(app_module, "_replica_states_by_ip", lambda _: {"100.92.148.18": ["STARTING"], "100.108.245.77": ["RUNNING"]})
    route = app_module._running_chat_route(cfg, "job-1")
    assert route is not None
    assert route["url"] == "http://100.108.245.77/infer/v1/chat/completions"


def test_chat_fallback_uses_running_replica_when_router_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    class AsyncRouter:
        async def next(self, job_id: str) -> dict[str, object]:
            raise RuntimeError("no eligible")

        async def success(self, *args, **kwargs) -> None:
            return None

        async def failure(self, *args, **kwargs):
            raise RuntimeError("unused")

        async def metrics(self) -> dict[str, object]:
            return {}

    if hasattr(app_module._router_client, "cache_clear"):
        app_module._router_client.cache_clear()
    monkeypatch.setattr(app_module, "_router_client", lambda: AsyncRouter())
    monkeypatch.setattr(app_module, "load_runtime_config", lambda: stub_config())
    monkeypatch.setattr(app_module, "_running_chat_route", lambda cfg, job_id: {"node": "serve:100.108.245.77", "url": "http://100.108.245.77/infer/v1/chat/completions", "attempt": 1, "job_id": job_id})
    monkeypatch.setattr(app_module.httpx, "AsyncClient", lambda **_: StubAsyncClient({"id": "cmpl-1", "object": "chat.completion", "created": 1, "model": "test-model", "choices": [], "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}}))

    client = TestClient(app)
    response = client.post("/v1/chat/completions", json={"model": "test-model", "messages": [{"role": "user", "content": "hi"}]})
    assert response.status_code == 200


def test_prometheus_metrics() -> None:
    loop = asyncio.new_event_loop()
    service = RouterService(router_payload(), start_tasks=False, loop=loop)
    node = service.state.nodes["dgx"]
    node.jobs_total = 3
    node.jobs_failed = 1
    node.last_latency_ms = 15.0
    node.latency_samples.extend([10.0, 20.0, 30.0])
    metrics_text = _render_metrics(service.state).decode()
    loop.close()
    assert "router_latency_p95_ms{node=\"dgx\"}" in metrics_text


def test_benchmark_script() -> None:
    script_path = Path(__file__).resolve().parents[1] / "benchmark" / "run_benchmark.py"
    proc = subprocess.run([sys.executable, str(script_path)], capture_output=True, text=True, check=True)
    payload = json.loads(proc.stdout)
    assert payload["within_sla"] is True


def test_no_hardcoded_urls() -> None:
    src_root = Path(__file__).resolve().parents[1] / "src"
    offenders: list[tuple[str, int, str]] = []
    for path in src_root.rglob("*.py"):
        for idx, line in enumerate(path.read_text().splitlines(), 1):
            if "http://" in line:
                segment = line.split("http://", 1)[1]
                prefix = segment.split("/", 1)[0]
                host = prefix.split(":", 1)[0]
                if host and host[0].isdigit() and host not in {"127.0.0.1", "0.0.0.0"}:
                    offenders.append((str(path), idx, line.strip()))
    assert offenders == []
