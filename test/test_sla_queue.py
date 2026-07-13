"""Tests for SLA-aware routing, admission queueing, and routed batch dispatch."""
import asyncio
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def _install_stub(module_name: str, builder) -> None:
    if module_name not in sys.modules:
        sys.modules[module_name] = builder()


def _build_boto_stub() -> types.ModuleType:
    stub = types.ModuleType("boto3")

    class _ClientStub:
        def put_object(self, **kwargs):
            return None

        def put_metric_data(self, **kwargs):
            return None

        def put_log_events(self, **kwargs):
            return None

    stub.client = lambda *args, **kwargs: _ClientStub()  # type: ignore[attr-defined]
    return stub


def _build_ray_data_stub() -> types.ModuleType:
    stub = types.ModuleType("ray.data")
    stub.from_items = lambda items: items  # type: ignore[attr-defined]
    stub.Dataset = object  # type: ignore[attr-defined]
    return stub


_install_stub("boto3", _build_boto_stub)
_install_stub("ray.data", _build_ray_data_stub)
if not hasattr(sys.modules.get("ray"), "data"):
    sys.modules["ray"].data = sys.modules["ray.data"]  # type: ignore[attr-defined]

from router import RouterService  # noqa: E402


def _payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "nodes": [
            {"name": "dgx", "infer_url": "http://dgx", "health_url": "http://dgx/health", "controller": "dgx", "priority": 0},
            {"name": "radeon", "infer_url": "http://radeon", "health_url": "http://radeon/health", "controller": "radeon", "priority": 1},
            {"name": "standby", "infer_url": "http://standby", "health_url": "http://standby/health", "controller": "standby", "priority": 2},
        ],
        "interval_s": 0.1,
        "threshold": 3,
        "max_retries": 3,
        "metrics_port": 9998,
        "actor_name": "router-sla-test",
        "primary_count": 2,
        "sla_p95_ms": 100.0,
        "queue_max": 2,
        "queue_timeout_s": 0.5,
    }
    payload.update(overrides)
    return payload


def _silence_router(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("router._controller", lambda name, cfg: type("C", (), {"start": lambda self: None})())
    monkeypatch.setattr("router._load_cfg", lambda: None)
    monkeypatch.setattr("router._log", lambda msg: None)
    monkeypatch.setattr("router._log_retry", lambda *args, **kwargs: None)


def test_sla_breaching_node_deprioritised(monkeypatch: pytest.MonkeyPatch) -> None:
    _silence_router(monkeypatch)

    async def scenario() -> None:
        service = RouterService(_payload(), start_tasks=False, loop=asyncio.get_event_loop())
        # dgx breaches the 100ms p95 SLA; radeon stays within it.
        service.state.nodes["dgx"].latency_samples.extend([500.0] * 20)
        service.state.nodes["radeon"].latency_samples.extend([20.0] * 20)

        seen = set()
        for idx in range(4):
            route = await service.route(f"sla-{idx}")
            seen.add(route["node"])
            await service.report_success(f"sla-{idx}", route["node"], 20.0)

        assert seen == {"radeon"}
        snapshot = service.state.snapshot()
        assert snapshot["dgx"]["sla_ok"] is False
        assert snapshot["radeon"]["sla_ok"] is True

    asyncio.run(scenario())


def test_all_nodes_breaching_still_routes(monkeypatch: pytest.MonkeyPatch) -> None:
    _silence_router(monkeypatch)

    async def scenario() -> None:
        service = RouterService(_payload(), start_tasks=False, loop=asyncio.get_event_loop())
        for name in ("dgx", "radeon"):
            service.state.nodes[name].latency_samples.extend([500.0] * 20)

        route = await service.route("sla-degraded")
        assert route["node"] in {"dgx", "radeon"}

    asyncio.run(scenario())


def test_queue_waits_for_recovery(monkeypatch: pytest.MonkeyPatch) -> None:
    _silence_router(monkeypatch)

    async def scenario() -> None:
        service = RouterService(_payload(queue_timeout_s=2.0), start_tasks=False, loop=asyncio.get_event_loop())
        for name in ("dgx", "radeon", "standby"):
            await service._handle_failure(service.state.nodes[name])
        with pytest.raises(RuntimeError):
            service.state.pick()

        async def recover_later() -> None:
            await asyncio.sleep(0.15)
            await service._handle_recovery(service.state.nodes["dgx"])

        recovery = asyncio.ensure_future(recover_later())
        route = await service.route("queued-job")
        await recovery
        assert route["node"] == "dgx"
        assert len(service.state.queue) == 0

    asyncio.run(scenario())


def test_queue_timeout_rejects(monkeypatch: pytest.MonkeyPatch) -> None:
    _silence_router(monkeypatch)

    async def scenario() -> None:
        service = RouterService(_payload(queue_timeout_s=0.2), start_tasks=False, loop=asyncio.get_event_loop())
        for name in ("dgx", "radeon", "standby"):
            await service._handle_failure(service.state.nodes[name])

        with pytest.raises(RuntimeError, match="queue timeout"):
            await service.route("doomed-job")
        assert len(service.state.queue) == 0

    asyncio.run(scenario())


def test_queue_full_rejects(monkeypatch: pytest.MonkeyPatch) -> None:
    _silence_router(monkeypatch)

    async def scenario() -> None:
        service = RouterService(_payload(queue_max=1, queue_timeout_s=0.4), start_tasks=False, loop=asyncio.get_event_loop())
        for name in ("dgx", "radeon", "standby"):
            await service._handle_failure(service.state.nodes[name])

        first = asyncio.ensure_future(service.route("waiter-1"))
        await asyncio.sleep(0.05)
        with pytest.raises(RuntimeError, match="queue full"):
            await service.route("waiter-2")
        with pytest.raises(RuntimeError):
            await first

    asyncio.run(scenario())


def test_queue_depth_in_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    _silence_router(monkeypatch)
    from router import _render_metrics

    async def scenario() -> None:
        service = RouterService(_payload(), start_tasks=False, loop=asyncio.get_event_loop())
        text = _render_metrics(service.state).decode()
        assert "router_queue_depth 0" in text
        assert 'router_sla_ok{node="dgx"} 1.0' in text

    asyncio.run(scenario())


def test_routed_batch_annotates_served_by(monkeypatch: pytest.MonkeyPatch) -> None:
    import batch_job

    class FakeAdapter:
        def route(self, job_id: str) -> dict[str, object]:
            return {"node": "radeon", "url": "http://radeon/infer", "attempt": 1}

        def success(self, job_id: str, node: str, latency_ms: float) -> None:
            pass

        def failure(self, job_id: str, node: str, error: str, retry: bool) -> dict[str, object]:
            raise AssertionError("unexpected failure path")

    monkeypatch.setattr(batch_job.BatchDispatcher, "_post", lambda self, url, payload: {"id": "cmpl-batch", "url": url})

    items = [{"custom_id": "a", "request": {"model": "m", "messages": []}}]
    results = batch_job._run_routed_chat_completion(items, FakeAdapter())

    assert results[0]["error"] is None
    assert results[0]["response"]["_odyn_served_by"]["node"] == "radeon"


def test_batch_falls_back_without_router(monkeypatch: pytest.MonkeyPatch) -> None:
    import batch_job

    monkeypatch.setattr(batch_job, "_router_adapter", lambda: None)
    monkeypatch.setattr(batch_job.BatchDispatcher, "_post", lambda self, url, payload: {"id": "cmpl-static"})

    results = batch_job._run_chat_completion([{"custom_id": "a", "request": {"model": "m", "messages": []}}])
    assert results[0] == {"response": {"id": "cmpl-static"}, "error": None}
