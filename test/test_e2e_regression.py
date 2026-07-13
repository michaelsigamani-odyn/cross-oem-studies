import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from router import RouterService, _render_metrics
from router_dispatch import RouterHandleAdapter, dispatch_batch_item


class SyncRouter:
    def __init__(self, service: RouterService, loop: asyncio.AbstractEventLoop) -> None:
        self.service = service
        self.loop = loop
        self.failure_calls: list[tuple[str, str, bool]] = []

    def route(self, job_id: str) -> dict[str, object]:
        return self.loop.run_until_complete(self.service.route(job_id))

    def success(self, job_id: str, node: str, latency_ms: float) -> None:
        self.loop.run_until_complete(self.service.report_success(job_id, node, latency_ms))

    def failure(self, job_id: str, node: str, error: str, retry: bool) -> dict[str, object]:
        self.failure_calls.append((job_id, node, retry))
        return self.loop.run_until_complete(self.service.report_failure(job_id, node, error, retry))


def regression_payload() -> dict[str, object]:
    return {
        "nodes": [
            {"name": "dgx", "infer_url": "http://dgx", "health_url": "http://dgx/health", "controller": "dgx", "priority": 0},
            {"name": "node-b", "infer_url": "http://node-b", "health_url": "http://node-b/health", "controller": "node-b", "priority": 1},
            {"name": "standby", "infer_url": "http://standby", "health_url": "http://standby/health", "controller": "standby", "priority": 2},
        ],
        "interval_s": 0.1,
        "threshold": 2,
        "max_retries": 3,
        "metrics_port": 9988,
        "actor_name": "router-regression",
        "primary_count": 2,
    }


def config_stub() -> object:
    return type("Cfg", (), {})()


def test_e2e_regression(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("router._controller", lambda name, cfg: type("Ctrl", (), {"start": lambda self: None})())
    monkeypatch.setattr("router._load_cfg", config_stub)
    monkeypatch.setattr("router._log", lambda msg: None)
    monkeypatch.setattr("router._log_retry", lambda *args, **kwargs: None)

    loop = asyncio.new_event_loop()
    service = RouterService(regression_payload(), start_tasks=False, loop=loop)
    router = SyncRouter(service, loop)

    failed_nodes = {"dgx"}

    def request_fn(url: str, payload: dict[str, object]) -> dict[str, object]:
        if any(node in url for node in failed_nodes):
            raise RuntimeError("node failed")
        return {"url": url}

    jobs = [f"job-{idx}" for idx in range(8)]
    results = []

    for job in jobs[:2]:
        results.append(dispatch_batch_item(job, {"prompt": job}, router, request_fn, 3))

    results.append(dispatch_batch_item(jobs[2], {"prompt": "fail"}, router, request_fn, 3))
    assert router.failure_calls

    failed_nodes.clear()
    loop.run_until_complete(service._handle_recovery(service.state.nodes["dgx"]))

    for job in jobs[3:]:
        results.append(dispatch_batch_item(job, {"prompt": job}, router, request_fn, 3))

    loop.close()

    assert all(entry["error"] is None for entry in results)
    urls = {entry["response"]["url"] for entry in results if entry["response"]}
    assert any("dgx" in url for url in urls)
    assert any("node-b" in url for url in urls)
    metrics_text = _render_metrics(service.state).decode()
    assert "router_jobs_total" in metrics_text
    assert {"dgx", "node-b", "standby"}.issubset(service.state.nodes.keys())
