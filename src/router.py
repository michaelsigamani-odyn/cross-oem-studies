from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional

import httpx
import ray

from config import RuntimeConfig
from dgx_worker import DGXWorkerController
from failover import log_to_cloudwatch
from radeon_worker import RadeonWorkerController
from standby_worker import StandbyWorkerController
from observability import emit_structured_log, MetricDatum, emit_metrics


LOGGER = logging.getLogger("cross_oem.router")


@dataclass
class NodeSpec:
    name: str
    infer_url: str
    health_url: str
    controller: str
    priority: int


@dataclass
class NodeRuntime:
    spec: NodeSpec
    status: str
    failures: int = 0
    jobs_total: int = 0
    jobs_failed: int = 0
    respawns: int = 0
    last_latency_ms: float = 0.0
    last_check: float = field(default_factory=time.time)
    last_error: Optional[str] = None
    latency_samples: Deque[float] = field(default_factory=lambda: deque(maxlen=1024))
    recover_to_primary: bool = False
    readiness_ok: bool = False

    def mark_primary(self) -> None:
        self.status = "primary"
        self.failures = 0

    def mark_reserve(self) -> None:
        self.status = "reserve"
        self.failures = 0

    def mark_failed(self) -> None:
        self.status = "failed"
        self.failures = 0

    def mark_respawning(self) -> None:
        self.status = "respawning"
        self.failures = 0

    def mark_respawned(self, recover_to_primary: bool) -> None:
        self.respawns += 1
        self.last_error = None
        if recover_to_primary:
            self.mark_primary()
        else:
            self.mark_reserve()
        self.recover_to_primary = False


@dataclass
class RouterConfig:
    nodes: List[NodeSpec]
    interval_s: float
    threshold: int
    max_retries: int
    metrics_port: int
    actor_name: str
    primary_count: int
    sla_p95_ms: float = 30000.0
    queue_max: int = 64
    queue_timeout_s: float = 15.0

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "RouterConfig":
        specs = [NodeSpec(**entry) for entry in payload["nodes"]]
        return cls(
            specs,
            payload["interval_s"],
            payload["threshold"],
            payload["max_retries"],
            payload["metrics_port"],
            payload["actor_name"],
            payload["primary_count"],
            payload.get("sla_p95_ms", 30000.0),
            payload.get("queue_max", 64),
            payload.get("queue_timeout_s", 15.0),
        )


class RouterState:
    def __init__(self, cfg: RouterConfig) -> None:
        self.cfg = cfg
        self.nodes = self._build_nodes()
        self.pointer = 0
        self.inflight: Dict[str, str] = {}
        self.attempts: Dict[str, int] = {}
        self.queue: Deque[str] = deque()

    def _build_nodes(self) -> Dict[str, NodeRuntime]:
        nodes = {spec.name: NodeRuntime(spec, "reserve") for spec in self.cfg.nodes}
        for node in nodes.values():
            node.readiness_ok = True
        for node in sorted(nodes.values(), key=lambda n: n.spec.priority)[: self.cfg.primary_count]:
            node.mark_primary()
        return nodes

    def active(self) -> List[NodeRuntime]:
        primaries = [node for node in self.nodes.values() if node.status == "primary"]
        return primaries if primaries else [node for node in self.nodes.values() if node.status == "reserve"]

    def within_sla(self, node: NodeRuntime) -> bool:
        """True when the node's observed p95 latency honours the SLA target."""
        if self.cfg.sla_p95_ms <= 0 or not node.latency_samples:
            return True
        return _latency_snapshot(node.latency_samples)["latency_p95_ms"] <= self.cfg.sla_p95_ms

    def pick(self) -> NodeRuntime:
        choices = sorted(self.active(), key=lambda node: node.spec.priority)
        if not choices:
            raise RuntimeError("no eligible nodes available")
        preferred = [node for node in choices if self.within_sla(node)] or choices
        choice = preferred[self.pointer % len(preferred)]
        self.pointer = (self.pointer + 1) % len(preferred)
        return choice

    def register_dispatch(self, job_id: str, node: NodeRuntime) -> int:
        attempt = self.attempts.get(job_id, 0) + 1
        self.attempts[job_id] = attempt
        self.inflight[job_id] = node.spec.name
        node.jobs_total += 1
        return attempt

    def promote_reserve(self) -> None:
        reserves = [node for node in self.nodes.values() if node.status == "reserve" and node.readiness_ok]
        if reserves:
            sorted(reserves, key=lambda node: node.spec.priority)[0].mark_primary()

    def reconcile_primaries(self) -> None:
        eligible = sorted([node for node in self.nodes.values() if node.status in {"primary", "reserve"}], key=lambda node: node.spec.priority)
        for idx, node in enumerate(eligible):
            node.mark_primary() if idx < self.cfg.primary_count else node.mark_reserve()

    def record_failure(self, node: NodeRuntime) -> None:
        node.jobs_failed += 1

    def on_failover(self, node: NodeRuntime, recover_to_primary: bool) -> None:
        node.recover_to_primary = recover_to_primary
        node.mark_failed()

    def snapshot(self) -> Dict[str, Any]:
        return {
            node.spec.name: {
                "status": node.status,
                "jobs_total": node.jobs_total,
                "jobs_failed": node.jobs_failed,
                "respawns": node.respawns,
                "last_latency_ms": node.last_latency_ms,
                "last_check": node.last_check,
                "failures": node.failures,
                "last_error": node.last_error,
                "readiness_ok": node.readiness_ok,
                "inflight": sum(1 for current in self.inflight.values() if current == node.spec.name),
                "sla_ok": self.within_sla(node),
                "sla_p95_target_ms": self.cfg.sla_p95_ms,
                "queue_depth": len(self.queue),
                **_latency_snapshot(node.latency_samples),
            }
            for node in self.nodes.values()
        }

    def clear_job(self, job_id: str) -> None:
        self.inflight.pop(job_id, None)
        self.attempts.pop(job_id, None)

    def inflight_node(self, job_id: str) -> Optional[str]:
        return self.inflight.get(job_id)


def _health(url: str) -> str:
    base = url.split("/infer/v1")[0]
    return f"{base}/health"


def _spec(name: str, url: str, controller: str, priority: int) -> NodeSpec:
    return NodeSpec(name, url, _health(url), controller, priority)


def _router_payload(cfg: RuntimeConfig) -> Dict[str, Any]:
    specs = [_spec("dgx", cfg.dgx_vllm_url, "dgx", 0), _spec("radeon", cfg.radeon_vllm_url, "radeon", 1), _spec("standby", cfg.standby_vllm_url, "standby", 2)]
    return {
        "nodes": [spec.__dict__ for spec in specs],
        "interval_s": cfg.router_health_interval_s,
        "threshold": cfg.router_failure_threshold,
        "max_retries": cfg.router_max_request_retries,
        "metrics_port": cfg.router_metrics_port,
        "actor_name": cfg.router_actor_name,
        "primary_count": cfg.router_primary_count,
        "sla_p95_ms": cfg.router_sla_p95_ms,
        "queue_max": cfg.router_queue_max,
        "queue_timeout_s": cfg.router_queue_timeout_s,
    }


async def _probe_ok(node: NodeRuntime, timeout: float) -> bool:
    async with httpx.AsyncClient(timeout=timeout) as client:
        return (await client.get(node.spec.health_url)).status_code == 200


async def _probe(node: NodeRuntime, timeout: float) -> bool:
    try:
        return await _probe_ok(node, timeout)
    except Exception:
        return False


async def _probe_chat(node: NodeRuntime, timeout: float) -> bool:
    payload = {"model": os.getenv("MODEL_NAME", "qwen2.5-7b"), "messages": [{"role": "user", "content": "ping"}], "max_tokens": 1}
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(node.spec.infer_url, json=payload)
            return response.status_code == 200
    except Exception:
        return False


def _controller(name: str, cfg: RuntimeConfig):
    mapping = {"dgx": DGXWorkerController, "radeon": RadeonWorkerController}
    return mapping.get(name, StandbyWorkerController)(cfg)


def _log(msg: str) -> None:
    log_to_cloudwatch(msg)


def _log_retry(job_id: str, from_node: str, to_node: str, attempt: int, error: str) -> None:
    message = f"RETRY job_id={job_id} from={from_node} to={to_node} attempt={attempt} error={error}"
    LOGGER.warning(message)
    _log(message)


def _active_standby_lists(state: RouterState) -> Dict[str, List[str]]:
    active = sorted([node.spec.name for node in state.nodes.values() if node.status == "primary"])
    standby = sorted([node.spec.name for node in state.nodes.values() if node.status == "reserve"])
    return {"active": active, "standby": standby}


def _percentile(data: List[float], pct: float) -> float:
    if not data:
        return 0.0
    k = (len(data) - 1) * pct
    lower = math.floor(k)
    upper = math.ceil(k)
    if lower == upper:
        return data[int(k)]
    lower_val = data[lower]
    upper_val = data[upper]
    return lower_val + (upper_val - lower_val) * (k - lower)


def _latency_snapshot(samples: Deque[float]) -> Dict[str, float]:
    if not samples:
        return {"latency_p50_ms": 0.0, "latency_p95_ms": 0.0, "latency_p99_ms": 0.0}
    sorted_samples = sorted(samples)
    return {
        "latency_p50_ms": _percentile(sorted_samples, 0.5),
        "latency_p95_ms": _percentile(sorted_samples, 0.95),
        "latency_p99_ms": _percentile(sorted_samples, 0.99),
    }


def _metric_rows(node: NodeRuntime, inflight: int, quantiles: Dict[str, float], sla_ok: bool) -> List[str]:
    status = 1.0 if node.status in {"primary", "reserve"} else 0.0
    readiness = 1.0 if node.readiness_ok else 0.0
    return [
        f"router_jobs_total{{node=\"{node.spec.name}\"}} {node.jobs_total}\n",
        f"router_jobs_failed{{node=\"{node.spec.name}\"}} {node.jobs_failed}\n",
        f"router_respawns{{node=\"{node.spec.name}\"}} {node.respawns}\n",
        f"router_latency_ms{{node=\"{node.spec.name}\"}} {node.last_latency_ms}\n",
        f"router_status{{node=\"{node.spec.name}\"}} {status}\n",
        f"router_inflight{{node=\"{node.spec.name}\"}} {inflight}\n",
        f"router_readiness{{node=\"{node.spec.name}\"}} {readiness}\n",
        f"router_sla_ok{{node=\"{node.spec.name}\"}} {1.0 if sla_ok else 0.0}\n",
        f"router_latency_p50_ms{{node=\"{node.spec.name}\"}} {quantiles['latency_p50_ms']}\n",
        f"router_latency_p95_ms{{node=\"{node.spec.name}\"}} {quantiles['latency_p95_ms']}\n",
        f"router_latency_p99_ms{{node=\"{node.spec.name}\"}} {quantiles['latency_p99_ms']}\n",
        f"router_last_check_seconds{{node=\"{node.spec.name}\"}} {node.last_check}\n",
    ]


def _render_metrics(state: RouterState) -> bytes:
    rows = []
    for node in state.nodes.values():
        inflight = sum(1 for current in state.inflight.values() if current == node.spec.name)
        quantiles = _latency_snapshot(node.latency_samples)
        rows.extend(_metric_rows(node, inflight, quantiles, state.within_sla(node)))
    rows.append(f"router_queue_depth {len(state.queue)}\n")
    rows.append(f"router_sla_p95_target_ms {state.cfg.sla_p95_ms}\n")
    body = "# TYPE router_info gauge\n" + "".join(rows)
    return body.encode()


async def _metrics_handler(state: RouterState, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    body = _render_metrics(state)
    writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\n" + f"Content-Length: {len(body)}\r\n\r\n".encode() + body)
    await writer.drain()
    writer.close()


async def _serve_metrics(state: RouterState, port: int) -> None:
    server = await asyncio.start_server(lambda r, w: _metrics_handler(state, r, w), "0.0.0.0", port)
    async with server:
        await server.serve_forever()


def _load_cfg() -> RuntimeConfig:
    from config import load_runtime_config
    return load_runtime_config()


class RouterService:
    def __init__(self, payload: Dict[str, Any], *, start_tasks: bool = True, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        self.cfg = RouterConfig.from_payload(payload)
        self.state = RouterState(self.cfg)
        self.loop = loop or asyncio.get_event_loop()
        self.last_failover_time = None
        self.failed_node_name = None
        if start_tasks:
            self._start_tasks()

    def _start_tasks(self) -> None:
        self.loop.create_task(self._tick())
        self.loop.create_task(_serve_metrics(self.state, self.cfg.metrics_port))

    async def _tick(self) -> None:
        await asyncio.sleep(self.cfg.interval_s)
        await self._probe_all()
        self.loop.create_task(self._tick())

    async def _probe_all(self) -> None:
        await asyncio.gather(*[self._probe_node(node) for node in self.state.nodes.values()])

    async def _probe_node(self, node: NodeRuntime) -> None:
        healthy = await _probe(node, self.cfg.interval_s)
        node.last_check = time.time()
        node.failures = 0 if healthy else node.failures + 1
        node.readiness_ok = healthy
        if healthy and node.spec.controller == "standby":
            node.readiness_ok = await _probe_chat(node, self.cfg.interval_s)
        await self._post_probe(node, healthy)

    async def _post_probe(self, node: NodeRuntime, healthy: bool) -> None:
        if healthy:
            await self._handle_recovery(node)
            return
        if node.failures >= self.cfg.threshold:
            await self._handle_failure(node)

    def _record_failover_event(self, node: NodeRuntime) -> None:
        active_targets = self.state.active()
        has_routing_impact = any(target.spec.name == node.spec.name for target in active_targets)
        reserves = [n.spec.name for n in self.state.nodes.values() if n.status == "reserve"]
        standby = reserves[0] if reserves else "none"
        if has_routing_impact:
            self.last_failover_time, self.failed_node_name = time.time(), node.spec.name
        self.state.on_failover(node, has_routing_impact)
        if has_routing_impact:
            self.state.promote_reserve()
            state_lists = _active_standby_lists(self.state)
            if node.spec.name == "radeon":
                _log("Radeon backend unhealthy")
            if standby != "none":
                _log(f"Standby DGX Spark promoted: {standby}")
            _log(f"Active backends updated active={state_lists['active']} standby={state_lists['standby']}")
        emit_structured_log("odyn-failover", "events", {"event": "FAILOVER", "node": node.spec.name, "failed_node_id": node.spec.name, "standby_node_id": standby, "timestamp": time.time()})

    def _apply_failover(self, node: NodeRuntime) -> None:
        self._record_failover_event(node)
        node.mark_respawning()
        _log(f"FAILOVER {node.spec.name}")

    async def _respawn(self, node: NodeRuntime) -> None:
        _controller(node.spec.controller, _load_cfg()).start()

    async def _handle_failure(self, node: NodeRuntime) -> None:
        if node.status in {"failed", "respawning"}:
            return
        self._apply_failover(node)
        await self._respawn(node)

    async def _handle_recovery(self, node: NodeRuntime) -> None:
        if node.status in {"failed", "respawning"}:
            node.mark_respawned(node.recover_to_primary)
            self.state.reconcile_primaries()
            _log(f"RESPAWN {node.spec.name}")
            state_lists = _active_standby_lists(self.state)
            _log(f"Active backends updated active={state_lists['active']} standby={state_lists['standby']}")
        node.failures = 0

    async def _wait_in_queue(self, job_id: str) -> NodeRuntime:
        """FIFO admission queue used while no node is routable (all failed or
        respawning).  Callers wait up to ``queue_timeout_s`` for a node to
        recover or a reserve to be promoted before the request is rejected."""
        if len(self.state.queue) >= self.cfg.queue_max:
            raise RuntimeError("router queue full")
        self.state.queue.append(job_id)
        deadline = time.monotonic() + self.cfg.queue_timeout_s
        try:
            while time.monotonic() < deadline:
                if self.state.queue[0] == job_id:
                    try:
                        return self.state.pick()
                    except RuntimeError:
                        pass
                await asyncio.sleep(0.05)
            raise RuntimeError("no eligible nodes available (queue timeout)")
        finally:
            try:
                self.state.queue.remove(job_id)
            except ValueError:
                pass

    async def _acquire_node(self, job_id: str) -> NodeRuntime:
        try:
            return self.state.pick()
        except RuntimeError:
            if self.cfg.queue_timeout_s <= 0:
                raise
            return await self._wait_in_queue(job_id)

    async def route(self, job_id: str) -> Dict[str, Any]:
        node = await self._acquire_node(job_id)
        attempt = self.state.register_dispatch(job_id, node)
        node.last_error = None
        return {"node": node.spec.name, "url": node.spec.infer_url, "attempt": attempt}

    async def report_success(self, job_id: str, node_name: str, latency_ms: float) -> None:
        node = self.state.nodes[node_name]
        node.last_latency_ms, node.last_error = latency_ms, None
        node.latency_samples.append(latency_ms)
        self.state.clear_job(job_id)

    def _check_and_log_rto(self, node: str) -> None:
        if self.last_failover_time is not None:
            elapsed = time.time() - self.last_failover_time
            emit_structured_log("odyn-failover", "events", {"event": "RETRY", "node": node, "timestamp": time.time(), "elapsed_seconds": elapsed})
            emit_metrics([MetricDatum("failover_rto_seconds", elapsed, "Seconds")]); self.last_failover_time = None

    async def _handle_failure_retry(self, job_id: str, node_name: str, error: str) -> Dict[str, Any]:
        try: route = await self.route(job_id)
        except RuntimeError: self.state.clear_job(job_id); raise
        _log_retry(job_id, node_name, route["node"], route["attempt"], error)
        self._check_and_log_rto(route["node"])
        return route

    def _handle_failure_final(self, job_id: str, node_name: str) -> Dict[str, Any]:
        attempt = self.state.attempts.get(job_id, 0)
        self.state.clear_job(job_id)
        return {"node": node_name, "url": "", "attempt": attempt}

    async def report_failure(self, job_id: str, node_name: str, error: str, retry: bool = True) -> Dict[str, Any]:
        node = self.state.nodes[node_name]
        node.last_error = error; self.state.record_failure(node)
        await self._handle_failure(node)
        return await self._handle_failure_retry(job_id, node_name, error) if retry else self._handle_failure_final(job_id, node_name)

    async def metrics(self) -> Dict[str, Any]:
        return self.state.snapshot()

    def metrics_sync(self) -> Dict[str, Any]:
        return self.state.snapshot()


def ensure_router(cfg: RuntimeConfig) -> ray.actor.ActorHandle:
    try:
        return ray.get_actor(cfg.router_actor_name)
    except Exception:
        return _create_router(cfg)


def _create_router(cfg: RuntimeConfig) -> ray.actor.ActorHandle:
    payload = _router_payload(cfg)
    return FailoverRouter.options(
        name=cfg.router_actor_name,
        lifetime="detached",
        resources={"node:__internal_head__": 0.001},
    ).remote(payload)


FailoverRouter = ray.remote(max_restarts=-1)(RouterService)
