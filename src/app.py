import asyncio
import functools
import logging
import os
import time
from urllib.parse import urlparse
from typing import Any, AsyncGenerator, Dict
from uuid import uuid4
from datetime import datetime
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import httpx
from ray import serve
from ray.job_submission import JobSubmissionClient
from schema import BatchJob, BatchRequestItem, BatchJobStatus, ChatCompletionRequest, ChatCompletionResponse
from persistence import ChatCompletionRecord, save
from config import load_runtime_config
from observability import emit_gateway_metrics, emit_batch_metrics
from serve_ops import _json_get

LOGGER = logging.getLogger("cross_oem.api")


class _AsyncRouterHandle:
    def __init__(self, handle: Any) -> None:
        self._h = handle

    async def next(self, job_id: str) -> Dict[str, Any]:
        return await self._h.route.remote(job_id)

    async def success(self, job_id: str, node: str, latency_ms: float) -> None:
        await self._h.report_success.remote(job_id, node, latency_ms)

    async def failure(self, job_id: str, node: str, error: str, retry: bool = True) -> Dict[str, Any]:
        return await self._h.report_failure.remote(job_id, node, error, retry)

    async def metrics(self) -> Dict[str, Any]:
        return await self._h.metrics.remote()


@functools.lru_cache(maxsize=1)
def _router_client() -> _AsyncRouterHandle:
    from router import ensure_router
    return _AsyncRouterHandle(ensure_router(load_runtime_config()))


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["x-served-by"],
)
JOBS: dict[str, BatchJob] = {}


def _build_stream_record(req: ChatCompletionRequest, raw: str, latency: float) -> ChatCompletionRecord:
    msg = [{"role": m.role, "content": m.content} for m in req.messages]
    return ChatCompletionRecord(f"chatcmpl-{uuid4().hex[:12]}", int(time.time()), req.model, msg, {"raw_stream": raw}, latency, 0, 0, 0)


async def _save_stream_record(req: ChatCompletionRequest, chunks: list[bytes], latency: float) -> None:
    raw = b"".join(chunks).decode("utf-8", errors="ignore")
    await save(_build_stream_record(req, raw, latency))


def _node_label(name: str) -> str:
    mapping = {"dgx": "NVIDIA DGX Spark — n02", "radeon": "AMD Radeon gfx1151 — n01", "standby": "dgx-spark-02"}
    return mapping.get(name, name)


def _router_lifecycle(status: str) -> str:
    lifecycle_map = {
        "primary": "LIVE",
        "reserve": "LIVE",
        "respawning": "WARMING",
        "starting": "WARMING",
    }
    return lifecycle_map.get(status, "OFFLINE")


def _node_host(url: str) -> str:
    return (urlparse(url).hostname or "").strip()


def _node_identifiers(cfg: Any) -> dict[str, set[str]]:
    invalid = {"", "127.0.0.1", "localhost", "0.0.0.0"}
    return {
        "dgx": {cfg.expected_dgx_ip, _node_host(cfg.dgx_vllm_url)} - invalid,
        "radeon": {cfg.expected_radeon_ip, _node_host(cfg.radeon_vllm_url)} - invalid,
        "standby": {cfg.expected_standby_ip, _node_host(cfg.standby_vllm_url)} - invalid,
    }


def _ray_nodes(cfg: Any) -> list[dict[str, Any]]:
    data = _json_get(f"{cfg.ray_dashboard_url}/api/v0/nodes")
    result = data.get("data", {}).get("result", {}).get("result", [])
    if isinstance(result, list):
        return result
    return []


def _node_resources(node: dict[str, Any]) -> dict[str, Any]:
    return node.get("resources_total", {})


def _node_ip_value(node: dict[str, Any]) -> str:
    return str(node.get("node_ip", "")).strip()


def _node_name_value(node: dict[str, Any]) -> str:
    name = str(node.get("node_name", "")).strip()
    return name if name else _node_ip_value(node)


def _is_ip_like(value: str) -> bool:
    parts = value.split(".")
    return len(parts) == 4 and all(part.isdigit() for part in parts)


def _ip_aliases(cfg: Any) -> dict[str, str]:
    return {
        cfg.expected_dgx_ip: "odyn-dgx",
        cfg.expected_radeon_ip: "odyn-radeon",
        cfg.expected_standby_ip: "odyn-dgx2",
        cfg.expected_runpod_ip: "odyn-rtx5060-01",
    }


def _stable_node_name(cfg: Any, node: dict[str, Any]) -> str:
    name = _node_name_value(node)
    if name and not _is_ip_like(name):
        return name
    return _ip_aliases(cfg).get(_node_ip_value(node), name)


def _has_gpu_capacity(node: dict[str, Any]) -> bool:
    resources = _node_resources(node)
    keys = set(resources.keys())
    if any(key in keys for key in {"GPU", "AMD_GPU", "NVIDIA_GPU", "STANDBY_GPU"}):
        return True
    return any(key.startswith("accelerator_type:") for key in keys)


def _alive_gpu_nodes(cfg: Any) -> list[dict[str, Any]]:
    nodes = _ray_nodes(cfg)
    return [node for node in nodes if node.get("state") == "ALIVE" and _has_gpu_capacity(node) and not node.get("is_head_node", False)]


def _replica_states_by_ip(cfg: Any) -> dict[str, list[str]]:
    data = _json_get(f"{cfg.ray_dashboard_url}/api/serve/applications/")
    replicas = data.get("applications", {}).get("qwen7b", {}).get("deployments", {}).get("VLLMDeployment", {}).get("replicas", [])
    states: dict[str, list[str]] = {}
    for replica in replicas:
        node_ip = str(replica.get("node_ip", "")).strip()
        state = str(replica.get("state", "")).strip()
        if node_ip and state:
            states.setdefault(node_ip, []).append(state)
    return states


def _replicas_by_ip(cfg: Any) -> dict[str, list[dict[str, str]]]:
    data = _json_get(f"{cfg.ray_dashboard_url}/api/serve/applications/")
    replicas = data.get("applications", {}).get("qwen7b", {}).get("deployments", {}).get("VLLMDeployment", {}).get("replicas", [])
    grouped: dict[str, list[dict[str, str]]] = {}
    for replica in replicas:
        node_ip = str(replica.get("node_ip", "")).strip()
        if not node_ip:
            continue
        grouped.setdefault(node_ip, []).append({
            "replica_id": str(replica.get("replica_id", "")).strip(),
            "state": str(replica.get("state", "")).strip(),
        })
    return grouped


def _resolve_lifecycle(replica_states: list[str], joined: bool, usable: bool = True) -> str:
    if any(state == "RUNNING" for state in replica_states):
        return "LIVE"
    if any(state in {"STARTING", "UPDATING", "DEPLOYING", "RESTARTING", "PENDING", "RECOVERING"} for state in replica_states):
        return "WARMING"
    if any(state in {"FAILED", "STOPPING", "STOPPED", "UNHEALTHY"} for state in replica_states):
        return "FAILED"
    if not usable:
        return "FAILED" if joined else "OFFLINE"
    if joined:
        return "IDLE"
    return "OFFLINE"


def _replica_priority(state: str) -> int:
    if state == "RUNNING":
        return 0
    if state in {"STARTING", "UPDATING", "DEPLOYING", "RESTARTING", "PENDING", "RECOVERING"}:
        return 1
    if state in {"FAILED", "STOPPING", "STOPPED", "UNHEALTHY"}:
        return 2
    return 3


def _preferred_replica(replicas: list[dict[str, str]]) -> dict[str, str] | None:
    if not replicas:
        return None
    return sorted(replicas, key=lambda item: _replica_priority(item.get("state", "")))[0]


def _router_metrics_by_ip(cfg: Any, metrics: dict[str, Any]) -> dict[str, dict[str, Any]]:
    identifiers = _node_identifiers(cfg)
    mapped: dict[str, dict[str, Any]] = {}
    for name, row in metrics.items():
        for node_ip in identifiers.get(name, set()):
            mapped[node_ip] = {"router_name": name, **row}
    return mapped


def _router_role(row: dict[str, Any] | None) -> str:
    if not row:
        return "unmanaged"
    status = str(row.get("status", "")).strip()
    if status == "primary":
        return "active"
    if status == "reserve":
        return "standby"
    return "unavailable"


def _iso_ts(value: Any) -> str | None:
    if value is None:
        return None
    return datetime.utcfromtimestamp(float(value)).isoformat() + "Z"


def _node_row(name: str, node_ip: str, ray_node_id: str | None, status: str, role: str, replica: dict[str, str] | None, router_row: dict[str, Any] | None, joined: bool) -> dict[str, Any]:
    row = router_row or {}
    return {
        "name": name,
        "node_ip": node_ip,
        "ray_node_id": ray_node_id,
        "status": status,
        "role": role,
        "raw_status": row.get("status", "unmanaged"),
        "serve_replica_id": replica.get("replica_id") if replica else None,
        "serve_replica_state": replica.get("state") if replica else None,
        "last_check_iso": _iso_ts(row.get("last_check")),
        "last_error": row.get("last_error"),
        "replica_states": [replica.get("state")] if replica else [],
        "joined": joined,
        # Router-observed workload/SLA telemetry (zeroed for unmanaged nodes).
        "gpu_utilisation": 0.0,
        "memory_used_mib": 0,
        "memory_total_mib": 0,
        "jobs_total": row.get("jobs_total", 0),
        "jobs_failed": row.get("jobs_failed", 0),
        "respawns": row.get("respawns", 0),
        "inflight": row.get("inflight", 0),
        "last_latency_ms": row.get("last_latency_ms", 0.0),
        "latency_p50_ms": row.get("latency_p50_ms", 0.0),
        "latency_p95_ms": row.get("latency_p95_ms", 0.0),
        "latency_p99_ms": row.get("latency_p99_ms", 0.0),
        "sla_ok": row.get("sla_ok", True),
        "sla_p95_target_ms": row.get("sla_p95_target_ms", 0.0),
        "queue_depth": row.get("queue_depth", 0),
        "readiness_ok": row.get("readiness_ok", True),
    }


def _build_nodes_view(cfg: Any, metrics: dict[str, Any], include_offline: bool = False) -> list[dict[str, Any]]:
    alive_nodes = _alive_gpu_nodes(cfg)
    alive_by_ip = {_node_ip_value(node): node for node in alive_nodes if _node_ip_value(node)}
    replicas_by_ip = _replicas_by_ip(cfg)
    router_by_ip = _router_metrics_by_ip(cfg, metrics)
    rows: list[dict[str, Any]] = []
    for node_ip, node in alive_by_ip.items():
        replicas = replicas_by_ip.get(node_ip, [])
        states = [item.get("state", "") for item in replicas if item.get("state")]
        router_row = router_by_ip.get(node_ip)
        usable = bool(router_row.get("readiness_ok", True)) if router_row else True
        status = _resolve_lifecycle(states, True, usable)
        role = _router_role(router_row)
        chosen = _preferred_replica(replicas)
        last_error = (router_row or {}).get("last_error")
        row = _node_row(_stable_node_name(cfg, node), node_ip, str(node.get("node_id", "")) or None, status, role, chosen, router_row, True)
        row["last_error"] = last_error
        rows.append(row)
    if not include_offline:
        return rows
    alive_ips = set(alive_by_ip.keys())
    for node_ip, router_row in router_by_ip.items():
        if node_ip in alive_ips:
            continue
        replicas = replicas_by_ip.get(node_ip, [])
        states = [item.get("state", "") for item in replicas if item.get("state")]
        status = _resolve_lifecycle(states, False, bool(router_row.get("readiness_ok", False)))
        chosen = _preferred_replica(replicas)
        row = _node_row(_node_label(str(router_row.get("router_name", "unknown"))), node_ip, None, status, _router_role(router_row), chosen, router_row, False)
        if not row["last_error"]:
            row["last_error"] = "Node missing from Ray cluster"
        rows.append(row)
    return rows


def _configured_route_by_ip(cfg: Any) -> dict[str, str]:
    return {
        cfg.expected_dgx_ip: cfg.dgx_vllm_url,
        cfg.expected_radeon_ip: cfg.radeon_vllm_url,
        cfg.expected_standby_ip: cfg.standby_vllm_url,
    }


def _running_chat_route(cfg: Any, job_id: str) -> dict[str, Any] | None:
    routes = _configured_route_by_ip(cfg)
    running_ips = [ip for ip, states in _replica_states_by_ip(cfg).items() if any(state == "RUNNING" for state in states)]
    for node_ip in sorted(running_ips):
        if node_ip in routes:
            return {"node": f"serve:{node_ip}", "url": routes[node_ip], "attempt": 1, "job_id": job_id}
    return None


_ROUTE_COUNTER = 0


async def _radeon_healthy(cfg: Any) -> bool:
    base = urlparse(cfg.radeon_vllm_url)
    health_url = f"{base.scheme}://{base.netloc}/health"
    try:
        async with httpx.AsyncClient(timeout=3) as client:
            r = await client.get(health_url)
            return r.status_code == 200
    except Exception:
        return False


def _demo_fallback_pool(cfg: Any) -> list[str]:
    replica_states = _replica_states_by_ip(cfg)
    configured = _configured_route_by_ip(cfg)
    pool = [
        url for ip, url in configured.items()
        if ip != cfg.expected_radeon_ip and any(s == "RUNNING" for s in replica_states.get(ip, []))
    ]
    pool.append(cfg.radeon_vllm_url)
    return pool


async def _report_routed_success(job_id: str, node: str, latency_ms: float) -> None:
    try:
        await _router_client().success(job_id, node, latency_ms)
    except Exception as err:  # pragma: no cover - reporting is best-effort
        LOGGER.warning("router success report failed job=%s err=%s", job_id, err)


async def _report_routed_failure(job_id: str, node: str, error: str) -> None:
    try:
        await _router_client().failure(job_id, node, error, False)
    except Exception as err:  # pragma: no cover - reporting is best-effort
        LOGGER.warning("router failure report failed job=%s err=%s", job_id, err)


async def _non_stream_with_direct_url(req: ChatCompletionRequest, bg: BackgroundTasks, url: str, node: str = "", job_id: str | None = None) -> ChatCompletionResponse:
    start = time.time()
    try:
        async with httpx.AsyncClient() as client:
            res = await _non_stream_chat(client, url, req.to_wire())
    except Exception as err:
        if job_id:
            await _report_routed_failure(job_id, node, str(err))
        raise
    latency = (time.time() - start) * 1000.0
    if node and not res.served_by:
        res.served_by = {"node": node, "url": url, "latency_ms": round(latency, 2)}
    if job_id:
        await _report_routed_success(job_id, node, latency)
    bg.add_task(save, _build_record(req, res, latency))
    emit_gateway_metrics(200, latency)
    return res


async def _stream_with_direct_url(req: ChatCompletionRequest, url: str, node: str = "", job_id: str | None = None) -> StreamingResponse:
    payload = req.to_wire()

    async def iterator() -> AsyncGenerator[bytes, None]:
        chunks: list[bytes] = []
        start = time.time()
        try:
            async with httpx.AsyncClient(timeout=90) as client:
                async with client.stream("POST", url, json=payload, timeout=90) as response:
                    async for chunk in response.aiter_bytes():
                        chunks.append(chunk)
                        yield chunk
        except Exception as err:
            if job_id:
                await _report_routed_failure(job_id, node, str(err))
            raise
        latency = (time.time() - start) * 1000.0
        if job_id:
            await _report_routed_success(job_id, node, latency)
        await _save_stream_record(req, chunks, latency)
        emit_gateway_metrics(200, latency)

    headers = {"x-served-by": node} if node else {}
    return StreamingResponse(iterator(), media_type="text/event-stream", headers=headers)


def _serve_lifecycle_by_node(node_names: list[str], metrics: dict[str, Any]) -> dict[str, dict[str, Any]]:
    cfg = load_runtime_config()
    joined_ips = {row["node_ip"] for row in _build_nodes_view(cfg, metrics) if row.get("joined")}
    replica_states = _replica_states_by_ip(cfg)
    identifiers = _node_identifiers(cfg)
    node_state: dict[str, dict[str, Any]] = {}
    for name in node_names:
        metrics_row = metrics.get(name, {})
        readiness_ok = bool(metrics_row.get("readiness_ok", False))
        keys = identifiers.get(name, set())
        states = [state for key in keys for state in replica_states.get(key, [])]
        joined = any(key in joined_ips for key in keys)
        lifecycle = _resolve_lifecycle(states, joined, readiness_ok)
        node_state[name] = {
            "lifecycle": lifecycle,
            "replica_states": states,
            "joined": joined,
            "readiness_ok": readiness_ok,
        }
    return node_state


def _node_status(name: str, data: dict[str, Any], serve_state: dict[str, Any] | None = None) -> dict[str, Any]:
    status = data["status"]
    lifecycle = serve_state["lifecycle"] if serve_state else _router_lifecycle(status)
    role = "active" if status == "primary" else "standby" if status == "reserve" else "unavailable"
    last_check_ts = data["last_check"]
    last_check_iso = datetime.utcfromtimestamp(last_check_ts).isoformat() + "Z"
    return {
        "name": _node_label(name),
        "status": lifecycle,
        "role": role,
        "raw_status": status,
        "gpu_utilisation": 0.0,
        "memory_used_mib": 0,
        "memory_total_mib": 0,
        "jobs_total": data["jobs_total"],
        "jobs_failed": data["jobs_failed"],
        "respawns": data["respawns"],
        "last_latency_ms": data["last_latency_ms"],
        "last_check": last_check_ts,
        "last_check_iso": last_check_iso,
        "last_error": data.get("last_error"),
        "inflight": data.get("inflight", 0),
        "latency_p50_ms": data.get("latency_p50_ms", 0.0),
        "latency_p95_ms": data.get("latency_p95_ms", 0.0),
        "latency_p99_ms": data.get("latency_p99_ms", 0.0),
        "readiness_ok": data.get("readiness_ok", False),
        "replica_states": serve_state.get("replica_states", []) if serve_state else [],
        "joined": serve_state.get("joined", False) if serve_state else False,
    }


def _build_record(req: ChatCompletionRequest, res: ChatCompletionResponse, latency: float) -> ChatCompletionRecord:
    msg = [{"role": m.role, "content": m.content} for m in req.messages]
    return ChatCompletionRecord(res.id, res.created, res.model, msg, res.__dict__, latency, res.usage.prompt_tokens, res.usage.completion_tokens, res.usage.total_tokens)


async def _post_chat(client: httpx.AsyncClient, url: str, body: dict) -> ChatCompletionResponse:
    r = await client.post(url, json=body, timeout=90)
    r.raise_for_status()
    return ChatCompletionResponse.from_wire(r.json())


async def _non_stream_retry(client: httpx.AsyncClient, url: str, body: dict, att: int) -> ChatCompletionResponse:
    try: return await _post_chat(client, url, body)
    except Exception as e:
        if att >= 5: raise e
        return await _handle_retry_sleep(client, url, body, att)


async def _handle_retry_sleep(client: httpx.AsyncClient, url: str, body: dict, att: int) -> ChatCompletionResponse:
    await asyncio.sleep(2)
    return await _non_stream_retry(client, url, body, att + 1)


async def _non_stream_chat(client: httpx.AsyncClient, url: str, req_body: dict) -> ChatCompletionResponse:
    return await _non_stream_retry(client, url, req_body, 0)


@app.post("/v1/chat/completions")
async def chat_completions(req: ChatCompletionRequest, request: Request, bg: BackgroundTasks, response: Response) -> Any:
    global _ROUTE_COUNTER
    cfg = load_runtime_config()
    job_id = f"chatcmpl-{uuid4().hex[:12]}"

    # Demo-only fallback: set DEMO_FALLBACK=1 in the serve config env_vars to activate.
    # When a DGX replica is missing and Radeon is healthy, round-robins between the
    # remaining DGX direct URL and the Radeon vLLM URL instead of relying on Ray Serve.
    if os.getenv("DEMO_FALLBACK", "0") == "1":
        try:
            replica_states = _replica_states_by_ip(cfg)
            running_dgx = sum(
                1 for ip, states in replica_states.items()
                if ip != cfg.expected_radeon_ip and any(s == "RUNNING" for s in states)
            )
            if running_dgx < 2 and await _radeon_healthy(cfg):
                pool = _demo_fallback_pool(cfg)
                url = pool[_ROUTE_COUNTER % len(pool)]
                _ROUTE_COUNTER += 1
                node = f"fallback:{_node_host(url)}"
                LOGGER.info("demo_fallback url=%s running_dgx=%d", url, running_dgx)
                if req.stream:
                    return await _stream_with_direct_url(req, url, node)
                response.headers["x-served-by"] = node
                return await _non_stream_with_direct_url(req, bg, url, node)
        except Exception as err:
            LOGGER.warning("demo_fallback check failed err=%s", err)

    # Normal path: route through the actor, fall back to a directly-observed running replica.
    url, node, routed = cfg.infer_url, "gateway", False
    try:
        route = await _router_client().next(job_id)
        url, node, routed = route["url"], route["node"], True
    except Exception as err:
        LOGGER.warning("router unavailable, attempting serve fallback err=%s", err)
        fallback = _running_chat_route(cfg, job_id)
        if fallback:
            url, node = fallback["url"], fallback["node"]

    report_id = job_id if routed else None
    if req.stream:
        return await _stream_with_direct_url(req, url, node, report_id)
    response.headers["x-served-by"] = node
    return await _non_stream_with_direct_url(req, bg, url, node, report_id)


def _write_input(job: BatchJob) -> None:
    import json, pathlib
    items_payload = [{"custom_id": item.custom_id, "request": item.request.to_wire()} for item in job.items]
    pathlib.Path(f"/tmp/batch_input_{job.id}.json").write_text(json.dumps(items_payload))


def _submit_ray_job(job_id: str) -> str:
    cfg = load_runtime_config()
    client = JobSubmissionClient(cfg.serve_address)
    cmd = f"python3 /home/ubuntu/batch_job.py --input /tmp/batch_input_{job_id}.json --output /tmp/batch_output_{job_id}.json"
    env_vars = {
        "INFER_URL": cfg.infer_url,
        "ROUTER_ACTOR_NAME": cfg.router_actor_name,
        "ROUTER_MAX_REQUEST_RETRIES": str(cfg.router_max_request_retries),
    }
    runtime_env = _job_runtime_env({"pip": ["pandas", "pyarrow"], "env_vars": env_vars})
    return client.submit_job(entrypoint=cmd, runtime_env=runtime_env, metadata={"job_id": job_id})


def _job_runtime_env(runtime_env: dict[str, Any] | None) -> dict[str, Any]:
    env = {} if runtime_env is None else dict(runtime_env)
    env["working_dir"] = "/home/ubuntu/job-runtime"
    return env


def _load_output(job: BatchJob) -> None:
    import json, pathlib
    data = json.loads(pathlib.Path(f"/tmp/batch_output_{job.id}.json").read_text())
    for item, res in zip(job.items, data):
        item.response = ChatCompletionResponse.from_wire(res["response"]) if res["response"] else None
        item.error = res["error"]


def _build_offline_record(item: BatchRequestItem, jid: str, sub_id: str) -> ChatCompletionRecord:
    r, msg = item.response, [{"role": m.role, "content": m.content} for m in item.request.messages]
    res = r.__dict__ if r else {"error": item.error}
    pt, ct, tt = (r.usage.prompt_tokens, r.usage.completion_tokens, r.usage.total_tokens) if r else (0, 0, 0)
    rid, t = (r.id, r.created) if r else (f"err-{jid}", int(time.time()))
    return ChatCompletionRecord(rid, t, item.request.model, msg, res, 0.0, pt, ct, tt, {"ray_job_id": sub_id})


def _save_offline_item(item: BatchRequestItem, jid: str, sub_id: str) -> None:
    asyncio.create_task(save(_build_offline_record(item, jid, sub_id)))


def _finalize_status(job: BatchJob) -> None:
    _load_output(job)
    job.status = BatchJobStatus.COMPLETED if job.failed_count == 0 else BatchJobStatus.FAILED
    job.completed_at = int(time.time())
    for item in job.items: _save_offline_item(item, job.id, job.metadata.get("submission_id", ""))
    emit_batch_metrics(job.status.value)


def _update_state(job: BatchJob, ray_status: str) -> None:
    if ray_status == "SUCCEEDED" and job.status != BatchJobStatus.COMPLETED: _finalize_status(job)
    elif ray_status == "RUNNING" and job.status == BatchJobStatus.PENDING: job.status, job.started_at = BatchJobStatus.RUNNING, int(time.time())
    elif ray_status in ("FAILED", "STOPPED"): job.status = BatchJobStatus.FAILED if ray_status == "FAILED" else BatchJobStatus.CANCELLED


def _map_ray_status(job: BatchJob, ray_status: str) -> None:
    _update_state(job, ray_status)
    if job.status in {BatchJobStatus.COMPLETED, BatchJobStatus.FAILED, BatchJobStatus.CANCELLED}:
        emit_batch_metrics(job.status.value)



@app.post("/v1/batch")
async def create_batch(items: list[BatchRequestItem]) -> dict[str, Any]:
    job = BatchJob(items=items)
    JOBS[job.id] = job
    _write_input(job)
    job.metadata["submission_id"] = _submit_ray_job(job.id)
    return job.to_status_dict()


@app.get("/v1/batch/{id}")
async def get_batch(id: str) -> dict[str, Any]:
    if id not in JOBS: raise HTTPException(status_code=404, detail="Job not found")
    client = JobSubmissionClient(load_runtime_config().serve_address)
    _map_ray_status(JOBS[id], str(client.get_job_status(JOBS[id].metadata["submission_id"])))
    return JOBS[id].to_status_dict()


@app.get("/v1/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}


REQUEST_COUNTS_RATE = []


def _check_rate_limit() -> None:
    import time; now = time.time()
    while REQUEST_COUNTS_RATE and REQUEST_COUNTS_RATE[0] < now - 10: REQUEST_COUNTS_RATE.pop(0)
    if len(REQUEST_COUNTS_RATE) >= 15: raise HTTPException(status_code=429, detail="Too Many Requests")
    REQUEST_COUNTS_RATE.append(now)


def _validate_api_key(r: Request) -> None:
    _check_rate_limit()
    k = r.headers.get("x-api-key")
    cfg = load_runtime_config()
    if not cfg.gateway_api_key:
        return
    if not k:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if k != cfg.gateway_api_key:
        raise HTTPException(status_code=403, detail="Forbidden")


@app.post("/v1/jobs")
async def create_job(request: Request) -> dict[str, Any]:
    _validate_api_key(request)
    b = await request.json()
    if "entrypoint" not in b: raise HTTPException(status_code=400, detail="Missing entrypoint")
    runtime_env = _job_runtime_env(b.get("runtime_env"))
    sub_id = JobSubmissionClient(load_runtime_config().serve_address).submit_job(entrypoint=b["entrypoint"], runtime_env=runtime_env)
    return {"submission_id": sub_id}


@app.get("/v1/jobs/{jobId}")
async def get_job_status(jobId: str, request: Request) -> dict[str, Any]:
    _validate_api_key(request)
    try: return {"status": JobSubmissionClient(load_runtime_config().serve_address).get_job_status(jobId).value}
    except Exception as e: raise HTTPException(status_code=404, detail=str(e))


@app.get("/v1/jobs/{jobId}/logs")
async def get_job_logs(jobId: str, request: Request) -> dict[str, Any]:
    _validate_api_key(request)
    try: return {"logs": JobSubmissionClient(load_runtime_config().serve_address).get_job_logs(jobId)}
    except Exception as e: raise HTTPException(status_code=404, detail=str(e))


@app.get("/v1/nodes")
async def get_nodes(request: Request, include_offline: bool = False) -> list[dict[str, Any]]:
    _validate_api_key(request)
    metrics: dict[str, Any] = {}
    try:
        metrics = await _router_client().metrics()
    except Exception as err:
        LOGGER.warning("router metrics unavailable err=%s", err)
    try:
        return _build_nodes_view(load_runtime_config(), metrics, include_offline)
    except Exception as err:
        LOGGER.error("node_status_degraded error=%s", err)
        raise HTTPException(status_code=503, detail=f"Ray dashboard unavailable: {err}") from err


@serve.deployment(num_replicas=1, ray_actor_options={"num_gpus": 0})
@serve.ingress(app)
class GatewayDeployment:
    def __init__(self) -> None:
        pass


gateway = GatewayDeployment.bind()
