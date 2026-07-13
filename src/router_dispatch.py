from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Dict

import ray


@dataclass
class RouterHandleAdapter:
    handle: Any

    def route(self, job_id: str) -> Dict[str, Any]:
        return ray.get(self.handle.route.remote(job_id))

    def success(self, job_id: str, node: str, latency_ms: float) -> None:
        ray.get(self.handle.report_success.remote(job_id, node, latency_ms))

    def failure(self, job_id: str, node: str, error: str, retry: bool) -> Dict[str, Any]:
        return ray.get(self.handle.report_failure.remote(job_id, node, error, retry))


def dispatch_batch_item(
    job_id: str,
    payload: dict,
    router: RouterHandleAdapter,
    request_fn: Callable[[str, dict], dict],
    max_attempts: int,
) -> dict:
    attempts = max_attempts
    cfg_attempts = max_attempts if max_attempts > 0 else 1
    route = router.route(job_id)
    while attempts > 0:
        node = route["node"]
        url = route["url"]
        attempt = route.get("attempt", cfg_attempts - attempts + 1)
        start = time.time()
        try:
            response = request_fn(url, payload)
            latency = (time.time() - start) * 1000.0
            router.success(job_id, node, latency)
            if isinstance(response, dict):
                response["_odyn_served_by"] = {"node": node, "url": url, "attempt": attempt, "latency_ms": round(latency, 2)}
            return {"response": response, "error": None}
        except Exception as err:
            attempts -= 1
            retry = attempts > 0
            try:
                route = router.failure(job_id, node, str(err), retry)
            except Exception as router_err:
                return {"response": None, "error": str(router_err)}
            if not retry:
                return {"response": None, "error": str(err)}
    return {"response": None, "error": "router exhausted retries"}
