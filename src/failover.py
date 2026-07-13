import asyncio
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import boto3
import ray

@dataclass
class FailoverEvent:
    timestamp: float
    failed_node: str
    promoted_node: str

def _cw_client() -> Any:
    return boto3.client("logs", region_name="eu-central-1")

def _send_cw_event(client: Any, msg: str) -> Any:
    ts = int(time.time() * 1000)
    ev = [{"timestamp": ts, "message": msg}]
    return client.put_log_events(logGroupName="odyn-failover", logStreamName="events", logEvents=ev)

def log_to_cloudwatch(msg: str) -> None:
    try:
        _send_cw_event(_cw_client(), msg)
    except Exception as e:
        print(f"[failover] Failed to log to CloudWatch: {e}")

def _respawn_radeon(cfg: Any) -> None:
    from radeon_worker import RadeonWorkerController
    RadeonWorkerController(cfg).start()

def _respawn_dgx(cfg: Any) -> None:
    from dgx_worker import DGXWorkerController
    DGXWorkerController(cfg).start()

def trigger_node_respawn(node_name: str, cfg: Any) -> None:
    if "vast" in node_name or "radeon" in node_name:
        from config import load_runtime_config
        _respawn_radeon(load_runtime_config())
    elif "dgx" in node_name:
        _respawn_dgx(cfg)

@ray.remote(max_restarts=-1)
class FailoverController:
    def __init__(self, expected_nodes: List[Dict[str, str]], cfg_data: Dict[str, Any]) -> None:
        self.expected = expected_nodes
        self.cfg_data = cfg_data
        self.missed: Dict[str, int] = {n["ip"]: 0 for n in expected_nodes}
        self.failed_at: Dict[str, float] = {}
        self.active = True
        asyncio.create_task(self._loop())

    async def _loop(self) -> None:
        while self.active:
            await asyncio.sleep(5)
            await self._check()

    async def _check(self) -> None:
        alive_ips = {n["NodeManagerAddress"] for n in ray.nodes() if n.get("Alive")}
        for node in self.expected:
            await self._eval(node, node["ip"] in alive_ips)

    async def _eval(self, node: Dict[str, str], is_alive: bool) -> None:
        if is_alive:
            await self._handle_alive(node)
            return
        self.missed[node["ip"]] += 1
        if self.missed[node["ip"]] == 2:
            await self._handle_failed(node)

    async def _handle_alive(self, node: Dict[str, str]) -> None:
        if node["ip"] in self.failed_at:
            dur = time.time() - self.failed_at.pop(node["ip"])
            log_to_cloudwatch(f"RESPAWN: {node['name']} rejoined cluster in {dur:.2f}s")
        self.missed[node["ip"]] = 0

    async def _handle_failed(self, node: Dict[str, str]) -> None:
        self.failed_at[node["ip"]] = time.time()
        standby = self._find_standby(node["ip"])
        log_to_cloudwatch(f"FAILOVER: {node['name']} failed, promoted={standby}")
        from config import load_runtime_config
        trigger_node_respawn(node["name"], load_runtime_config())

    def _find_standby(self, failed_ip: str) -> str:
        for n in self.expected:
            if n["ip"] != failed_ip:
                return n["name"]
        return "none"
