"""Node visibility verification for the cross-OEM Ray cluster.

Confirms that every expected node (head, DGX, Radeon, RunPod) is joined to
the Ray cluster and ALIVE, resolving identity from the runtime config.  The
fetcher is injectable so the verification logic is unit-testable without a
live dashboard.
"""
from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from config import RuntimeConfig
from serve_ops import _json_get

NodeFetcher = Callable[[RuntimeConfig], List[Dict[str, Any]]]


@dataclass(frozen=True)
class NodeCheck:
    name: str
    expected: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class VisibilityReport:
    checks: List[NodeCheck]

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)


def _fetch_nodes(cfg: RuntimeConfig) -> List[Dict[str, Any]]:
    data = _json_get(f"{cfg.ray_dashboard_url}/api/v0/nodes")
    result = data.get("data", {}).get("result", {}).get("result", [])
    return result if isinstance(result, list) else []


def _find_ip(payload: Any) -> str:
    if isinstance(payload, dict):
        for key in ("ssh_host", "host", "ip"):
            value = payload.get(key)
            if isinstance(value, str) and value:
                return value
        for value in payload.values():
            found = _find_ip(value)
            if found:
                return found
    if isinstance(payload, list):
        for entry in payload:
            found = _find_ip(entry)
            if found:
                return found
    return ""


def _runpod_ip(cfg: RuntimeConfig) -> str:
    if cfg.expected_runpod_ip:
        return cfg.expected_runpod_ip
    if not cfg.runpod_lookup_cmd:
        return ""
    try:
        proc = subprocess.run(cfg.runpod_lookup_cmd, shell=True, capture_output=True, text=True, check=True)
        return _find_ip(json.loads(proc.stdout))
    except Exception:
        return ""


def _node_ip(node: Dict[str, Any]) -> str:
    return str(node.get("node_ip", "")).strip()


def _is_alive(node: Dict[str, Any]) -> bool:
    return node.get("state") == "ALIVE"


def _check_by_ip(name: str, expected_ip: str, nodes: List[Dict[str, Any]]) -> NodeCheck:
    if not expected_ip:
        return NodeCheck(name, "", True, "skipped (no expected identity configured)")
    matches = [node for node in nodes if _node_ip(node) == expected_ip]
    if not matches:
        return NodeCheck(name, expected_ip, False, "not joined to Ray cluster")
    if not any(_is_alive(node) for node in matches):
        return NodeCheck(name, expected_ip, False, f"joined but state={matches[0].get('state')}")
    return NodeCheck(name, expected_ip, True, "ALIVE")


def _check_head(cfg: RuntimeConfig, nodes: List[Dict[str, Any]]) -> NodeCheck:
    heads = [node for node in nodes if node.get("is_head_node")]
    if not heads:
        return NodeCheck("head", cfg.expected_head_ip, False, "no head node reported")
    if cfg.expected_head_ip and all(_node_ip(node) != cfg.expected_head_ip for node in heads):
        return NodeCheck("head", cfg.expected_head_ip, False, f"head ip mismatch (saw {_node_ip(heads[0])})")
    alive = any(_is_alive(node) for node in heads)
    return NodeCheck("head", cfg.expected_head_ip, alive, "ALIVE" if alive else "head not ALIVE")


def _check_runpod(cfg: RuntimeConfig, nodes: List[Dict[str, Any]]) -> NodeCheck:
    ip = _runpod_ip(cfg)
    if ip:
        return _check_by_ip("runpod", ip, nodes)
    key = cfg.expected_runpod_resource_key
    matches = [node for node in nodes if key in node.get("resources_total", {})]
    if not matches:
        return NodeCheck("runpod", key, False, "no node advertises expected resource key")
    alive = any(_is_alive(node) for node in matches)
    return NodeCheck("runpod", key, alive, "ALIVE" if alive else "matched node not ALIVE")


def verify_node_visibility(cfg: RuntimeConfig, fetch_nodes: Optional[NodeFetcher] = None) -> VisibilityReport:
    nodes = (fetch_nodes or _fetch_nodes)(cfg)
    checks = [
        _check_head(cfg, nodes),
        _check_by_ip("dgx", cfg.expected_dgx_ip, nodes),
        _check_by_ip("radeon", cfg.expected_radeon_ip, nodes),
        _check_runpod(cfg, nodes),
    ]
    return VisibilityReport(checks)


def render_visibility_report(report: VisibilityReport) -> str:
    lines = ["[cross-oem] node visibility report"]
    for check in report.checks:
        state = "PASS" if check.passed else "FAIL"
        lines.append(f"  {check.name:<8} {state}  expected={check.expected or '-'}  {check.detail}")
    lines.append(f"  overall: {'PASS' if report.passed else 'FAIL'}")
    return "\n".join(lines)
