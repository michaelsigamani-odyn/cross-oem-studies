"""End-to-end demo smoke verification (4 phases).

Phases
------
1. pre-checks               — expected nodes joined & ALIVE (node visibility)
2. heterogeneous-load       — a live chat completion succeeds on every demo
                              endpoint (gateway + per-OEM direct URLs)
3. failover-tracking        — the failover router is reachable and reports at
                              least ``primary_count`` routable nodes
4. recovery-tracking        — no node is stuck failed/respawning and every
                              routable node passes its readiness probe

Writes a markdown report (default ``build/demo-verification-report.md``,
override with ``DEMO_REPORT_PATH``) and optionally posts a summary to Slack
when ``SLACK_WEBHOOK_URL`` is set.  All probes are injectable for tests.
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx

from config import RuntimeConfig, load_runtime_config
from node_visibility import render_visibility_report, verify_node_visibility

Probe = Callable[[], Tuple[bool, str]]


@dataclass(frozen=True)
class PhaseResult:
    name: str
    passed: bool
    detail: str = ""


@dataclass(frozen=True)
class DemoReport:
    phases: List[PhaseResult]
    report_path: str

    @property
    def overall_passed(self) -> bool:
        return all(phase.passed for phase in self.phases)


@dataclass
class DemoProbes:
    pre_checks: Probe
    heterogeneous_load: Probe
    failover_tracking: Probe
    recovery_tracking: Probe


def _endpoint_urls(cfg: RuntimeConfig) -> List[str]:
    raw = os.getenv("DEMO_ENDPOINT_URLS", "")
    urls = [url.strip() for url in raw.split(",") if url.strip()]
    return urls if urls else [cfg.infer_url]

def _chat_payload(cfg: RuntimeConfig) -> Dict[str, object]:
    return {"model": cfg.model_name, "messages": [{"role": "user", "content": "ping"}], "max_tokens": 8}


def _probe_endpoint(url: str, payload: Dict[str, object], api_key: str) -> Tuple[bool, str]:
    headers = {"x-api-key": api_key} if api_key else {}
    try:
        response = httpx.post(url, json=payload, headers=headers, timeout=90)
        ok = response.status_code == 200
        return ok, f"{url} -> {response.status_code}"
    except Exception as err:
        return False, f"{url} -> {err}"


def _pre_checks(cfg: RuntimeConfig) -> Tuple[bool, str]:
    report = verify_node_visibility(cfg)
    return report.passed, render_visibility_report(report)


def _heterogeneous_load(cfg: RuntimeConfig) -> Tuple[bool, str]:
    payload = _chat_payload(cfg)
    results = [_probe_endpoint(url, payload, cfg.gateway_api_key) for url in _endpoint_urls(cfg)]
    return all(ok for ok, _ in results), "\n".join(detail for _, detail in results)


def _router_snapshot(cfg: RuntimeConfig) -> Dict[str, Dict[str, object]]:
    from router import ensure_router
    import ray
    if not ray.is_initialized():
        ray.init(address=os.getenv("RAY_ADDRESS", "auto"), ignore_reinit_error=True)
    return ray.get(ensure_router(cfg).metrics.remote())


def _failover_tracking(cfg: RuntimeConfig) -> Tuple[bool, str]:
    try:
        snapshot = _router_snapshot(cfg)
    except Exception as err:
        return False, f"router unreachable: {err}"
    routable = [name for name, row in snapshot.items() if row.get("status") in {"primary", "reserve"}]
    detail = ", ".join(f"{name}={row.get('status')}" for name, row in sorted(snapshot.items()))
    return len(routable) >= cfg.router_primary_count, detail


def _recovery_tracking(cfg: RuntimeConfig) -> Tuple[bool, str]:
    try:
        snapshot = _router_snapshot(cfg)
    except Exception as err:
        return False, f"router unreachable: {err}"
    stuck = {name: row for name, row in snapshot.items() if row.get("status") in {"failed", "respawning"}}
    not_ready = {name: row for name, row in snapshot.items() if row.get("status") in {"primary", "reserve"} and not row.get("readiness_ok", False)}
    if stuck:
        return False, f"nodes stuck in recovery: {sorted(stuck)}"
    if not_ready:
        return False, f"routable nodes failing readiness: {sorted(not_ready)}"
    return True, "all nodes recovered and ready"


def _default_probes(cfg: RuntimeConfig) -> DemoProbes:
    return DemoProbes(
        pre_checks=lambda: _pre_checks(cfg),
        heterogeneous_load=lambda: _heterogeneous_load(cfg),
        failover_tracking=lambda: _failover_tracking(cfg),
        recovery_tracking=lambda: _recovery_tracking(cfg),
    )


def _render_report(phases: List[PhaseResult]) -> str:
    lines = ["# Cross-OEM Demo Verification Report", "", f"Generated: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}", ""]
    for phase in phases:
        lines.append(f"## {phase.name}: {'PASS' if phase.passed else 'FAIL'}")
        lines.append("")
        lines.append("```")
        lines.append(phase.detail or "(no detail)")
        lines.append("```")
        lines.append("")
    overall = all(phase.passed for phase in phases)
    lines.append(f"**Overall: {'PASS' if overall else 'FAIL'}**")
    return "\n".join(lines) + "\n"


def _write_report(phases: List[PhaseResult]) -> str:
    path = Path(os.getenv("DEMO_REPORT_PATH", "build/demo-verification-report.md"))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_render_report(phases))
    return str(path)


def _post_slack(phases: List[PhaseResult]) -> None:
    webhook = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook:
        return
    overall = all(phase.passed for phase in phases)
    summary = " | ".join(f"{phase.name}: {'PASS' if phase.passed else 'FAIL'}" for phase in phases)
    try:
        httpx.post(webhook, json={"text": f"Cross-OEM demo verification {'PASS' if overall else 'FAIL'} — {summary}"}, timeout=10)
    except Exception:
        pass


def _run_phase(name: str, probe: Probe) -> PhaseResult:
    try:
        passed, detail = probe()
    except Exception as err:
        passed, detail = False, f"probe error: {err}"
    return PhaseResult(name, passed, detail)


def run_demo_verification(probes: Optional[DemoProbes] = None) -> DemoReport:
    resolved = probes or _default_probes(load_runtime_config())
    phases = [
        _run_phase("pre-checks", resolved.pre_checks),
        _run_phase("heterogeneous-load", resolved.heterogeneous_load),
        _run_phase("failover-tracking", resolved.failover_tracking),
        _run_phase("recovery-tracking", resolved.recovery_tracking),
    ]
    report_path = _write_report(phases)
    _post_slack(phases)
    return DemoReport(phases, report_path)
