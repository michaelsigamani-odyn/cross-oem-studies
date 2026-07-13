"""Recovery (reboot/rejoin) verification aligned to Phase 1-3 acceptance.

Phases
------
1. baseline-visibility — all expected nodes joined & ALIVE before the drill
2. outage-detected     — the target node leaves the Ray cluster (operator
                         triggered by default; set ``RECOVERY_TRIGGER_REBOOT=1``
                         plus ``RECOVERY_REBOOT_CMD`` for automatic trigger)
3. rejoin-confirmed    — the node returns to ALIVE within the timeout

Writes a markdown report (default ``build/recovery-verification-report.md``,
override with ``RECOVERY_REPORT_PATH``) including per-phase PASS/FAIL and node
states post-recovery.  Fetcher/clock/sleep are injectable for tests.
"""
from __future__ import annotations

import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from config import RuntimeConfig, load_runtime_config
from node_visibility import NodeFetcher, _fetch_nodes, render_visibility_report, verify_node_visibility


@dataclass(frozen=True)
class PhaseResult:
    name: str
    passed: bool
    detail: str = ""


@dataclass(frozen=True)
class RecoveryReport:
    phases: List[PhaseResult]
    node_lines: List[str]
    report_path: str

    @property
    def overall_passed(self) -> bool:
        return all(phase.passed for phase in self.phases)


def _read_float(name: str, default: str) -> float:
    return float(os.getenv(name, default))


def _target_ip(cfg: RuntimeConfig) -> str:
    return os.getenv("RECOVERY_TARGET_IP", cfg.expected_radeon_ip)


def _node_state(nodes: List[Dict[str, Any]], ip: str) -> str:
    for node in nodes:
        if str(node.get("node_ip", "")).strip() == ip:
            return str(node.get("state", "UNKNOWN"))
    return "MISSING"


def _maybe_trigger_reboot() -> str:
    if os.getenv("RECOVERY_TRIGGER_REBOOT", "0") != "1":
        return "waiting for operator-triggered reboot"
    cmd = os.getenv("RECOVERY_REBOOT_CMD", "")
    if not cmd:
        return "RECOVERY_TRIGGER_REBOOT=1 but RECOVERY_REBOOT_CMD unset"
    try:
        subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True, timeout=60)
        return f"reboot triggered via: {cmd}"
    except Exception as err:
        return f"reboot command failed: {err}"


def _wait_for_state(
    cfg: RuntimeConfig,
    fetch_nodes: NodeFetcher,
    ip: str,
    want_alive: bool,
    timeout_s: float,
    poll_s: float,
    sleep: Callable[[float], None],
    clock: Callable[[], float],
) -> PhaseResult:
    name = "rejoin-confirmed" if want_alive else "outage-detected"
    deadline = clock() + timeout_s
    last_state = "UNKNOWN"
    while clock() <= deadline:
        try:
            last_state = _node_state(fetch_nodes(cfg), ip)
        except Exception as err:
            last_state = f"fetch-error: {err}"
        is_alive = last_state == "ALIVE"
        if is_alive == want_alive:
            return PhaseResult(name, True, f"node {ip} state={last_state}")
        sleep(poll_s)
    return PhaseResult(name, False, f"timed out after {timeout_s:.0f}s; node {ip} last state={last_state}")


def _node_lines(cfg: RuntimeConfig, fetch_nodes: NodeFetcher) -> List[str]:
    try:
        nodes = fetch_nodes(cfg)
    except Exception as err:
        return [f" node-listing failed: {err}"]
    lines = []
    for node in nodes:
        ip = str(node.get("node_ip", "?"))
        name = str(node.get("node_name", "")) or ip
        state = str(node.get("state", "UNKNOWN"))
        role = "head" if node.get("is_head_node") else "worker"
        lines.append(f" {name:<24} {ip:<16} {role:<7} {state}")
    return lines or [" no nodes reported"]


def _render_report(phases: List[PhaseResult], node_lines: List[str]) -> str:
    lines = ["# Cross-OEM Recovery Verification Report", "", f"Generated: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}", ""]
    for phase in phases:
        lines.append(f"## {phase.name}: {'PASS' if phase.passed else 'FAIL'}")
        lines.append("")
        lines.append("```")
        lines.append(phase.detail or "(no detail)")
        lines.append("```")
        lines.append("")
    lines.append("## Node states post-recovery")
    lines.append("")
    lines.append("```")
    lines.extend(node_lines)
    lines.append("```")
    lines.append("")
    overall = all(phase.passed for phase in phases)
    lines.append(f"**Overall: {'PASS' if overall else 'FAIL'}**")
    return "\n".join(lines) + "\n"


def _write_report(phases: List[PhaseResult], node_lines: List[str]) -> str:
    path = Path(os.getenv("RECOVERY_REPORT_PATH", "build/recovery-verification-report.md"))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_render_report(phases, node_lines))
    return str(path)


def run_recovery_verification(
    fetch_nodes: Optional[NodeFetcher] = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.time,
) -> RecoveryReport:
    cfg = load_runtime_config()
    fetcher = fetch_nodes or _fetch_nodes
    poll_s = _read_float("RECOVERY_POLL_INTERVAL_S", "5")
    target = _target_ip(cfg)

    visibility = verify_node_visibility(cfg, fetcher)
    baseline = PhaseResult("baseline-visibility", visibility.passed, render_visibility_report(visibility))

    trigger_detail = _maybe_trigger_reboot()
    outage = _wait_for_state(cfg, fetcher, target, False, _read_float("RECOVERY_OUTAGE_TIMEOUT_S", "600"), poll_s, sleep, clock)
    outage = PhaseResult(outage.name, outage.passed, f"{trigger_detail}\n{outage.detail}")

    rejoin = _wait_for_state(cfg, fetcher, target, True, _read_float("RECOVERY_REJOIN_TIMEOUT_S", "900"), poll_s, sleep, clock)

    phases = [baseline, outage, rejoin]
    node_lines = _node_lines(cfg, fetcher)
    report_path = _write_report(phases, node_lines)
    return RecoveryReport(phases, node_lines, report_path)
