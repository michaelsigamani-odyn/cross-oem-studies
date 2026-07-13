"""Tests for node visibility, demo, and recovery verifiers."""
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def _install_stub(module_name: str, builder) -> None:
    if module_name not in sys.modules:
        sys.modules[module_name] = builder()


def _build_boto_stub() -> types.ModuleType:
    stub = types.ModuleType("boto3")
    stub.client = lambda *args, **kwargs: SimpleNamespace(put_log_events=lambda **kw: None, put_metric_data=lambda **kw: None)  # type: ignore[attr-defined]
    return stub


_install_stub("boto3", _build_boto_stub)

from demo_verifier import DemoProbes, run_demo_verification  # noqa: E402
from node_visibility import render_visibility_report, verify_node_visibility  # noqa: E402
from recovery_verifier import run_recovery_verification  # noqa: E402


def _cfg(**overrides: object) -> SimpleNamespace:
    defaults = {
        "ray_dashboard_url": "http://127.0.0.1:8265",
        "expected_head_ip": "10.0.0.1",
        "expected_dgx_ip": "10.0.0.2",
        "expected_radeon_ip": "10.0.0.3",
        "expected_runpod_ip": "10.0.0.4",
        "expected_runpod_resource_key": "acceleratorType:Amd-Instinct-Mi300X-Oam",
        "runpod_lookup_cmd": "",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _node(ip: str, alive: bool = True, head: bool = False, resources: dict | None = None) -> dict:
    return {
        "node_ip": ip,
        "node_name": ip,
        "state": "ALIVE" if alive else "DEAD",
        "is_head_node": head,
        "resources_total": resources or {"GPU": 1},
    }


def _all_nodes() -> list[dict]:
    return [
        _node("10.0.0.1", head=True),
        _node("10.0.0.2"),
        _node("10.0.0.3"),
        _node("10.0.0.4"),
    ]


def test_visibility_passes_when_all_alive() -> None:
    report = verify_node_visibility(_cfg(), fetch_nodes=lambda cfg: _all_nodes())
    assert report.passed
    rendered = render_visibility_report(report)
    assert "overall: PASS" in rendered


def test_visibility_fails_on_missing_node() -> None:
    nodes = [node for node in _all_nodes() if node["node_ip"] != "10.0.0.3"]
    report = verify_node_visibility(_cfg(), fetch_nodes=lambda cfg: nodes)
    assert not report.passed
    radeon = next(check for check in report.checks if check.name == "radeon")
    assert not radeon.passed
    assert "not joined" in radeon.detail


def test_visibility_fails_on_dead_node() -> None:
    nodes = [_node("10.0.0.1", head=True), _node("10.0.0.2", alive=False), _node("10.0.0.3"), _node("10.0.0.4")]
    report = verify_node_visibility(_cfg(), fetch_nodes=lambda cfg: nodes)
    assert not report.passed


def test_visibility_runpod_via_resource_key() -> None:
    nodes = [
        _node("10.0.0.1", head=True),
        _node("10.0.0.2"),
        _node("10.0.0.3"),
        _node("10.9.9.9", resources={"acceleratorType:Amd-Instinct-Mi300X-Oam": 1}),
    ]
    report = verify_node_visibility(_cfg(expected_runpod_ip=""), fetch_nodes=lambda cfg: nodes)
    assert report.passed


def test_demo_verification_pass(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEMO_REPORT_PATH", str(tmp_path / "demo.md"))
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    probes = DemoProbes(
        pre_checks=lambda: (True, "nodes ok"),
        heterogeneous_load=lambda: (True, "3/3 endpoints 200"),
        failover_tracking=lambda: (True, "dgx=primary radeon=primary"),
        recovery_tracking=lambda: (True, "all recovered"),
    )
    report = run_demo_verification(probes)
    assert report.overall_passed
    assert [phase.name for phase in report.phases] == ["pre-checks", "heterogeneous-load", "failover-tracking", "recovery-tracking"]
    content = Path(report.report_path).read_text()
    assert "**Overall: PASS**" in content


def test_demo_verification_fail_and_probe_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEMO_REPORT_PATH", str(tmp_path / "demo.md"))
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)

    def boom() -> tuple[bool, str]:
        raise RuntimeError("probe exploded")

    probes = DemoProbes(
        pre_checks=lambda: (True, "ok"),
        heterogeneous_load=lambda: (False, "endpoint 503"),
        failover_tracking=boom,
        recovery_tracking=lambda: (True, "ok"),
    )
    report = run_demo_verification(probes)
    assert not report.overall_passed
    failover = next(phase for phase in report.phases if phase.name == "failover-tracking")
    assert "probe exploded" in failover.detail
    assert "**Overall: FAIL**" in Path(report.report_path).read_text()


def _recovery_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("RECOVERY_REPORT_PATH", str(tmp_path / "recovery.md"))
    monkeypatch.setenv("RECOVERY_POLL_INTERVAL_S", "0")
    monkeypatch.setenv("RECOVERY_OUTAGE_TIMEOUT_S", "5")
    monkeypatch.setenv("RECOVERY_REJOIN_TIMEOUT_S", "5")
    monkeypatch.setenv("RECOVERY_TARGET_IP", "10.0.0.3")
    monkeypatch.setenv("EXPECTED_HEAD_IP", "10.0.0.1")
    monkeypatch.setenv("EXPECTED_DGX_IP", "10.0.0.2")
    monkeypatch.setenv("EXPECTED_RADEON_IP", "10.0.0.3")
    monkeypatch.setenv("EXPECTED_RUNPOD_IP", "10.0.0.4")
    monkeypatch.delenv("RECOVERY_TRIGGER_REBOOT", raising=False)


def test_recovery_verification_full_cycle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _recovery_env(monkeypatch, tmp_path)
    calls = {"count": 0}

    def fetcher(cfg) -> list[dict]:
        calls["count"] += 1
        if calls["count"] <= 1:
            return _all_nodes()  # baseline: everything alive
        if calls["count"] <= 3:
            return [node for node in _all_nodes() if node["node_ip"] != "10.0.0.3"]  # outage
        return _all_nodes()  # rejoin

    report = run_recovery_verification(fetch_nodes=fetcher, sleep=lambda s: None)
    assert report.overall_passed
    assert [phase.name for phase in report.phases] == ["baseline-visibility", "outage-detected", "rejoin-confirmed"]
    assert any("10.0.0.3" in line for line in report.node_lines)
    assert "**Overall: PASS**" in Path(report.report_path).read_text()


def test_recovery_verification_times_out(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _recovery_env(monkeypatch, tmp_path)
    monkeypatch.setenv("RECOVERY_OUTAGE_TIMEOUT_S", "0")

    report = run_recovery_verification(fetch_nodes=lambda cfg: _all_nodes(), sleep=lambda s: None)
    assert not report.overall_passed
    outage = next(phase for phase in report.phases if phase.name == "outage-detected")
    assert not outage.passed
    assert "timed out" in outage.detail
