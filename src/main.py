import argparse
from pathlib import Path

from commands import run_e2e, run_safe_refactor
from config import load_runtime_config
from demo_verifier import run_demo_verification
from recovery_verifier import run_recovery_verification
from node_visibility import render_visibility_report, verify_node_visibility
from serve_ops import assert_cluster_nodes_present, assert_replica_spread, check_metric_health, list_running_replicas, run_deploy, wait_healthy


from typing import Any


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cross-OEM deployment CLI")
    parser.add_argument("command", choices=["deploy", "check", "e2e", "safe-refactor", "start-radeon", "start-dgx", "start-rtx5090", "start-standby", "fix-grafana", "failover", "provision-vast-worker", "simulate-failover", "respawn-node", "cluster-status", "verify-node-visibility", "demo-verify", "recovery-verify"])
    parser.add_argument("--node", help="Node name for simulate-failover or respawn-node")
    return parser


def _run_deploy_only() -> None:
    cfg = load_runtime_config()
    run_deploy(cfg)
    wait_healthy(cfg)


def _run_check_only() -> None:
    cfg = load_runtime_config()
    check_metric_health(cfg)
    assert_cluster_nodes_present(cfg)
    assert_replica_spread(cfg, list_running_replicas(cfg))
    print("[cross-oem] cluster check PASS")


def _run_e2e() -> None:
    result = run_e2e(load_runtime_config())
    print("[cross-oem] response code counts", result.codes)
    print("[cross-oem] per-replica deltas", result.deltas)
    print("[cross-oem] per-replica split pct", {rid: round(pct, 2) for rid, pct in result.split_pct.items()})
    print(f"[cross-oem] split max error={result.max_error_pct:.2f}%")


def _run_safe_refactor() -> None:
    result = run_safe_refactor(load_runtime_config())
    print("[cross-oem] safe refactor PASS", {"max_error_pct": round(result.max_error_pct, 2)})


def _run_start_radeon() -> None:
    from radeon_worker import RadeonWorkerController
    RadeonWorkerController(load_runtime_config()).start()


def _run_start_dgx() -> None:
    from dgx_worker import DGXWorkerController
    DGXWorkerController(load_runtime_config()).start()


def _run_start_rtx5090() -> None:
    from rtx5090_worker import RTX5090WorkerController
    RTX5090WorkerController(load_runtime_config()).start()


def _run_start_standby() -> None:
    from standby_worker import StandbyWorkerController
    controller = StandbyWorkerController(load_runtime_config())
    controller.bootstrap()
    controller.start()
    controller.wait_joined()
    controller.start_vllm()
    controller.wait_ready()


def _run_fix_grafana() -> None:
    from grafana_fixer import GrafanaFixer
    GrafanaFixer(load_runtime_config()).fix()


def _run_failover() -> None:
    from failover_tester import FailoverTester
    FailoverTester(load_runtime_config()).run()


def _read_tid() -> int:
    import json, pathlib
    return int(json.loads(pathlib.Path("vast_template.json").read_text())["template_id"])


def _get_top_offer() -> int:
    import json, subprocess
    cmd = ["vastai", "search", "offers", "gpu_name=RTX_4090 num_gpus=1 rented=false", "--raw"]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return int(json.loads(res.stdout)[0]["id"])


def _run_provision_vast() -> None:
    import subprocess
    tid, oid = _read_tid(), _get_top_offer()
    subprocess.run(["vastai", "create", "instance", str(oid), "--template_id", str(tid), "--disk", "40"], check=True)


def _stop_vast() -> None:
    import os, subprocess
    instance_id = os.getenv("VAST_INSTANCE_ID", "")
    if not instance_id:
        raise RuntimeError("VAST_INSTANCE_ID is not set")
    subprocess.run(["vastai", "stop", "instance", instance_id], check=True)


def _stop_dgx(cfg: Any) -> None:
    import subprocess
    subprocess.run(["ssh", cfg.dgx_host, "docker stop ray-worker-dgx"], check=True)


def _stop_radeon(cfg: Any) -> None:
    from radeon_worker import RadeonWorkerController
    RadeonWorkerController(cfg).stop()


def _run_simulate_failover() -> None:
    cfg = load_runtime_config()
    node = _parser().parse_args().node
    if not node:
        node = "radeon"
    _stop_radeon(cfg) if "radeon" in node or "vast" in node else _stop_dgx(cfg)


def _start_radeon(cfg: Any) -> None:
    from radeon_worker import RadeonWorkerController
    RadeonWorkerController(cfg).start()


def _start_dgx(cfg: Any) -> None:
    from dgx_worker import DGXWorkerController
    DGXWorkerController(cfg).start()


def _run_respawn_node() -> None:
    cfg = load_runtime_config()
    node = _parser().parse_args().node
    if not node:
        node = "radeon"
    _start_radeon(cfg) if "radeon" in node or "vast" in node else _start_dgx(cfg)


def _print_nodes() -> None:
    from serve_ops import _json_get
    cfg = load_runtime_config()
    data = _json_get(f"{cfg.ray_dashboard_url}/api/cluster_status")["data"]["clusterStatus"]["loadMetricsReport"]["usageByNode"]
    for nid, usage in data.items():
        node_ip = next((k.split(":")[1] for k in usage if "node:" in k), "unknown")
        print(f"Node: {nid} | IP: {node_ip} | Resources: {list(usage.keys())}")


def _print_replicas(cfg: Any) -> None:
    from serve_ops import list_running_replicas
    for r in list_running_replicas(cfg):
        print(f"Replica: {r.replica_id} | Node IP: {r.node_ip} | Node ID: {r.node_id}")


def _run_cluster_status() -> None:
    cfg = load_runtime_config()
    _print_nodes()
    _print_replicas(cfg)


def _run_verify_node_visibility() -> None:
    report = verify_node_visibility(load_runtime_config())
    print(render_visibility_report(report))
    if not report.passed:
        raise RuntimeError("node visibility verification failed")


def _run_demo_verify() -> None:
    report = run_demo_verification()
    for phase in report.phases:
        print(f"[cross-oem] {phase.name}: {'PASS' if phase.passed else 'FAIL'}")
    print(f"[cross-oem] Overall Demo Status: {'PASS' if report.overall_passed else 'FAIL'}")
    print(f"[cross-oem] Report: {report.report_path}")
    if not report.overall_passed:
        raise RuntimeError("demo verification failed")


def _run_recovery_verify() -> None:
    report = run_recovery_verification()
    for phase in report.phases:
        print(f"[cross-oem] {phase.name}: {'PASS' if phase.passed else 'FAIL'}")
    print("[cross-oem] Node states post-recovery:")
    for line in report.node_lines:
        print(f"[cross-oem]{line}")
    print(f"[cross-oem] Overall: {'PASS' if report.overall_passed else 'FAIL'}")
    print(f"[cross-oem] Report: {report.report_path}")
    if not report.overall_passed:
        raise RuntimeError("recovery verification failed")


def main() -> None:
    command = _parser().parse_args().command
    {"deploy": _run_deploy_only, "check": _run_check_only, "e2e": _run_e2e, "safe-refactor": _run_safe_refactor, "start-radeon": _run_start_radeon, "start-dgx": _run_start_dgx, "start-rtx5090": _run_start_rtx5090, "start-standby": _run_start_standby, "fix-grafana": _run_fix_grafana, "failover": _run_failover, "provision-vast-worker": _run_provision_vast, "simulate-failover": _run_simulate_failover, "respawn-node": _run_respawn_node, "cluster-status": _run_cluster_status, "verify-node-visibility": _run_verify_node_visibility, "demo-verify": _run_demo_verify, "recovery-verify": _run_recovery_verify}[command]()


if __name__ == "__main__":
    main()
