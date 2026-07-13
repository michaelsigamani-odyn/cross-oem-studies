from dataclasses import dataclass
from pathlib import Path

from config import RuntimeConfig
from serve_ops import (
    assert_cluster_nodes_present,
    assert_replica_spread,
    assert_split_tolerance,
    check_metric_health,
    compute_split,
    ensure_remote_grafana_host,
    list_running_replicas,
    post_count,
    rollback_to_snapshot,
    run_deploy,
    run_load,
    snapshot_serve_config,
    wait_healthy,
)


@dataclass
class E2EResult:
    codes: dict[int, int]
    deltas: dict[str, int]
    split_pct: dict[str, float]
    max_error_pct: float


def run_e2e(cfg: RuntimeConfig) -> E2EResult:
    ensure_remote_grafana_host(cfg)
    check_metric_health(cfg)
    assert_cluster_nodes_present(cfg)
    replicas = list_running_replicas(cfg)
    assert_replica_spread(cfg, replicas)
    before = {r.replica_id: post_count(cfg, r) for r in replicas}
    codes = run_load(cfg)
    after = {r.replica_id: post_count(cfg, r) for r in replicas}
    split = compute_split(cfg, before, after)
    _assert_http_ok(cfg, codes)
    assert_split_tolerance(cfg, split)
    return E2EResult(codes, split.deltas, split.split_pct, split.max_error_pct)


def _assert_http_ok(cfg: RuntimeConfig, codes: dict[int, int]) -> None:
    if codes.get(200, 0) == cfg.requests:
        return
    raise RuntimeError(f"expected {cfg.requests} HTTP 200 responses, got {codes}")


def run_safe_refactor(cfg: RuntimeConfig) -> E2EResult:
    snapshot = snapshot_serve_config(cfg)
    try:
        run_deploy(cfg)
        wait_healthy(cfg)
        return run_e2e(cfg)
    except Exception as error:
        rollback_to_snapshot(cfg, snapshot)
        wait_healthy(cfg)
        raise RuntimeError(f"refactor validation failed and rollback was applied: {error}") from error
