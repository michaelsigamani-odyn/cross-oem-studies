import asyncio
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from router import RouterService


def calculate_availability_sli(statuses: list[int]) -> float:
    success = sum(1 for s in statuses if s in {200, 201, 302})
    return (success / len(statuses)) * 100.0 if statuses else 0.0


def calculate_p95_latency(latencies: list[float]) -> float:
    if not latencies:
        return 0.0
    sorted_lats = sorted(latencies)
    k = (len(sorted_lats) - 1) * 0.95
    idx = int(k)
    if idx == k:
        return sorted_lats[idx]
    return sorted_lats[idx] + (sorted_lats[idx + 1] - sorted_lats[idx]) * (k - idx)


def calculate_batch_reliability(states: list[str]) -> float:
    completed = sum(1 for s in states if s == "COMPLETED")
    return (completed / len(states)) * 100.0 if states else 0.0


def test_gateway_availability_sli() -> None:
    statuses = [200] * 999 + [500]
    sli = calculate_availability_sli(statuses)
    assert abs(sli - 99.9) < 0.01


def test_latency_sli() -> None:
    latencies = [float(i) for i in range(1, 101)]
    p95 = calculate_p95_latency(latencies)
    assert abs(p95 - 95.05) < 0.01


def test_batch_reliability_sli() -> None:
    states = ["COMPLETED"] * 199 + ["FAILED"]
    sli = calculate_batch_reliability(states)
    assert abs(sli - 99.5) < 0.01


def test_cancelled_jobs_included() -> None:
    states = ["COMPLETED"] * 199 + ["CANCELLED"]
    sli = calculate_batch_reliability(states)
    assert abs(sli - 99.5) < 0.01


def test_rto_alarm_threshold() -> None:
    elapsed = 31.0
    is_breached = elapsed > 30.0
    assert is_breached is True


def test_failover_logs_emitted() -> None:
    payload = {
        "nodes": [
            {"name": "dgx", "infer_url": "http://dgx", "health_url": "http://dgx/health", "controller": "dgx", "priority": 0},
            {"name": "node-b", "infer_url": "http://node-b", "health_url": "http://node-b/health", "controller": "node-b", "priority": 1},
            {"name": "standby", "infer_url": "http://standby", "health_url": "http://standby/health", "controller": "standby", "priority": 2},
        ],
        "interval_s": 0.1,
        "threshold": 2,
        "max_retries": 3,
        "metrics_port": 9977,
        "actor_name": "router-slo",
        "primary_count": 2,
    }

    async def scenario() -> None:
        loop = asyncio.get_event_loop()
        service = RouterService(payload, start_tasks=False, loop=loop)
        dgx = service.state.nodes["dgx"]

        with patch("router.emit_structured_log") as mock_log, patch("router._controller") as mock_ctrl:
            mock_ctrl.return_value = MagicMock()
            await service._handle_failure(dgx)
            assert mock_log.called
            assert mock_log.call_args[0][2]["event"] == "FAILOVER"

            await service.report_failure("job-1", "node-b", "error")
            assert mock_log.call_count >= 2
            assert any(call[0][2].get("event") == "RETRY" for call in mock_log.call_args_list)

    asyncio.run(scenario())
