#!/usr/bin/env python3

import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, List


def _load_raw_csv(path: Path) -> List[Dict[str, int]]:
    jobs = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            jobs.append({
                "input_tokens": int(row["input_tokens"]),
                "output_tokens": int(row["output_tokens"]),
            })
            if len(jobs) >= 49:
                break
    return jobs


def _synthetic_jobs() -> List[Dict[str, int]]:
    return [{"input_tokens": 50 + (i * 3) % 50, "output_tokens": 100 + (i * 7) % 200} for i in range(49)]


def load_jobs() -> List[Dict[str, int]]:
    path = Path("/Users/michaelsigamani/Documents/DevelopmentCode/2026-winter/phase2/data/benchmark_profile_D.csv")
    if path.exists():
        return _load_raw_csv(path)
    return _synthetic_jobs()


def _item_cost(job: Dict[str, int]) -> float:
    prefill_ms = job["input_tokens"] * 1.5
    decode_ms = job["output_tokens"] * 18.0
    return (prefill_ms + decode_ms) / 1000.0


def _simulate_baseline(costs: List[float]) -> float:
    workers = [0.0, 0.0]
    for cost in costs:
        workers.sort()
        workers[0] += cost
    return max(workers)


def _simulate_failover(costs: List[float]) -> float:
    workers = [0.0, 0.0]
    for idx, cost in enumerate(costs):
        workers.sort()
        if idx == 24:
            workers[0] = max(workers) + 3.0
        workers[0] += cost
    return max(workers)


def run_benchmark() -> Dict[str, Any]:
    jobs = load_jobs()
    costs = [_item_cost(j) for j in jobs]
    baseline = _simulate_baseline(costs)
    failover = _simulate_failover(costs)
    return {
        "job_count": len(jobs),
        "total_prefill_tokens": sum(j["input_tokens"] for j in jobs),
        "total_decode_tokens": sum(j["output_tokens"] for j in jobs),
        "baseline_seconds": round(baseline, 3),
        "failover_seconds": round(failover, 3),
        "within_sla": failover <= baseline * 1.1,
    }


def main() -> None:
    result = run_benchmark()
    print(json.dumps(result, indent=2))
    if not result["within_sla"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
