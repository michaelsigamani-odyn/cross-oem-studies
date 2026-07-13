from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class MetricDatum:
    name: str
    value: float
    unit: str = "None"

    def to_dict(self) -> Dict[str, Any]:
        return {"MetricName": self.name, "Value": self.value, "Unit": self.unit}


def _cw_client() -> Any:
    import boto3
    return boto3.client("cloudwatch", region_name="eu-central-1")


def _log_client() -> Any:
    import boto3
    return boto3.client("logs", region_name="eu-central-1")


def _put_metrics(data: List[Dict[str, Any]]) -> None:
    try:
        _cw_client().put_metric_data(Namespace="odyn-cross-oem", MetricData=data)
    except Exception:
        pass


def _write_log(group: str, stream: str, msg: str) -> None:
    try:
        ts = int(time.time() * 1000)
        _log_client().put_log_events(logGroupName=group, logStreamName=stream, logEvents=[{"timestamp": ts, "message": msg}])
    except Exception:
        pass


def emit_metrics(items: List[MetricDatum]) -> None:
    _put_metrics([item.to_dict() for item in items])


def emit_structured_log(group: str, stream: str, data: Dict[str, Any]) -> None:
    _write_log(group, stream, json.dumps(data))


def emit_gateway_metrics(status: int, latency_ms: float) -> None:
    ok = 1.0 if status in {200, 201, 302} else 0.0
    data = [MetricDatum("request_count", 1.0), MetricDatum("availability", ok), MetricDatum("latency_ms", latency_ms, "Milliseconds")]
    emit_metrics(data)


def emit_batch_metrics(state: str) -> None:
    ok = 1.0 if state == "COMPLETED" else 0.0
    data = [MetricDatum("batch_job_count", 1.0), MetricDatum("batch_reliability", ok)]
    emit_metrics(data)
