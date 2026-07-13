import sys
from typing import Any


def _cw_client() -> Any:
    import boto3
    return boto3.client("cloudwatch", region_name="eu-central-1")


def _put_alarm(name: str, metric: str, threshold: float, period: int, comparison: str) -> None:
    try:
        _cw_client().put_metric_alarm(AlarmName=name, MetricName=metric, Namespace="odyn-cross-oem", Statistic="Average", Period=period, EvaluationPeriods=1, Threshold=threshold, ComparisonOperator=comparison)
    except Exception as e:
        print(f"Skipped alarm creation for {name}: {e}")


def create_availability_alarm() -> None:
    _put_alarm("GatewayAvailabilityLow", "availability", 0.999, 300, "LessThanThreshold")


def create_latency_alarm() -> None:
    _put_alarm("GatewayLatencyHigh", "latency_ms", 4500.0, 300, "GreaterThanThreshold")


def create_batch_alarm() -> None:
    _put_alarm("BatchReliabilityLow", "batch_reliability", 0.995, 3600, "LessThanThreshold")


def create_rto_alarm() -> None:
    _put_alarm("FailoverRTOHigh", "failover_rto_seconds", 30.0, 60, "GreaterThanThreshold")


def main() -> None:
    create_availability_alarm()
    create_latency_alarm()
    create_batch_alarm()
    create_rto_alarm()


if __name__ == "__main__":
    main()
