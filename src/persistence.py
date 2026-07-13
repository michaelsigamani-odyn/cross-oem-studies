import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class ChatCompletionRecord:
    request_id: str
    timestamp: int
    model_name: str
    input_messages: List[Dict[str, Any]]
    output_response: Dict[str, Any]
    latency_ms: float
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    metadata: Dict[str, str] = field(default_factory=dict)


def _to_plain(obj: Any) -> Any:
    if hasattr(obj, "__dict__"): return _to_plain(obj.__dict__)
    if hasattr(obj, "tolist"): return _to_plain(obj.tolist())
    if isinstance(obj, dict): return {k: _to_plain(v) for k, v in obj.items()}
    if isinstance(obj, list): return [_to_plain(x) for x in obj]
    return obj


def _build_s3_key(rec: ChatCompletionRecord) -> str:
    if "ray_job_id" in rec.metadata:
        jid = rec.metadata["ray_job_id"].replace("raysubmit_", "ray_job_")
        return f"outputs/offline/{jid}/{rec.request_id}.json"
    return f"outputs/online/{rec.request_id}.json"


def _s3_upload(rec: ChatCompletionRecord) -> None:
    import boto3

    s3 = boto3.client("s3")
    data = json.dumps(_to_plain(rec), indent=2)
    s3.put_object(Bucket="odyn-oem-tests", Key=_build_s3_key(rec), Body=data)


async def save(rec: ChatCompletionRecord) -> None:
    await asyncio.to_thread(_s3_upload, rec)


def _s3_upload_logs(log_text: str, filename: str) -> None:
    import boto3

    s3 = boto3.client("s3")
    s3.put_object(Bucket="odyn-oem-tests", Key=f"logs/{filename}", Body=log_text)


async def ship_logs(log_text: str, filename: str) -> None:
    await asyncio.to_thread(_s3_upload_logs, log_text, filename)
