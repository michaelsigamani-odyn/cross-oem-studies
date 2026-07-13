#!/usr/bin/env python3
from dataclasses import dataclass
import os
import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
import requests

def _safe_print(message: str) -> None:
    data = message + "\n"
    try:
        sys.stdout.write(data)
    except UnicodeEncodeError:
        sys.stdout.buffer.write(data.encode("utf-8", errors="replace"))


@dataclass
class JobConfig:
    token: str
    base_url: str

@dataclass
class RayJobRequest:
    entrypoint: str
    runtime_env: Dict[str, Any]

@dataclass
class RayJobResponse:
    submission_id: str

def _validate_token(token: Optional[str]) -> str:
    if not token:
        raise ValueError("Missing RAY_DASHBOARD_TOKEN")
    return token

def _get_token() -> str:
    token = os.getenv("RAY_DASHBOARD_TOKEN") or os.getenv("ODYN_API_KEY")
    return _validate_token(token)

def _get_base_url(args: argparse.Namespace) -> str:
    if args.base_url:
        return args.base_url
    url = os.getenv("RAY_DASHBOARD_URL") or os.getenv("ODYN_BASE_URL")
    return url if url else "https://zba37co3g7.execute-api.eu-central-1.amazonaws.com/prod/v1"

def _add_job_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--type", choices=["chat_completion", "preprocess"], default="chat_completion")
    p.add_argument("--input", default="/tmp/batch_input_batch_b880814dc56c.json")
    p.add_argument("--output", default="/tmp/test_ray_output.json")

def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    _add_job_args(p)
    p.add_argument("--base-url", default="")
    return p

def _parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    return _parser().parse_known_args(args)[0]

def _build_headers(token: str, base_url: str) -> Dict[str, str]:
    if "/v1" in base_url:
        return {
            "x-api-key": token,
            "Content-Type": "application/json"
        }
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

def build_url(base_url: str) -> str:
    cleaned = base_url.rstrip("/")
    if "/v1" in cleaned:
        return f"{cleaned}/jobs"
    return f"{cleaned}/api/jobs/"

def _send_http(method: str, url: str, headers: Dict[str, str], data: str) -> requests.Response:
    return requests.request(method, url, headers=headers, data=data, timeout=60)

def _handle_http_error(url: str, exc: Exception) -> None:
    if isinstance(exc, requests.exceptions.Timeout):
        raise TimeoutError(f"Connection timeout to {url}") from exc
    raise ConnectionError(f"Connection refused to {url}") from exc

def make_request(url: str, method: str, headers: Dict[str, str], data: str) -> requests.Response:
    try:
        return _send_http(method, url, headers, data)
    except Exception as e:
        _handle_http_error(url, e)

def _raise_403() -> None:
    raise PermissionError("Auth failed — check RAY_DASHBOARD_TOKEN")

def _raise_non_2xx(r: requests.Response) -> None:
    _safe_print(f"Error {r.status_code}: {r.text}")
    raise RuntimeError(f"HTTP {r.status_code}: {r.text}")

def check_response(r: requests.Response) -> None:
    if r.status_code == 403:
        _raise_403()
    if not r.ok:
        _raise_non_2xx(r)

def parse_response(data: Dict[str, Any]) -> RayJobResponse:
    sub_id = data.get("submission_id", "")
    return RayJobResponse(submission_id=sub_id)

def _prepare_payload(request: RayJobRequest) -> str:
    payload = {"entrypoint": request.entrypoint, "runtime_env": request.runtime_env}
    return json.dumps(payload)

def _parse_json(resp: requests.Response, *, allow_text: bool = False) -> Dict[str, Any]:
    try:
        return resp.json()
    except ValueError:
        _safe_print(f"Unexpected non-JSON response: {resp.text}")
        if allow_text:
            return {"logs": resp.text}
        raise RuntimeError("Gateway returned non-JSON payload") from None


def submit_job(config: JobConfig, request: RayJobRequest) -> RayJobResponse:
    url = build_url(config.base_url)
    resp = make_request(url, "POST", _build_headers(config.token, config.base_url), _prepare_payload(request))
    check_response(resp)
    return parse_response(_parse_json(resp))

def _build_entry(args: argparse.Namespace) -> str:
    ensure = (
        "python3 -c \"import json; from pathlib import Path; "
        f"Path('{args.input}').write_text(json.dumps([{{'custom_id':'ray-job-1','request':{{'model':'qwen2.5-7b','messages':[{{'role':'user','content':'hello'}}],'max_tokens':32}}}}]))\""
    )
    run = f"python3 /home/ubuntu/batch_job.py --input {args.input} --output {args.output} --type {args.type}"
    return f"{ensure} && {run}"


def _default_input_payload() -> List[Dict[str, Any]]:
    return [{"custom_id": "ray-job-1", "request": {"model": "qwen2.5-7b", "messages": [{"role": "user", "content": "hello"}], "max_tokens": 32}}]


def _ensure_input_file(path: str) -> None:
    p = Path(path)
    if p.exists():
        return
    p.write_text(json.dumps(_default_input_payload()))

def _build_job_url(base_url: str, job_id: str, suffix: str = "") -> str:
    cleaned = base_url.rstrip("/")
    end = f"/{suffix}" if suffix else ""
    if "/v1" in cleaned:
        return f"{cleaned}/jobs/{job_id}{end}"
    return f"{cleaned}/api/jobs/{job_id}{end}"

def get_job_status(config: JobConfig, job_id: str) -> str:
    url = _build_job_url(config.base_url, job_id)
    resp = make_request(url, "GET", _build_headers(config.token, config.base_url), "")
    check_response(resp)
    return _parse_json(resp).get("status", "")

def _is_terminal(status: str) -> bool:
    return status in {"SUCCEEDED", "FAILED", "STOPPED"}

def _check_and_sleep(config: JobConfig, job_id: str) -> Optional[str]:
    st = get_job_status(config, job_id)
    if _is_terminal(st): return st
    time.sleep(3)

def wait_for_job(config: JobConfig, job_id: str) -> str:
    for _ in range(15):
        st = _check_and_sleep(config, job_id)
        if st: return st
    return get_job_status(config, job_id)

def fetch_logs(config: JobConfig, job_id: str) -> str:
    url = _build_job_url(config.base_url, job_id, "logs")
    resp = make_request(url, "GET", _build_headers(config.token, config.base_url), "")
    check_response(resp)
    data = _parse_json(resp, allow_text=True)
    return data.get("logs", "")

def _log_success(cfg: JobConfig, sub_id: str) -> None:
    status = wait_for_job(cfg, sub_id)
    if status != "SUCCEEDED":
        logs = fetch_logs(cfg, sub_id)
        raise RuntimeError(f"Job {sub_id} ended in {status}\n{logs}")
    _safe_print(f"SUCCEEDED: {sub_id}")
    _safe_print(f"Job logs:\n{fetch_logs(cfg, sub_id)}")

def main() -> None:
    args = _parse_args()
    _ensure_input_file(args.input)
    cfg = JobConfig(token=_get_token(), base_url=_get_base_url(args))
    req = RayJobRequest(entrypoint=_build_entry(args), runtime_env={"pip": ["pandas", "pyarrow"]})
    _log_success(cfg, submit_job(cfg, req).submission_id)

if __name__ == "__main__":
    main()
