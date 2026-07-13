#!/usr/bin/env python3
"""Submit an offline batch ChatCompletion job to the production gateway.

Required env vars:
    ODYN_API_KEY   -- API key (required)

Optional env vars:
    ODYN_BASE_URL  -- Base API URL (default production gateway)
    ODYN_MODEL     -- Model to request (default qwen2.5-7b)
"""
import json
import io
import os
import sys
import time
import urllib.error
import urllib.request


ODYN_BASE_URL = os.getenv(
    "ODYN_BASE_URL",
    "https://zba37co3g7.execute-api.eu-central-1.amazonaws.com/prod/v1",
)

ODYN_API_KEY = os.getenv("ODYN_API_KEY")

MODEL = os.getenv("ODYN_MODEL", "qwen2.5-7b")

POLL_INTERVAL_SECONDS = 4
MAX_POLL_ATTEMPTS = 15


BATCH_PAYLOAD = [
    {
        "custom_id": "q1",
        "request": {
            "model": MODEL,
            "messages": [
                {
                    "role": "user",
                    "content": "What is 2 + 2?",
                }
            ],
            "max_tokens": 5,
        },
    },
    {
        "custom_id": "q2",
        "request": {
            "model": MODEL,
            "messages": [
                {
                    "role": "user",
                    "content": "What is 5 * 5?",
                }
            ],
            "max_tokens": 5,
        },
    },
]


def _configure_stdout_utf8() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
        return
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)


def _print_json(data: object) -> None:
    text = json.dumps(data, indent=2, ensure_ascii=False)
    try:
        sys.stdout.write(text + "\n")
    except UnicodeEncodeError:
        sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))


def _safe_print(message: str) -> None:
    data = message + "\n"
    try:
        sys.stdout.write(data)
        sys.stdout.flush()
    except UnicodeEncodeError:
        sys.stdout.buffer.write(data.encode("utf-8", errors="replace"))
        sys.stdout.buffer.flush()


def api_request(path: str, method: str = "GET", payload: object | None = None) -> dict:
    if not ODYN_API_KEY:
        raise RuntimeError("Missing ODYN_API_KEY environment variable.")

    headers = {"x-api-key": ODYN_API_KEY}

    body = None

    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(
        url=f"{ODYN_BASE_URL}{path}",
        data=body,
        headers=headers,
        method=method,
    )

    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def submit_batch_job() -> str:
    job = api_request(
        path="/batch",
        method="POST",
        payload=BATCH_PAYLOAD,
    )

    job_id = job["id"]
    return job_id


def poll_batch_job(job_id: str) -> dict:
    final_info = {}

    for attempt in range(1, MAX_POLL_ATTEMPTS + 1):
        info = api_request(path=f"/batch/{job_id}")
        final_info = info

        status = info.get("status", "unknown")
        counts = info.get("request_counts", {})

        completed = counts.get("completed", 0)
        total = counts.get("total", len(BATCH_PAYLOAD))

        _safe_print(
            f"[{attempt}/{MAX_POLL_ATTEMPTS}] "
            f"Status: {status} | Completed: {completed}/{total}"
        )

        if status in {"completed", "failed", "cancelled"}:
            break

        time.sleep(POLL_INTERVAL_SECONDS)

    return final_info


def main() -> None:
    _configure_stdout_utf8()
    _safe_print(f"Submitting offline batch job via {ODYN_BASE_URL}...")

    try:
        job_id = submit_batch_job()
        _safe_print("\nBatch job submitted successfully.")
        _safe_print(f"Job ID: {job_id}")

        _safe_print("\nPolling batch job status...")
        final_info = poll_batch_job(job_id)

    except urllib.error.HTTPError as error:
        _safe_print(f"Request failed with HTTP {error.code}: {error.reason}")
        _safe_print(error.read().decode("utf-8", errors="replace"))
        sys.exit(1)

    except urllib.error.URLError as error:
        _safe_print(f"Request failed: {error.reason}")
        sys.exit(1)

    except Exception as error:
        _safe_print(f"Unexpected error: {error}")
        sys.exit(1)

    _safe_print("\nBatch job run completed.")
    _safe_print("Final response:")
    _print_json(final_info)


if __name__ == "__main__":
    main()
