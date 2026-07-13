#!/usr/bin/env python3
"""Send a single ChatCompletion request to the gateway.

Required env vars:
    ODYN_API_KEY     -- API key for the gateway (required)

Optional env vars:
    ODYN_CHAT_URL    -- Chat completion endpoint (default production gateway)
    ODYN_MODEL       -- Model name to request (default qwen2.5-7b)
    ODYN_PROMPT      -- Prompt text to send
    ODYN_PROMPT_FILE -- Prompt file path (takes precedence over ODYN_PROMPT)
"""

import json
import os
from pathlib import Path
import sys
import urllib.error
import urllib.request


ODYN_CHAT_URL = os.getenv(
    "ODYN_CHAT_URL",
    "https://zba37co3g7.execute-api.eu-central-1.amazonaws.com/prod/v1/chat/completions",
)

ODYN_API_KEY = os.getenv("ODYN_API_KEY", "")

MODEL = os.getenv("ODYN_MODEL", "qwen2.5-7b")

DEFAULT_PROMPT = "What is the capital of France? Answer in one word."

MAX_TOKENS = 5


def _read_prompt_file(path: str) -> str:
    prompt = Path(path).expanduser().read_text(encoding="utf-8").strip()
    if prompt:
        return prompt
    raise RuntimeError(f"Prompt file is empty: {path}")


def _resolve_prompt() -> str:
    prompt_file = os.getenv("ODYN_PROMPT_FILE")
    if prompt_file:
        return _read_prompt_file(prompt_file)
    return os.getenv("ODYN_PROMPT", DEFAULT_PROMPT)


def _safe_print(message: str) -> None:
    data = message + "\n"
    try:
        sys.stdout.write(data)
    except UnicodeEncodeError:
        sys.stdout.buffer.write(data.encode("utf-8", errors="replace"))


def send_chat_completion(prompt: str) -> dict:
    if not ODYN_API_KEY:
        raise RuntimeError("Missing ODYN_API_KEY environment variable.")

    payload = {
        "model": MODEL,
        "messages": [
            {
                "role": "user",
                "content": prompt,
            }
        ],
        "max_tokens": MAX_TOKENS,
    }

    headers = {
        "Content-Type": "application/json",
        "x-api-key": ODYN_API_KEY,
    }

    request = urllib.request.Request(
        ODYN_CHAT_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> None:
    try:
        prompt = _resolve_prompt()
        _safe_print(f"Sending chat completion request to {ODYN_CHAT_URL}...")
        result = send_chat_completion(prompt)
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

    answer = result["choices"][0]["message"]["content"]

    _safe_print("\nResponse:")
    _safe_print(f"  Model:  {result.get('model', MODEL)}")
    _safe_print(f"  {prompt}")
    _safe_print(f"  Answer: {answer}")


if __name__ == "__main__":
    main()
