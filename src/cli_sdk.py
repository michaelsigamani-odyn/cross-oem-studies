from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, Optional

import typer

app = typer.Typer(help="Odyn Cross-OEM Developer SDK CLI")


def _run_sub(script: str, env_vars: Dict[str, str]) -> None:
    import os
    import subprocess

    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / script
    subprocess.run([sys.executable, str(script_path)], env={**os.environ, **env_vars}, check=True)


def _build_env(
    api_key: Optional[str],
    chat_url: Optional[str] = None,
    base_url: Optional[str] = None,
    model: Optional[str] = None,
    token: Optional[str] = None,
    prompt: Optional[str] = None,
    prompt_file: Optional[str] = None,
) -> Dict[str, str]:
    data = {
        "ODYN_API_KEY": api_key,
        "ODYN_CHAT_URL": chat_url,
        "ODYN_BASE_URL": base_url,
        "ODYN_MODEL": model,
        "RAY_DASHBOARD_TOKEN": token,
        "ODYN_PROMPT": prompt,
        "ODYN_PROMPT_FILE": prompt_file,
    }
    return {k: v for k, v in data.items() if v is not None}


@app.command(help="Submit online real-time chat completions request")
def chat(
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", help="Odyn API Gateway Key"),
    chat_url: Optional[str] = typer.Option(None, "--chat-url", "-u", help="Inference Endpoint URL"),
    model: Optional[str] = typer.Option(None, "--model", "-m", help="Target model identifier"),
    prompt: Optional[str] = typer.Option(None, "--prompt", "-p", help="Prompt text to send"),
    prompt_file: Optional[str] = typer.Option(None, "--prompt-file", "-f", help="Prompt file path (for example user-workloads/chat_prompt.txt)"),
) -> None:
    env = _build_env(api_key, chat_url=chat_url, model=model, prompt=prompt, prompt_file=prompt_file)
    _run_sub("submit_chat_completions.py", env)


@app.command(help="Submit offline batch chat completions request")
def batch(
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", help="Odyn API Gateway Key"),
    base_url: Optional[str] = typer.Option(None, "--base-url", "-b", help="Base API Gateway URL"),
    model: Optional[str] = typer.Option(None, "--model", "-m", help="Target model identifier"),
) -> None:
    env = _build_env(api_key, base_url=base_url, model=model)
    _run_sub("submit_chat_completions_offline.py", env)


@app.command(help="Submit parallel data job directly to Ray cluster")
def ray_job(
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", help="Odyn API Gateway Key"),
    token: Optional[str] = typer.Option(None, "--token", "-t", help="Ray dashboard auth token"),
    base_url: Optional[str] = typer.Option(None, "--base-url", "-b", help="Base API Gateway URL"),
) -> None:
    env = _build_env(api_key, base_url=base_url, token=token or api_key)
    _run_sub("submit_ray_job.py", env)


if __name__ == "__main__":
    app()
