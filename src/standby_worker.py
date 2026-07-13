import subprocess
import time
from pathlib import Path

from config import RuntimeConfig


class StandbyWorkerController:
    def __init__(self, cfg: RuntimeConfig) -> None:
        self.cfg = cfg

    def _ssh(self, command: str, check: bool = True) -> subprocess.CompletedProcess:
        cmd = ["ssh", "-i", self.cfg.standby_ssh_key, "-o", "IdentitiesOnly=yes", self.cfg.standby_host, command]
        return subprocess.run(cmd, check=check, capture_output=True, text=True)

    def stop(self) -> None:
        cmd = "bash -lc 'if [ -f ~/vllm-env/bin/activate ]; then source ~/vllm-env/bin/activate; fi; ray stop --force 2>/dev/null || true; pkill -f raylet 2>/dev/null || true'"
        self._ssh(cmd, check=False)

    def bootstrap(self) -> None:
        cmd = (
            "bash -lc 'set -euo pipefail; "
            "python3 -m venv ~/vllm-env; source ~/vllm-env/bin/activate; "
            "python3 -m pip install --upgrade pip; "
            "python3 -m pip install --upgrade \"ray[serve]==2.49.2\" vllm fastapi httpx requests; "
            "command -v nvidia-smi >/dev/null; nvidia-smi -L >/dev/null'"
        )
        self._ssh(cmd)
        self._sync_worker_module()

    def _sync_worker_module(self) -> None:
        local_file = Path(__file__).resolve().parent / "vllm_deployment_simple.py"
        target = f"{self.cfg.standby_host}:/home/michael/vllm_deployment_simple.py"
        cmd = ["scp", "-i", self.cfg.standby_ssh_key, "-o", "IdentitiesOnly=yes", str(local_file), target]
        subprocess.run(cmd, check=True)

    def _start_remote_cmd(self) -> str:
        patch = "if [ -f ~/vllm-env/lib/python3.12/site-packages/ray/_private/utils.py ]; then sed -i 's/python_version_match_level=\\\"patch\\\"/python_version_match_level=\\\"minor\\\"/' ~/vllm-env/lib/python3.12/site-packages/ray/_private/utils.py; fi"
        resources = '{\"STANDBY_GPU\":1.0,\"NVIDIA_GPU\":1.0,\"NODE_TYPE_DGX_SPARK\":1.0,\"ROLE_STANDBY\":1.0,\"SERVING_ENGINE_VLLM\":1.0}'
        p1 = "set -euo pipefail; source ~/vllm-env/bin/activate; export PYTHONPATH=/home/michael:${PYTHONPATH:-};"
        p2 = f" {patch} && nohup ray start --address={self.cfg.ray_gcs_address} --resources='\\''{resources}'\\'' --num-gpus=1 --node-ip-address={self.cfg.expected_standby_ip} --node-name={self.cfg.standby_node_name} --metrics-export-port=8081 --disable-usage-stats > /tmp/ray-worker-standby.log 2>&1 &"
        return f"bash -lc '{p1}{p2}'"

    def start(self) -> None:
        self.stop()
        time.sleep(2)
        self._ssh(self._start_remote_cmd())

    def _count_nodes_once(self) -> int:
        cmd = f"bash -lc 'source ~/vllm-env/bin/activate; ray status --address {self.cfg.ray_gcs_address} 2>/dev/null || true'"
        return self._ssh(cmd, check=False).stdout.count("node_")

    def wait_joined(self, max_checks: int = 20) -> None:
        for _ in range(max_checks):
            if self._count_nodes_once() >= 2:
                return
            time.sleep(5)
        raise RuntimeError("Standby worker failed to join cluster")

    def start_vllm(self) -> None:
        return

    def wait_ready(self, max_checks: int = 3) -> None:
        self.wait_joined(max_checks=max_checks)
