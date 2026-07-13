import subprocess
import time
from config import RuntimeConfig


class DGXWorkerController:
    def __init__(self, cfg: RuntimeConfig) -> None:
        self.cfg = cfg

    def stop(self) -> None:
        cmd = "source ~/vllm-env/bin/activate && ray stop --force 2>/dev/null || true"
        subprocess.run(["ssh", self.cfg.dgx_host, cmd], check=False)

    def _start_remote_cmd(self) -> str:
        target_file = "~/vllm-env/lib/python3.12/site-packages/ray/_private/utils.py"
        py_patch = f"sed -i s/python_version_match_level=\\\"patch\\\"/python_version_match_level=\\\"minor\\\"/ {target_file}"
        r1 = "set -euo pipefail; source ~/vllm-env/bin/activate;"
        r2 = f" {py_patch} && nohup ray start --address={self.cfg.ray_gcs_address}"
        r3 = f" --resources=\"$RAY_CUSTOM_RESOURCES\" --num-gpus=1 --node-ip-address={self.cfg.expected_dgx_ip}"
        r4 = " --metrics-export-port=8081 --disable-usage-stats > /tmp/ray-worker-dgx.log 2>&1 &"
        return f"bash -lc '{r1}{r2}{r3}{r4}'"

    def start(self) -> None:
        self.stop()
        time.sleep(2)
        cmd = f"export RAY_CUSTOM_RESOURCES='{{\"NVIDIA_GPU\":1.0,\"SERVING_ENGINE_VLLM\":1.0}}' && {self._start_remote_cmd()}"
        subprocess.run(["ssh", self.cfg.dgx_host, cmd], check=True)

    def _count_nodes_once(self) -> int:
        cmd = f"source ~/vllm-env/bin/activate && ray status --address {self.cfg.ray_gcs_address} 2>/dev/null"
        res = subprocess.run(["ssh", self.cfg.dgx_host, cmd], capture_output=True, text=True)
        return res.stdout.count("node_")

    def wait_joined(self, max_checks: int = 20) -> None:
        for _ in range(max_checks):
            if self._count_nodes_once() >= 2:
                return
            time.sleep(5)
        raise RuntimeError("DGX worker failed to join cluster")
