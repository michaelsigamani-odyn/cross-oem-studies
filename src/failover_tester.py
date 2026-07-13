import subprocess
import time
from config import RuntimeConfig
from serve_ops import _request_once


class FailoverTester:
    def __init__(self, cfg: RuntimeConfig) -> None:
        self.cfg = cfg

    def _chat(self, label: str) -> None:
        try:
            code = _request_once(self.cfg)
            print(f"  [{label}] HTTP {code}")
        except Exception as err:
            print(f"  [{label}] ERROR: {err}")

    def _is_dgx_active(self) -> bool:
        cmd = "docker inspect ray-worker-dgx"
        res = subprocess.run(["ssh", self.cfg.dgx_host, cmd], capture_output=True)
        return res.returncode == 0

    def _toggle_dgx(self, action: str) -> None:
        cmd = f"docker {action} ray-worker-dgx"
        subprocess.run(["ssh", self.cfg.dgx_host, cmd], check=True)

    def _kill_radeon_vllm(self) -> None:
        cmd = "pgrep -f 'vllm.entrypoints.openai.api_server' | xargs kill"
        subprocess.run(["ssh", self.cfg.radeon_host, cmd], check=False)

    def _run_dgx_failover(self) -> None:
        self._toggle_dgx("stop")
        time.sleep(35)
        for i in range(3):
            self._chat(f"after-dgx-stop-{i}")
            time.sleep(2)
        self._toggle_dgx("start")

    def _run_single_node_failover(self) -> None:
        self._kill_radeon_vllm()
        for i in range(8):
            self._chat(f"recovery-{i}")
            time.sleep(8)

    def run(self) -> None:
        self._chat("before-1")
        self._chat("before-2")
        if self._is_dgx_active():
            self._run_dgx_failover()
        else:
            self._run_single_node_failover()
        self._chat("final-check")
