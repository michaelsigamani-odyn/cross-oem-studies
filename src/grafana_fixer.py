import os
import subprocess
import time
import urllib.parse
from config import RuntimeConfig
from dgx_worker import DGXWorkerController
from radeon_worker import RadeonWorkerController
from serve_ops import run_deploy, wait_healthy


class GrafanaFixer:
    def __init__(self, cfg: RuntimeConfig) -> None:
        self.cfg = cfg

    def _determine_iframe_host(self) -> str:
        u = urllib.parse.urlparse(self.cfg.ray_dashboard_url)
        return f"{u.scheme}://{u.hostname}/grafana"

    def _ssh_args(self) -> list[str]:
        return ["ssh", "-i", self.cfg.aws_ssh_key, "-o", "IdentitiesOnly=yes", self.cfg.aws_ssh_host]

    def _restart_head_cmd(self, iframe_host: str) -> str:
        r1 = "set -euo pipefail; ray stop --force || true;"
        r2 = f" RAY_GRAFANA_HOST='http://localhost:3000' RAY_PROMETHEUS_HOST='http://localhost:9090'"
        r3 = f" RAY_GRAFANA_IFRAME_HOST='{iframe_host}' RAY_GRAFANA_ORG_ID='1' RAY_PROMETHEUS_NAME='Prometheus'"
        r4 = f" python3.12 -m ray.scripts.scripts start --head --node-ip-address={self.cfg.expected_head_ip}"
        r5 = f" --port=6379 --dashboard-host='0.0.0.0' --dashboard-port={self.cfg.ray_head_dashboard_port}"
        r6 = f" --metrics-export-port={self.cfg.ray_head_metrics_port} --disable-usage-stats"
        return f"bash -s <<'REMOTE'\n{r1}{r2}{r3}{r4}{r5}{r6}\nREMOTE"

    def _restart_workers(self) -> None:
        rad = RadeonWorkerController(self.cfg)
        dgx = DGXWorkerController(self.cfg)
        rad.start()
        dgx.start()
        rad.wait_joined()
        dgx.wait_joined()

    def fix(self) -> None:
        iframe = self._determine_iframe_host()
        subprocess.run(self._ssh_args() + [self._restart_head_cmd(iframe)], check=True)
        self._restart_workers()
        run_deploy(self.cfg)
        wait_healthy(self.cfg)
