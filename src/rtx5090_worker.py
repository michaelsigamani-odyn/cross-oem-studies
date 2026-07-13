import subprocess
import time
from config import RuntimeConfig


class RTX5090WorkerController:
    """Starts/stops the NVIDIA RTX 5090 Ray worker container over SSH.

    Mirrors RadeonWorkerController but targets the CUDA runtime image
    (see rtx-5090/Dockerfile) and advertises NVIDIA_GPU capacity.
    """

    CONTAINER = "ray-worker-rtx5090"

    def __init__(self, cfg: RuntimeConfig) -> None:
        self.cfg = cfg

    def stop(self) -> None:
        cmd = f"docker rm -f {self.CONTAINER} 2>/dev/null || true"
        subprocess.run(["ssh", self.cfg.rtx5090_host, cmd], check=False)

    def _ray_start_args(self) -> list[str]:
        return [
            f"--address={self.cfg.ray_gcs_address}",
            "--resources=\"$RAY_CUSTOM_RESOURCES\"",
            "--num-gpus=1",
            f"--node-ip-address={self.cfg.expected_rtx5090_ip}",
            "--dashboard-agent-listen-port=52376",
            "--dashboard-agent-grpc-port=65429",
            "--runtime-env-agent-port=59528",
            "--metrics-export-port=8081",
            "--block",
        ]

    def _docker_run_cmd(self) -> str:
        args = " ".join(self._ray_start_args())
        return (
            f"docker run -d --name {self.CONTAINER} --restart unless-stopped "
            f"--network host --ipc host --gpus all --entrypoint /bin/bash "
            f"-e MODEL_NAME='{self.cfg.model_name}' -e SERVED_MODEL_NAME='{self.cfg.model_name}' "
            f"-e MAX_MODEL_LEN=4096 -e GPU_MEMORY_UTILIZATION=0.85 -e TENSOR_PARALLEL_SIZE=1 "
            f"-e VLLM_EXTRA_ARGS='--enforce-eager' "
            f"-e HF_HOME={self.cfg.model_cache_dir} -v {self.cfg.model_cache_dir}:{self.cfg.model_cache_dir}:ro "
            f"-e RAY_CUSTOM_RESOURCES=\"$RAY_CUSTOM_RESOURCES\" "
            f"{self.cfg.rtx5090_image} -lc 'exec ray start {args}'"
        )

    def start(self) -> None:
        self.stop()
        cmd = f"export RAY_CUSTOM_RESOURCES='{{\"NVIDIA_GPU\":1.0,\"SERVING_ENGINE_VLLM\":1.0}}' && {self._docker_run_cmd()}"
        subprocess.run(["ssh", self.cfg.rtx5090_host, cmd], check=True)

    def _check_logs_once(self) -> bool:
        cmd = f"docker logs {self.CONTAINER} 2>&1"
        res = subprocess.run(["ssh", self.cfg.rtx5090_host, cmd], capture_output=True, text=True)
        return "Ray runtime started" in res.stdout

    def wait_joined(self, max_checks: int = 20) -> None:
        for _ in range(max_checks):
            if self._check_logs_once():
                return
            time.sleep(5)
        raise RuntimeError("RTX 5090 worker failed to join cluster")
