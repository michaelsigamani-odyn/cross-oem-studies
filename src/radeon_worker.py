import subprocess
import time
from config import RuntimeConfig


class RadeonWorkerController:
    def __init__(self, cfg: RuntimeConfig) -> None:
        self.cfg = cfg

    def stop(self) -> None:
        cmd = "docker rm -f ray-worker-radeon 2>/dev/null || true"
        subprocess.run(["ssh", self.cfg.radeon_host, cmd], check=False)

    def _ray_start_args(self) -> list[str]:
        return [
            f"--address={self.cfg.ray_gcs_address}",
            "--resources=\"$RAY_CUSTOM_RESOURCES\"",
            "--num-gpus=1",
            f"--node-ip-address={self.cfg.expected_radeon_ip}",
            "--dashboard-agent-listen-port=52375",
            "--dashboard-agent-grpc-port=65428",
            "--runtime-env-agent-port=59527",
            "--metrics-export-port=8081",
            "--block",
        ]

    def _docker_run_cmd(self) -> str:
        args = " ".join(self._ray_start_args())
        target_file = "/usr/local/lib/python3.12/dist-packages/ray/_private/utils.py"
        py_patch = f"sed -i s/python_version_match_level=\\\"patch\\\"/python_version_match_level=\\\"minor\\\"/ {target_file}"
        return (
            f"docker run -d --name ray-worker-radeon --restart unless-stopped "
            f"--network host --ipc host --entrypoint /bin/bash --device /dev/kfd:/dev/kfd --device /dev/dri:/dev/dri "
            f"--group-add 44 --group-add 992 -e HIP_VISIBLE_DEVICES=0 -e PYTORCH_ROCM_ARCH=gfx1151 "
            f"-e HSA_NO_SCRATCH_RECLAIM=1 -e VLLM_USE_TRITON_FLASH_ATTN=1 -e MODEL_NAME='{self.cfg.model_name}' "
            f"-e SERVED_MODEL_NAME='{self.cfg.model_name}' -e MAX_MODEL_LEN=8192 -e GPU_MEMORY_UTILIZATION=0.90 "
            f"-e TENSOR_PARALLEL_SIZE=1 -e VLLM_EXTRA_ARGS='--enforce-eager --dtype bfloat16' "
            f"-e HF_HOME={self.cfg.model_cache_dir} -v {self.cfg.model_cache_dir}:{self.cfg.model_cache_dir}:ro "
            f"-e RAY_CUSTOM_RESOURCES=\"$RAY_CUSTOM_RESOURCES\" "
            f"{self.cfg.radeon_image} -lc '{py_patch} && exec ray start {args}'"
        )

    def start(self) -> None:
        self.stop()
        cmd = f"export RAY_CUSTOM_RESOURCES='{{\"AMD_GPU\":1.0,\"SERVING_ENGINE_VLLM\":1.0}}' && {self._docker_run_cmd()}"
        subprocess.run(["ssh", self.cfg.radeon_host, cmd], check=True)

    def _check_logs_once(self) -> bool:
        cmd = "docker logs ray-worker-radeon 2>&1"
        res = subprocess.run(["ssh", self.cfg.radeon_host, cmd], capture_output=True, text=True)
        return "Ray runtime started" in res.stdout

    def wait_joined(self, max_checks: int = 20) -> None:
        for _ in range(max_checks):
            if self._check_logs_once():
                return
            time.sleep(5)
        raise RuntimeError("Radeon worker failed to join cluster")
