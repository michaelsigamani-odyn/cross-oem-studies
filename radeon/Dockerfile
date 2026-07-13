# Radeon Ray worker — ROCm 7.2.4 + Python 3.12.3 + vLLM 0.20.2rc1 ROCm nightly
# Targets AMD Ryzen AI MAX+ 395 / Radeon 8060S (gfx1151).
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive \
    ROCM_VERSION=7.2.4 \
    # Exact commit that produced the nightly wheels used on the Radeon host
    VLLM_ROCM_COMMIT=321fa2d6d1644629ac39d173f6393f37e14bf7b4 \
    # gfx1151 = AMD Radeon 8060S (Ryzen AI MAX+ 395 iGPU)
    PYTORCH_ROCM_ARCH=gfx1151 \
    # Required for ROCm 7.x stability on this APU
    HSA_NO_SCRATCH_RECLAIM=1 \
    VLLM_USE_TRITON_FLASH_ATTN=1 \
    ROCM_PATH=/opt/rocm \
    HF_HOME=/home/michael/.cache/huggingface

# Append to PATH/LD_LIBRARY_PATH safely (avoids "undefined variable" warning)
ENV PATH="/opt/rocm/bin:${PATH}" \
    LD_LIBRARY_PATH="/opt/rocm/lib"

# ── System packages ──────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        wget gnupg2 ca-certificates curl zip \
        python3 python3-pip python3-dev \
        build-essential git \
        libopenmpi-dev \
        libdw1 \
    && rm -rf /var/lib/apt/lists/*

# ── ROCm 7.2.4 apt repository ────────────────────────────────────────────────
RUN mkdir -p /etc/apt/keyrings \
    && wget -qO - https://repo.radeon.com/rocm/rocm.gpg.key \
       | gpg --dearmor > /etc/apt/keyrings/rocm.gpg \
    && echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/rocm.gpg] \
https://repo.radeon.com/rocm/apt/${ROCM_VERSION} noble main" \
       > /etc/apt/sources.list.d/rocm.list

# ── ROCm ML userspace libraries (exact versions from Radeon host) ─────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        "hipblas=3.2.0.70204-93~24.04" \
        "hipblaslt=1.2.2.70204-93~24.04" \
        "hipfft=1.0.22.70204-93~24.04" \
        "hiprand=3.1.0.70204-93~24.04" \
        "hipsolver=3.2.0.70204-93~24.04" \
        "hipsparse=4.2.0.70204-93~24.04" \
        "hipsparselt=0.2.6.70204-93~24.04" \
        "miopen-hip=3.5.1.70204-93~24.04" \
        "rocblas=5.2.0.70204-93~24.04" \
        "rocprofiler-sdk=1.1.0-93~24.04" \
        "rccl=2.27.7.70204-93~24.04" \
        "hsa-amd-aqlprofile=1.0.0.70204-93~24.04" \
    && rm -rf /var/lib/apt/lists/*

# ── Custom ROCm-only wheels (not on PyPI) — must install before vLLM ─────────
RUN pip install --no-cache-dir --break-system-packages \
    "https://wheels.vllm.ai/rocm/${VLLM_ROCM_COMMIT}/triton_kernels-1.0.0-py3-none-any.whl" \
    "https://wheels.vllm.ai/rocm/${VLLM_ROCM_COMMIT}/amd_aiter-0.1.10.post3-cp312-cp312-manylinux_2_34_x86_64.whl" \
    "https://wheels.vllm.ai/rocm/${VLLM_ROCM_COMMIT}/amdsmi-26.2.2%2Be1a6bc5663-py3-none-any.whl"

# ── PyTorch ROCm nightly (from wheels.vllm.ai — exact hashes from host pip) ──
RUN pip install --no-cache-dir --break-system-packages \
    "https://wheels.vllm.ai/rocm/${VLLM_ROCM_COMMIT}/triton-3.6.0-cp312-cp312-manylinux_2_35_x86_64.whl" \
    "https://wheels.vllm.ai/rocm/${VLLM_ROCM_COMMIT}/torch-2.10.0%2Bgit8514f05-cp312-cp312-manylinux_2_35_x86_64.whl" \
    "https://wheels.vllm.ai/rocm/${VLLM_ROCM_COMMIT}/torchaudio-2.9.0%2Beaa9e4e-cp312-cp312-manylinux_2_34_x86_64.whl" \
    "https://wheels.vllm.ai/rocm/${VLLM_ROCM_COMMIT}/torchvision-0.24.1%2Bd801a34-cp312-cp312-manylinux_2_34_x86_64.whl"

# ── flash-attn + vLLM ROCm nightly ───────────────────────────────────────────
RUN pip install --no-cache-dir --break-system-packages \
    "https://wheels.vllm.ai/rocm/${VLLM_ROCM_COMMIT}/flash_attn-2.8.3-cp312-cp312-manylinux_2_34_x86_64.whl" \
    "https://wheels.vllm.ai/rocm/${VLLM_ROCM_COMMIT}/vllm-0.20.2rc1.dev15%2Bg321fa2d6d.rocm721-cp312-cp312-manylinux_2_34_x86_64.whl"

# ── Ray Serve — must match head node version ──────────────────────────────────
# pydantic>=2.12 required: vllm 0.20.2rc1 uses FieldInfo.exclude_if (added in 2.11)
RUN pip install --no-cache-dir --break-system-packages \
    "ray[serve,default]==2.49.2" \
    "pydantic>=2.12.0" \
    huggingface-hub \
    httpx \
    "redis[asyncio]" \
    "fastapi==0.138.1" \
    "prometheus-fastapi-instrumentator==6.1.0" \
    uvicorn

# Patch prometheus-fastapi-instrumentator route.path to avoid _IncludedRouter AttributeError
RUN python3 -c "import pathlib; p = pathlib.Path('/usr/local/lib/python3.12/dist-packages/prometheus_fastapi_instrumentator/routing.py'); p.write_text(p.read_text().replace('route.path', 'getattr(route, \"path\", None) or \"unknown\"'))"

# Patch Ray Serve get_asgi_route_name.py route.path to avoid _IncludedRouter AttributeError/noise
RUN python3 -c "import pathlib; p = pathlib.Path('/usr/local/lib/python3.12/dist-packages/ray/serve/_private/thirdparty/get_asgi_route_name.py'); p.write_text(p.read_text().replace('route.path', 'getattr(route, \"path\", None) or \"unknown\"'))"

# ── Deployment source ─────────────────────────────────────────────────────────
WORKDIR /app
COPY ray-head/src/ /app/src/
# Make vllm_deployment importable without runtime_env working_dir
RUN python3 -c "import site; open(site.getsitepackages()[0]+'/ray-serve-src.pth','w').write('/app/src\n')"

EXPOSE 8001

CMD ["ray", "start", "--block"]
