FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl wget git gcc g++ make && \
    ln -sf /usr/bin/gcc /usr/local/bin/gcc && \
    ln -sf /usr/bin/g++ /usr/local/bin/g++ && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir vllm \
    --extra-index-url https://download.pytorch.org/whl/cu128

ENV LD_LIBRARY_PATH=/usr/local/lib/python3.12/site-packages/nvidia/cu13/lib
ENV VLLM_USE_FLASHINFER_SAMPLER=0
ENV CC=/usr/bin/gcc
ENV CXX=/usr/bin/g++
ENV PATH=/usr/bin:/usr/local/bin:$PATH

EXPOSE 9000

ENTRYPOINT ["python3", "-m", "vllm.entrypoints.openai.api_server"]
CMD ["--model", "Qwen/Qwen2.5-7B-Instruct", \
     "--max-model-len", "4096", \
     "--gpu-memory-utilization", "0.85", \
     "--tensor-parallel-size", "4", \
     "--enforce-eager", \
     "--host", "0.0.0.0", \
     "--port", "9000", \
     "--served-model-name", "qwen2.5-7b"]
