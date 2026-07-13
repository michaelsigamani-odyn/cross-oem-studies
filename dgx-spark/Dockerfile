FROM vllm/vllm-openai:cu130-nightly

ENV HF_HOME=/opt/odyn/models

RUN pip install --no-cache-dir \
    "ray[serve,default]==2.49.2" \
    "fastapi==0.138.1" \
    "prometheus-fastapi-instrumentator==6.1.0" \
    huggingface-hub \
    httpx \
    "redis[asyncio]" \
    uvicorn

RUN sed -i 's/python_version_match_level="patch"/python_version_match_level="minor"/' \
    /usr/local/lib/python3.12/dist-packages/ray/_private/utils.py

# Patch prometheus-fastapi-instrumentator route.path to avoid _IncludedRouter AttributeError
RUN python3 -c "import pathlib; p = pathlib.Path('/usr/local/lib/python3.12/dist-packages/prometheus_fastapi_instrumentator/routing.py'); p.write_text(p.read_text().replace('route.path', 'getattr(route, \"path\", None) or \"unknown\"'))"

# Patch Ray Serve get_asgi_route_name.py route.path to avoid _IncludedRouter AttributeError/noise
RUN python3 -c "import pathlib; p = pathlib.Path('/usr/local/lib/python3.12/dist-packages/ray/serve/_private/thirdparty/get_asgi_route_name.py'); p.write_text(p.read_text().replace('route.path', 'getattr(route, \"path\", None) or \"unknown\"'))"
RUN mkdir -p /opt/odyn/models

WORKDIR /app
COPY ray-head/src/ /app/src/
RUN python3 -c "import site; open(site.getsitepackages()[0]+'/ray-serve-src.pth','w').write('/app/src\n')"

EXPOSE 8001

CMD ["ray", "start", "--block"]
