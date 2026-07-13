# cross-oem/Makefile — heterogeneous AMD+NVIDIA Ray Serve cluster
# Requires: RAY_HEAD_ADDRESS, HF_TOKEN
# Optional: MODEL_NAME, MODEL_CACHE_DIR

.PHONY: build-radeon push-radeon build-dgx push-dgx build-rtx5090 push-rtx5090 \
        start-radeon start-dgx start-rtx5090 start-all deploy status failover-test check e2e safe-refactor

IMAGE_RADEON  := michaelsigamaniodyn/runtime-vllm-radeon:rocm721-gfx1151
IMAGE_DGX     := michaelsigamaniodyn/runtime-vllm-dgx-spark:ray249
IMAGE_RTX5090 := michaelsigamaniodyn/runtime-vllm-rtx5090:latest
REPO_ROOT     := $(shell git rev-parse --show-toplevel)

# ── Build ─────────────────────────────────────────────────────────────────────
build-radeon:
	docker build \
	  -f cross-oem/radeon/Dockerfile \
	  -t $(IMAGE_RADEON) \
	  --build-arg BUILDKIT_INLINE_CACHE=1 \
	  $(REPO_ROOT)

build-dgx:
	docker build \
	  -f cross-oem/dgx-spark/Dockerfile \
	  -t $(IMAGE_DGX) \
	  --build-arg BUILDKIT_INLINE_CACHE=1 \
	  $(REPO_ROOT)

build-rtx5090:
	docker build \
	  -f cross-oem/rtx-5090/Dockerfile \
	  -t $(IMAGE_RTX5090) \
	  --build-arg BUILDKIT_INLINE_CACHE=1 \
	  $(REPO_ROOT)

# ── Push ──────────────────────────────────────────────────────────────────────
push-radeon: build-radeon
	docker push $(IMAGE_RADEON)

push-dgx: build-dgx
	docker push $(IMAGE_DGX)

push-rtx5090: build-rtx5090
	docker push $(IMAGE_RTX5090)

# ── Start workers ─────────────────────────────────────────────────────────────
start-radeon:
	PYTHONPATH=src python3 -c "import sys,main; sys.argv=['','start-radeon']; main.main()"

start-dgx:
	PYTHONPATH=src python3 -c "import sys,main; sys.argv=['','start-dgx']; main.main()"

start-rtx5090:
	PYTHONPATH=src python3 -c "import sys,main; sys.argv=['','start-rtx5090']; main.main()"

start-all: start-radeon start-dgx start-rtx5090

# ── Deploy Ray Serve ──────────────────────────────────────────────────────────
deploy:
	PYTHONPATH=src python3 -c "import sys,main; sys.argv=['','deploy']; main.main()"

# ── Cluster status ────────────────────────────────────────────────────────────
status:
	RAY_ADDRESS="http://$$(RAY_HEAD_ADDRESS):8265" ray status || true
	RAY_ADDRESS="http://$$(RAY_HEAD_ADDRESS):8265" serve status || true

# ── Failover demo ─────────────────────────────────────────────────────────────
failover-test:
	PYTHONPATH=src python3 -c "import sys,main; sys.argv=['','failover']; main.main()" || true

check:
	python3 -m pytest test/test_cluster.py -q --tb=line -k "no_hardcoded"

e2e:
	python3 -m pytest test/test_workflow_e2e.py -q --tb=line

safe-refactor:
	python3 -m pytest test/test_workflow_e2e.py -q --tb=line -k "refactor"
