# Cross-OEM Endpoint Steering Guide

This directory manages the deployment, orchestration, and E2E validation of the heterogeneous AMD (Radeon) and NVIDIA (DGX Spark) Ray Serve cluster.

## Layout

- `src/` - Core Python code and SOLID orchestration classes.
- `test/` - Python unittest-based E2E tests.

---

## 1. Deploying the Cluster

### Start Radeon GPU Worker (Docker)
```bash
../.venv/bin/python src/main.py start-radeon
```

### Start DGX Spark Worker (Native)
```bash
../.venv/bin/python src/main.py start-dgx
```

### Deploy Ray Serve Application
```bash
../.venv/bin/python src/main.py deploy
```

### Fix Grafana iframe Embedding (Self-Heal)
```bash
../.venv/bin/python src/main.py fix-grafana
```

---

## 2. Running E2E & Rollback Tests

All testing logic is consolidated into standard Python `unittest` / `pytest` suites located in the `test/` folder. Use the active virtual environment's python to run them.

### Run All Tests
```bash
../.venv/bin/python -m unittest discover -s test
# or
pytest
```

### Run Selected Test
```bash
pytest -k "test_happy_path"
```

---

## 3. Simulating Failover Recovery
Verify that the cluster dynamically scales and survives worker nodes going offline:
```bash
RAY_HEAD_ADDRESS=127.0.0.1 ../.venv/bin/python src/main.py failover
```
