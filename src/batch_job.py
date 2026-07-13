import argparse
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import httpx
import ray
import ray.data

LOGGER = logging.getLogger("cross_oem.batch")


def _to_plain(obj: Any) -> Any:
    if hasattr(obj, "tolist"): return _to_plain(obj.tolist())
    if isinstance(obj, dict): return {k: _to_plain(v) for k, v in obj.items()}
    if isinstance(obj, list): return [_to_plain(x) for x in obj]
    return obj
@dataclass
class BatchDispatcher:
    infer_url: str

    def run(self, raw_item: dict) -> dict:
        item = _to_plain(raw_item)
        try:
            return {"response": self._post(self.infer_url, item["request"]), "error": None}
        except Exception as err:
            return {"response": None, "error": str(err)}

    def _post(self, url: str, payload: dict) -> dict:
        with httpx.Client(timeout=90) as client:
            res = client.post(url, json=payload)
            res.raise_for_status()
            return res.json()

class GenericPreprocessingMapper:
    def _preprocess_text(self, text: Any) -> dict:
        clean = str(text).lower()
        words = clean.split()
        return {"processed_text": clean, "tokens": words, "token_count": len(words)}

    def __call__(self, batch: dict) -> dict:
        if "text" not in batch: return batch
        results = [self._preprocess_text(t) for t in batch["text"]]
        for k in ["processed_text", "tokens", "token_count"]:
            batch[k] = [r[k] for r in results]
        return batch


def _router_adapter() -> Any:
    """Best-effort handle to the failover router actor.

    Returns None when the router (or its module) is unavailable so batch jobs
    degrade to static single-URL dispatch instead of failing outright.
    """
    name = os.getenv("ROUTER_ACTOR_NAME", "cross-oem-failover-router")
    namespace = os.getenv("ROUTER_ACTOR_NAMESPACE", "serve")
    try:
        from router_dispatch import RouterHandleAdapter
        try:
            actor = ray.get_actor(name, namespace=namespace)
        except Exception:
            actor = ray.get_actor(name)
        return RouterHandleAdapter(actor)
    except Exception as err:
        LOGGER.warning("router unavailable, using static INFER_URL: %s", err)
        return None


def _run_routed_chat_completion(items: list[dict], adapter: Any) -> list:
    """Fan chat items out across cross-OEM nodes via the failover router.

    Each item is routed independently, retried/rerouted on failure, and the
    serving node is recorded so completion results show routing decisions.
    """
    from concurrent.futures import ThreadPoolExecutor
    from router_dispatch import dispatch_batch_item

    dispatcher = BatchDispatcher(os.getenv("INFER_URL", "http://127.0.0.1:8000/infer/v1/chat/completions"))
    max_attempts = int(os.getenv("ROUTER_MAX_REQUEST_RETRIES", "3"))
    workers = max(1, int(os.getenv("BATCH_CONCURRENCY", "4")))

    def run_item(indexed: tuple[int, dict]) -> dict:
        idx, raw_item = indexed
        item = _to_plain(raw_item)
        job_id = f"batch-{os.getpid()}-{idx}"
        return dispatch_batch_item(job_id, item["request"], adapter, dispatcher._post, max_attempts)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(run_item, enumerate(items)))


def _run_chat_completion(items: list[dict]) -> list:
    adapter = _router_adapter()
    if adapter is not None:
        return _run_routed_chat_completion(items, adapter)
    infer_url = os.getenv("INFER_URL", "http://127.0.0.1:8000/infer/v1/chat/completions")
    dispatcher = BatchDispatcher(infer_url)
    return [dispatcher.run(item) for item in items]


def _run_generic_preprocess(ds: ray.data.Dataset) -> list:
    res_ds = ds.map_batches(GenericPreprocessingMapper, batch_size=4096, concurrency=24)
    return [row for row in res_ds.iter_rows()]


def _load_and_run(args: argparse.Namespace) -> list:
    items = json.loads(Path(args.input).read_text())
    if args.type == "preprocess":
        return _run_generic_preprocess(ray.data.from_items(items))
    return _run_chat_completion(items)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--type", choices=["chat_completion", "preprocess"], default="chat_completion")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    ray.init()
    Path(args.output).write_text(json.dumps(_load_and_run(args)))


if __name__ == "__main__":
    main()
