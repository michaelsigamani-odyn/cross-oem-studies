"""
Installs lightweight stubs for `ray`, `ray.serve`, and `ray.job_submission`
before any test module is imported.  All three are top-level imports in
src/app.py and src/router.py; without these stubs the test collection phase
fails with ModuleNotFoundError in environments where the real `ray` package is
not installed.
"""
import sys
import types


def _install_stub(module_name: str, stub: types.ModuleType) -> None:
    if module_name not in sys.modules:
        sys.modules[module_name] = stub


def _make_remote_handle(cls: type) -> type:
    """Return a thin proxy that satisfies .options(...).remote(...) calls."""

    class _RemoteHandle:
        @staticmethod
        def options(**opts: object) -> "_RemoteHandle":
            class _Bound:
                @staticmethod
                def remote(*a: object, **kw: object) -> object:
                    return cls(*a, **kw)  # type: ignore[call-arg]

            return _Bound  # type: ignore[return-value]

        @staticmethod
        def remote(*a: object, **kw: object) -> object:
            return cls(*a, **kw)  # type: ignore[call-arg]

    return _RemoteHandle  # type: ignore[return-value]


# ── ray.actor ────────────────────────────────────────────────────────────────

_ray_actor = types.ModuleType("ray.actor")
_ray_actor.ActorHandle = object  # type: ignore[attr-defined]
_install_stub("ray.actor", _ray_actor)

# ── ray.serve ────────────────────────────────────────────────────────────────

_ray_serve = types.ModuleType("ray.serve")


def _deployment(*args: object, **kwargs: object) -> object:
    def _wrap(cls: type) -> type:
        cls.bind = classmethod(lambda c, *a, **kw: None)  # type: ignore[attr-defined]
        return cls

    if args and callable(args[0]) and not kwargs:
        return _wrap(args[0])  # type: ignore[arg-type]
    return _wrap


def _ingress(app: object) -> object:
    return lambda cls: cls


_ray_serve.deployment = _deployment  # type: ignore[attr-defined]
_ray_serve.ingress = _ingress  # type: ignore[attr-defined]
_install_stub("ray.serve", _ray_serve)

# ── ray.job_submission ───────────────────────────────────────────────────────

_ray_job_submission = types.ModuleType("ray.job_submission")


class _JobSubmissionClient:
    def __init__(self, address: str) -> None:
        pass

    def submit_job(self, entrypoint: str, **kwargs: object) -> str:
        return "stub-ray-job-id"

    def get_job_status(self, job_id: str) -> object:
        class _Status:
            value = "RUNNING"

        return _Status()

    def get_job_logs(self, job_id: str) -> str:
        return ""


_ray_job_submission.JobSubmissionClient = _JobSubmissionClient  # type: ignore[attr-defined]
_install_stub("ray.job_submission", _ray_job_submission)

# ── ray (parent) ─────────────────────────────────────────────────────────────
# Must be installed after submodules so we can attach them as attributes,
# which is required for `from ray import serve` to resolve the attribute
# lookup against the module object rather than falling back to a fresh import.

_ray = types.ModuleType("ray")


def _remote(*args: object, **kwargs: object) -> object:
    if args and callable(args[0]) and not kwargs:
        return _make_remote_handle(args[0])  # type: ignore[arg-type]

    def _decorator(cls: type) -> type:
        return _make_remote_handle(cls)

    return _decorator


def _get_actor(name: str) -> object:
    raise ValueError(f"no actor: {name}")


_ray.remote = _remote  # type: ignore[attr-defined]
_ray.get_actor = _get_actor  # type: ignore[attr-defined]
_ray.actor = _ray_actor  # type: ignore[attr-defined]
_ray.serve = _ray_serve  # type: ignore[attr-defined]
_ray.job_submission = _ray_job_submission  # type: ignore[attr-defined]

_install_stub("ray", _ray)

# ── yaml ─────────────────────────────────────────────────────────────────────
# Only used in serve_ops.snapshot_serve_config / rollback_to_snapshot at
# runtime; stubbed here so the module-level `import yaml` doesn't fail.

_yaml = types.ModuleType("yaml")
_yaml.safe_dump = lambda data, **kwargs: ""  # type: ignore[attr-defined]
_yaml.safe_load = lambda stream: {}  # type: ignore[attr-defined]
_install_stub("yaml", _yaml)
