"""
Cross-OEM cluster schema.

Canonical data model for machines, GPU specs, chat completions, and
offline batch jobs.  All wire-facing objects are OpenAI-API-compatible
so any client that speaks /v1/chat/completions or /v1/batch works
without modification.

Tailscale constraint
--------------------
Every worker node is reachable only via its Tailscale IP (100.x.x.x).
The Ray head node (AWS EC2) is the sole public-internet entry point and
is also on the Tailscale mesh.  No worker is directly addressable from
outside the mesh — all inbound traffic enters via the head node.
"""

from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class OEM(str, Enum):
    NVIDIA = "nvidia"
    AMD    = "amd"
    NONE   = "none"   # head / CPU-only nodes


class NodeRole(str, Enum):
    HEAD   = "head"    # Ray GCS + Serve proxy  (AWS EC2, public IP)
    WORKER = "worker"  # GPU compute             (Tailscale-only)


class GPUBackend(str, Enum):
    CUDA  = "cuda"    # NVIDIA — standard CUDA path
    ROCM  = "rocm"    # AMD    — ROCm / HIP path
    NONE  = "none"


class BatchJobStatus(str, Enum):
    PENDING   = "pending"
    RUNNING   = "running"
    COMPLETED = "completed"
    FAILED    = "failed"
    CANCELLED = "cancelled"


# ---------------------------------------------------------------------------
# GPU specification
# ---------------------------------------------------------------------------

@dataclass
class GPUSpec:
    """Hardware description of one physical GPU."""
    name: str                   # e.g. "NVIDIA B200", "AMD Radeon RX 9070 XT"
    memory_gb: float            # total VRAM
    backend: GPUBackend
    arch: str                   # CUDA compute capability *or* ROCm GFX target
    #   NVIDIA  → "sm_100" / "sm_90a"
    #   AMD     → "gfx1151"
    count: int = 1              # how many GPUs on this machine

    # Ray custom-resource key advertised to the scheduler
    @property
    def ray_resource_key(self) -> str:
        return "NVIDIA_GPU" if self.backend == GPUBackend.CUDA else "AMD_GPU"


# ---------------------------------------------------------------------------
# Machine
# ---------------------------------------------------------------------------

@dataclass
class Machine:
    """
    One physical (or virtual) node in the cluster.

    tailscale_ip is the *only* address workers are reachable on from
    outside Tailscale.  ssh_alias maps to ~/.ssh/config Host entries used
    by the worker-controller classes.
    """
    name: str                       # human label, e.g. "dgx-spark", "radeon"
    tailscale_ip: str               # 100.x.x.x  — authoritative reachability
    oem: OEM
    role: NodeRole
    gpu: GPUSpec | None = None      # None for head / CPU nodes

    # SSH alias from ~/.ssh/config  (worker controllers use this)
    ssh_alias: str = ""

    # Docker image used when the worker starts inside a container
    # (Radeon uses a custom ROCm image; DGX runs native)
    docker_image: str = ""

    @property
    def is_gpu_node(self) -> bool:
        return self.gpu is not None

    @property
    def ray_resources(self) -> dict[str, float]:
        """Custom resources to advertise when joining the Ray cluster."""
        if self.gpu is None:
            return {}
        return {self.gpu.ray_resource_key: float(self.gpu.count)}


# ---------------------------------------------------------------------------
# Cluster topology
# ---------------------------------------------------------------------------

@dataclass
class ClusterTopology:
    """
    Full description of the testnet.

    The Ray head is also the only node with a public IP; all workers are
    Tailscale-only and cannot be reached from the internet directly.
    """
    head: Machine
    workers: list[Machine]

    # Ray GCS address  (workers join via this)
    gcs_address: str = ""
    # Ray Serve / dashboard HTTP address  (deploy target)
    dashboard_url: str = ""

    @property
    def all_nodes(self) -> list[Machine]:
        return [self.head] + self.workers

    def worker_by_oem(self, oem: OEM) -> Machine | None:
        return next((w for w in self.workers if w.oem == oem), None)


# ---------------------------------------------------------------------------
# Default testnet topology
# ---------------------------------------------------------------------------

def default_topology() -> ClusterTopology:
    """
    Default testnet: values pulled from environment overrides.

    IPs fall back to loopback for local development when no overrides
    are provided.
    """
    head_ip = os.getenv("EXPECTED_HEAD_IP", "127.0.0.1")
    dgx_ip = os.getenv("EXPECTED_DGX_IP", "127.0.0.1")
    radeon_ip = os.getenv("EXPECTED_RADEON_IP", "127.0.0.1")
    head = Machine(
        name="aws-head",
        tailscale_ip=head_ip,
        oem=OEM.NONE,
        role=NodeRole.HEAD,
        gpu=None,
        ssh_alias=os.getenv("AWS_SSH_HOST", "ubuntu@127.0.0.1"),
    )

    dgx = Machine(
        name="dgx-spark",
        tailscale_ip=dgx_ip,
        oem=OEM.NVIDIA,
        role=NodeRole.WORKER,
        ssh_alias="dgx",
        docker_image=os.getenv("NVIDIA_WORKER_IMAGE", "michaelsigamaniodyn/runtime-vllm-dgx-spark:cuda12.9"),
        gpu=GPUSpec(
            name="NVIDIA B200",
            memory_gb=96.0,
            backend=GPUBackend.CUDA,
            arch="sm_100",
            count=1,
        ),
    )

    radeon = Machine(
        name="radeon",
        tailscale_ip=radeon_ip,
        oem=OEM.AMD,
        role=NodeRole.WORKER,
        ssh_alias="radeon",
        docker_image="michaelsigamaniodyn/runtime-vllm-radeon:rocm721-gfx1151",
        gpu=GPUSpec(
            name="AMD Radeon RX 9070 XT",
            memory_gb=16.0,
            backend=GPUBackend.ROCM,
            arch="gfx1151",
            count=1,
        ),
    )

    return ClusterTopology(
        head=head,
        workers=[dgx, radeon],
        gcs_address=os.getenv("RAY_GCS_ADDRESS", "127.0.0.1:6379"),
        dashboard_url=os.getenv("RAY_DASHBOARD_URL", "http://127.0.0.1:8265"),
    )


# ---------------------------------------------------------------------------
# Chat completions  (OpenAI-compatible)
# ---------------------------------------------------------------------------

@dataclass
class ChatMessage:
    role: str      # "system" | "user" | "assistant"
    content: str


@dataclass
class ChatCompletionRequest:
    """
    POST /v1/chat/completions

    Subset of the OpenAI Chat Completions API accepted by vLLM.
    """
    model: str
    messages: list[ChatMessage]
    max_tokens: int               = 512
    temperature: float            = 0.7
    top_p: float                  = 1.0
    n: int                        = 1
    stream: bool                  = False
    stop: list[str]               = field(default_factory=list)
    # Arbitrary extra kwargs forwarded verbatim to vLLM
    extra: dict[str, Any]         = field(default_factory=dict)

    def to_wire(self) -> dict[str, Any]:
        body: dict[str, Any] = {
            "model":       self.model,
            "messages":    [{"role": m.role, "content": m.content} for m in self.messages],
            "max_tokens":  self.max_tokens,
            "temperature": self.temperature,
            "top_p":       self.top_p,
            "n":           self.n,
            "stream":      self.stream,
        }
        if self.stop:
            body["stop"] = self.stop
        body.update(self.extra)
        return body


@dataclass
class ChatCompletionChoice:
    index: int
    message: ChatMessage
    finish_reason: str   # "stop" | "length" | "content_filter"


@dataclass
class UsageStats:
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int


@dataclass
class ChatCompletionResponse:
    """
    Matches the OpenAI Chat Completions response schema returned by vLLM.
    """
    id: str
    object: str                      # "chat.completion"
    created: int                     # unix timestamp
    model: str
    choices: list[ChatCompletionChoice]
    usage: UsageStats
    served_by: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_wire(cls, data: dict[str, Any]) -> ChatCompletionResponse:
        choices = [
            ChatCompletionChoice(
                index=c["index"],
                message=ChatMessage(
                    role=c["message"]["role"],
                    content=c["message"]["content"],
                ),
                finish_reason=c.get("finish_reason", "stop"),
            )
            for c in data.get("choices", [])
        ]
        u = data.get("usage", {})
        return cls(
            id=data.get("id", ""),
            object=data.get("object", "chat.completion"),
            created=data.get("created", 0),
            model=data.get("model", ""),
            choices=choices,
            usage=UsageStats(
                prompt_tokens=u.get("prompt_tokens", 0),
                completion_tokens=u.get("completion_tokens", 0),
                total_tokens=u.get("total_tokens", 0),
            ),
            served_by=data.get("_odyn_served_by", {}),
        )


# ---------------------------------------------------------------------------
# Offline batch
# ---------------------------------------------------------------------------

@dataclass
class BatchRequestItem:
    """One item in an offline batch — one request, one response slot."""
    custom_id: str                              # caller-supplied correlation key
    request: ChatCompletionRequest
    response: ChatCompletionResponse | None = None
    error: str | None = None                    # populated on per-item failure

    @property
    def succeeded(self) -> bool:
        return self.response is not None and self.error is None


@dataclass
class BatchJob:
    """
    POST /v1/batch  —  offline batch processing job.

    The endpoint accepts a list of BatchRequestItems, fans them out
    across the cluster concurrently, and returns all results when every
    item has completed or failed.

    Compatible with the OpenAI Batch API response envelope so callers
    that poll /v1/batch/{id} get the same shape regardless of backend.
    """
    id: str                             = field(default_factory=lambda: f"batch_{uuid.uuid4().hex[:12]}")
    object: str                         = "batch"
    endpoint: str                       = "/v1/chat/completions"
    items: list[BatchRequestItem]       = field(default_factory=list)
    status: BatchJobStatus              = BatchJobStatus.PENDING
    created_at: int                     = field(default_factory=lambda: int(time.time()))
    started_at: int | None              = None
    completed_at: int | None            = None
    # Metadata bag — caller may attach arbitrary string keys
    metadata: dict[str, str]            = field(default_factory=dict)

    @property
    def total(self) -> int:
        return len(self.items)

    @property
    def completed_count(self) -> int:
        return sum(1 for i in self.items if i.succeeded)

    @property
    def failed_count(self) -> int:
        return sum(1 for i in self.items if i.error is not None)

    @property
    def pending_count(self) -> int:
        return sum(1 for i in self.items if i.response is None and i.error is None)

    def to_status_dict(self) -> dict[str, Any]:
        return {
            "id":              self.id,
            "object":          self.object,
            "endpoint":        self.endpoint,
            "status":          self.status.value,
            "created_at":      self.created_at,
            "started_at":      self.started_at,
            "completed_at":    self.completed_at,
            "request_counts": {
                "total":     self.total,
                "completed": self.completed_count,
                "failed":    self.failed_count,
            },
            "metadata":        self.metadata,
        }
