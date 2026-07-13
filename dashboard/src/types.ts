export interface ChatMessage {
  role: "user" | "assistant" | "system";
  content: string;
}

export interface ChatCompletionRequest {
  model: string;
  messages: ChatMessage[];
  max_tokens?: number;
  stream?: boolean;
}

export interface ChatCompletionResponse {
  id: string;
  model: string;
  choices: {
    index: number;
    message: ChatMessage;
    finish_reason: string;
  }[];
  usage: {
    prompt_tokens: number;
    completion_tokens: number;
    total_tokens: number;
  };
}

export interface BatchRequest {
  custom_id: string;
  request: ChatCompletionRequest;
}

export interface BatchJobStatus {
  id: string;
  status: "pending" | "running" | "completed" | "failed" | "cancelled";
  request_counts: {
    total: number;
    completed: number;
    failed: number;
  };
}

export interface RayJobStatus {
  submission_id: string;
  status: "PENDING" | "RUNNING" | "SUCCEEDED" | "FAILED" | "STOPPED";
}

export interface GPUNode {
  name: string;
  node_ip?: string;
  status: "LIVE" | "WARMING" | "IDLE" | "FAILED" | "OFFLINE" | "RESPAWNING";
  raw_status?: string;
  role?: "active" | "standby" | "unavailable" | "unmanaged";
  gpu_utilisation: number;
  memory_used_mib: number;
  memory_total_mib: number;
  jobs_total: number;
  jobs_failed: number;
  respawns: number;
  last_latency_ms: number;
  last_check: number;
  last_check_iso: string;
  last_error: string | null;
  inflight: number;
  latency_p50_ms: number;
  latency_p95_ms: number;
  latency_p99_ms: number;
  sla_ok?: boolean;
  sla_p95_target_ms?: number;
  queue_depth?: number;
  readiness_ok?: boolean;
}

export interface StreamMetrics {
  ttft: number | null;
  tps: number | null;
  latency: number | null;
  output_tokens: number | null;
  served_by: string | null;
}

export interface HistoricalJob {
  id: string;
  type: "online_chat" | "offline_batch" | "ray_job";
  status: string;
  submittedAt: string;
  details: string;
}
