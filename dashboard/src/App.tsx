import React, { useState, useEffect, useRef } from "react";
import { 
  Key, Server, Cpu, Database, Play, Terminal, LogOut, 
  AlertTriangle, PlayCircle, StopCircle, RefreshCw, Layers 
} from "lucide-react";
import type { 
  BatchJobStatus, RayJobStatus, GPUNode, StreamMetrics, HistoricalJob 
} from "./types";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1/v1";

function App() {
  const [apiKey, setApiKey] = useState<string | null>(null);
  const [apiKeyInput, setApiKeyInput] = useState("");
  const [authError, setApiKeyError] = useState("");

  // Nodes Observability
  const [nodes, setNodes] = useState<GPUNode[]>([]);
  const [nodePollingError, setNodePollingError] = useState("");

  // Online Chat State
  const [chatPrompt, setChatPrompt] = useState("");
  const [chatResponse, setChatResponse] = useState("");
  const [chatState, setChatState] = useState<"Idle" | "Submitting" | "Running" | "Succeeded" | "Failed">("Idle");
  const [chatError, setChatError] = useState("");
  const [chatMetrics, setChatMetrics] = useState<StreamMetrics>({ ttft: null, tps: null, latency: null, output_tokens: null, served_by: null });
  const activeStreamController = useRef<AbortController | null>(null);

  // Offline Batch State
  const [batchPrompts, setBatchPrompts] = useState("");
  const [batchState, setBatchState] = useState<"Idle" | "Submitting" | "Running" | "Succeeded" | "Failed">("Idle");
  const [batchError, setBatchError] = useState("");

  // Ray Job State
  const [rayType, setRayType] = useState<"chat_completion" | "preprocess">("chat_completion");
  const [rayInput, setRayInput] = useState("/tmp/batch_input_batch_b880814dc56c.json");
  const [rayOutput, setRayOutput] = useState("/tmp/test_ray_output.json");
  const [rayState, setRayState] = useState<"Idle" | "Submitting" | "Running" | "Succeeded" | "Failed">("Idle");
  const [rayError, setRayError] = useState("");

  // Global Session History / Job Tracking
  const [jobs, setJobs] = useState<HistoricalJob[]>([]);
  const [rateLimitTimeout, setRateLimitTimeout] = useState<number | null>(null);

  // Active logs viewer modal
  const [selectedLogsJobId, setSelectedLogsJobId] = useState<string | null>(null);
  const [selectedJobLogs, setSelectedLogs] = useState<string | null>(null);
  const [logsLoading, setLogsLoading] = useState(false);

  // 1. Error Response Handler
  const handleHttpError = (status: number, message: string, cardSetter?: (err: string) => void) => {
    if (status === 401 || status === 403) {
      setApiKey(null);
      setApiKeyError(`Authentication failed (${status}): Please re-enter a valid API Key.`);
      return;
    }
    if (status === 429) {
      setRateLimitTimeout(30);
      const errMsg = "Rate limit exceeded (429): Throttling active.";
      if (cardSetter) cardSetter(errMsg);
      return;
    }
    const errMsg = `Server error (${status}): ${message}`;
    if (cardSetter) cardSetter(errMsg);
  };

  // Rate limit countdown effect
  useEffect(() => {
    if (rateLimitTimeout === null) return;
    if (rateLimitTimeout <= 0) {
      setRateLimitTimeout(null);
      return;
    }
    const interval = setInterval(() => {
      setRateLimitTimeout(prev => (prev !== null ? prev - 1 : null));
    }, 1000);
    return () => clearInterval(interval);
  }, [rateLimitTimeout]);

  // 2. Fetch Nodes Status
  const fetchNodesStatus = async (currentKey: string) => {
    try {
      const res = await fetch(`${API_BASE_URL}/nodes`, {
        headers: { "x-api-key": currentKey }
      });
      if (res.status !== 200) {
        handleHttpError(res.status, await res.text());
        return;
      }
      const data: GPUNode[] = await res.json();
      setNodes(data);
      setNodePollingError("");
    } catch (e: any) {
      setNodePollingError(`Connection timeout / network error: ${e.message}`);
    }
  };

  // Node polling interval
  useEffect(() => {
    if (!apiKey) return;
    fetchNodesStatus(apiKey);
    const interval = setInterval(() => fetchNodesStatus(apiKey), 5000);
    return () => clearInterval(interval);
  }, [apiKey]);

  // 3. Submit Streaming Online Chat
  const submitOnlineChat = async () => {
    if (!chatPrompt.trim() || !apiKey) return;
    setChatState("Submitting");
    setChatResponse("");
    setChatError("");
    setChatMetrics({ ttft: null, tps: null, latency: null, output_tokens: null, served_by: null });

    const startTime = performance.now();
    activeStreamController.current = new AbortController();

    try {
      const response = await fetch(`${API_BASE_URL}/chat/completions`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-api-key": apiKey
        },
        signal: activeStreamController.current.signal,
        body: JSON.stringify({
          model: "qwen2.5-7b",
          messages: [{ role: "user", content: chatPrompt }],
          stream: true,
          max_tokens: 256
        })
      });

      if (response.status !== 200) {
        setChatState("Failed");
        handleHttpError(response.status, await response.text(), setChatError);
        return;
      }

      setChatState("Running");
      const servedBy = response.headers.get("x-served-by");
      if (servedBy) {
        setChatMetrics(m => ({ ...m, served_by: servedBy }));
      }
      const reader = response.body?.getReader();
      const decoder = new TextDecoder();
      if (!reader) {
        setChatState("Failed");
        setChatError("Readable stream not supported by browser.");
        return;
      }

      let ttftCalculated = false;
      let accumText = "";
      let totalTokens = 0;

      const newJobId = `chatcmpl-${Math.random().toString(36).substring(2, 10)}`;
      const jobRecord: HistoricalJob = {
        id: newJobId,
        type: "online_chat",
        status: "RUNNING",
        submittedAt: new Date().toLocaleTimeString(),
        details: `Prompt: "${chatPrompt.substring(0, 30)}..."${servedBy ? ` → routed to ${servedBy}` : ""}`
      };
      setJobs(prev => [jobRecord, ...prev]);

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;

        const chunk = decoder.decode(value, { stream: true });
        const lines = chunk.split("\n");
        for (const line of lines) {
          const cleanLine = line.trim();
          if (cleanLine.startsWith("data: ")) {
            const dataStr = cleanLine.slice(6).trim();
            if (dataStr === "[DONE]") continue;
            try {
              const parsed = JSON.parse(dataStr);
              const token = parsed.choices[0]?.delta?.content || "";
              if (token) {
                accumText += token;
                totalTokens++;
                setChatResponse(accumText);

                if (!ttftCalculated) {
                  setChatMetrics(m => ({ ...m, ttft: performance.now() - startTime }));
                  ttftCalculated = true;
                }
              }
            } catch (e) {}
          }
        }

        const elapsedSeconds = (performance.now() - startTime) / 1000;
        setChatMetrics(m => ({
          ...m,
          tps: elapsedSeconds > 0 ? totalTokens / elapsedSeconds : 0,
          latency: performance.now() - startTime,
          output_tokens: totalTokens
        }));
      }

      setChatState("Succeeded");
      setJobs(prev => prev.map(j => j.id === newJobId ? { ...j, status: "SUCCEEDED" } : j));
    } catch (e: any) {
      if (e.name === "AbortError") {
        setChatState("Failed");
        setChatError("Stream aborted by user.");
      } else {
        setChatState("Failed");
        setChatError(`Network or timeout error: ${e.message}`);
      }
    } finally {
      activeStreamController.current = null;
    }
  };

  const cancelChatStream = () => {
    if (activeStreamController.current) {
      activeStreamController.current.abort();
    }
  };

  // 4. Submit Offline Batch
  const submitOfflineBatch = async () => {
    if (!batchPrompts.trim() || !apiKey) return;
    setBatchState("Submitting");
    setBatchError("");

    const promptList = batchPrompts.split("\n").filter(p => p.trim());
    const payload = promptList.map((prompt, index) => ({
      custom_id: `q${index + 1}`,
      request: {
        model: "qwen2.5-7b",
        messages: [{ role: "user", content: prompt }],
        max_tokens: 256
      }
    }));

    try {
      const res = await fetch(`${API_BASE_URL}/batch`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-api-key": apiKey
        },
        body: JSON.stringify(payload)
      });

      if (res.status !== 200) {
        setBatchState("Failed");
        handleHttpError(res.status, await res.text(), setBatchError);
        return;
      }

      const data = await res.json();
      const jobId = data.id;

      setBatchState("Running");
      const jobRecord: HistoricalJob = {
        id: jobId,
        type: "offline_batch",
        status: "RUNNING",
        submittedAt: new Date().toLocaleTimeString(),
        details: `Submitted ${promptList.length} prompts`
      };
      setJobs(prev => [jobRecord, ...prev]);
      pollBatchStatus(jobId);
    } catch (e: any) {
      setBatchState("Failed");
      setBatchError(`Network or timeout error: ${e.message}`);
    }
  };

  // Poll Offline Batch
  const MAX_POLL_NETWORK_ERRORS = 5;
  const pollBatchStatus = async (jobId: string, networkErrors = 0) => {
    if (!apiKey) return;
    try {
      const res = await fetch(`${API_BASE_URL}/batch/${jobId}`, {
        headers: { "x-api-key": apiKey }
      });
      if (res.status !== 200) {
        handleHttpError(res.status, await res.text(), setBatchError);
        return;
      }
      const data: BatchJobStatus = await res.json();
      const statusStr = `${data.status.toUpperCase()} (${data.request_counts.completed}/${data.request_counts.total}${data.request_counts.failed > 0 ? `, ${data.request_counts.failed} failed` : ""})`;

      setJobs(prev => prev.map(j => {
        if (j.id === jobId) {
          return { ...j, status: data.status.toUpperCase(), details: statusStr };
        }
        return j;
      }));

      if (data.status === "completed" || data.status === "failed" || data.status === "cancelled") {
        setBatchState(data.status === "completed" ? "Succeeded" : "Failed");
        return;
      }

      setTimeout(() => pollBatchStatus(jobId), 4000);
    } catch (e) {
      if (networkErrors + 1 >= MAX_POLL_NETWORK_ERRORS) {
        setBatchState("Failed");
        setBatchError("Lost connection while polling batch status.");
        return;
      }
      setTimeout(() => pollBatchStatus(jobId, networkErrors + 1), 4000);
    }
  };

  // 5. Submit Ray Job
  const submitRayJob = async () => {
    if (!apiKey) return;
    setRayState("Submitting");
    setRayError("");

    const payload = {
      entrypoint: `python3.12 /home/ubuntu/batch_job.py --input ${rayInput} --output ${rayOutput} --type ${rayType}`,
      runtime_env: {
        pip: ["pandas", "pyarrow"]
      }
    };

    try {
      const res = await fetch(`${API_BASE_URL}/jobs`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-api-key": apiKey
        },
        body: JSON.stringify(payload)
      });

      if (res.status !== 200) {
        setRayState("Failed");
        handleHttpError(res.status, await res.text(), setRayError);
        return;
      }

      const data = await res.json();
      const jobId = data.submission_id;

      setRayState("Running");
      const jobRecord: HistoricalJob = {
        id: jobId,
        type: "ray_job",
        status: "PENDING",
        submittedAt: new Date().toLocaleTimeString(),
        details: `Type: ${rayType}`
      };
      setJobs(prev => [jobRecord, ...prev]);
      pollRayStatus(jobId);
    } catch (e: any) {
      setRayState("Failed");
      setRayError(`Network or timeout error: ${e.message}`);
    }
  };

  // Poll Ray Job
  const pollRayStatus = async (jobId: string, networkErrors = 0) => {
    if (!apiKey) return;
    try {
      const res = await fetch(`${API_BASE_URL}/jobs/${jobId}`, {
        headers: { "x-api-key": apiKey }
      });
      if (res.status !== 200) {
        handleHttpError(res.status, await res.text(), setRayError);
        return;
      }
      const data: RayJobStatus = await res.json();

      setJobs(prev => prev.map(j => {
        if (j.id === jobId) {
          return { ...j, status: data.status };
        }
        return j;
      }));

      if (data.status === "SUCCEEDED" || data.status === "FAILED" || data.status === "STOPPED") {
        setRayState(data.status === "SUCCEEDED" ? "Succeeded" : "Failed");
        return;
      }

      setTimeout(() => pollRayStatus(jobId), 3000);
    } catch (e) {
      if (networkErrors + 1 >= MAX_POLL_NETWORK_ERRORS) {
        setRayState("Failed");
        setRayError("Lost connection while polling Ray job status.");
        return;
      }
      setTimeout(() => pollRayStatus(jobId, networkErrors + 1), 3000);
    }
  };

  // Fetch Ray Logs
  const fetchJobLogs = async (jobId: string) => {
    if (!apiKey) return;
    setSelectedLogsJobId(jobId);
    setSelectedLogs(null);
    setLogsLoading(true);

    try {
      const res = await fetch(`${API_BASE_URL}/jobs/${jobId}/logs`, {
        headers: { "x-api-key": apiKey }
      });
      if (res.status !== 200) {
        handleHttpError(res.status, await res.text());
        return;
      }
      const data = await res.json();
      setSelectedLogs(data.logs);
    } catch (e: any) {
      setSelectedLogs(`Failed to fetch logs: ${e.message}`);
    } finally {
      setLogsLoading(false);
    }
  };

  // 6. Handle Login Submit
  const handleLogin = (e: React.FormEvent) => {
    e.preventDefault();
    if (!apiKeyInput.trim()) {
      setApiKeyError("API key is required");
      return;
    }
    setApiKey(apiKeyInput.trim());
    setApiKeyError("");
  };

  const handleLogout = () => {
    setApiKey(null);
    setApiKeyInput("");
    setJobs([]);
  };

  // API Key entry screen (React state-only)
  if (!apiKey) {
    return (
      <div className="min-h-screen bg-odyn-dark flex items-center justify-center p-4">
        <div className="max-w-md w-full bg-odyn-card border border-gray-800 p-8 rounded-xl shadow-2xl space-y-6">
          <div className="text-center space-y-2">
            <div className="mx-auto bg-odyn-teal/10 p-3 rounded-full w-fit">
              <Layers className="h-10 w-10 text-odyn-teal" />
            </div>
            <h1 className="text-2xl font-bold tracking-tight text-white">Odyn Cross-OEM</h1>
            <p className="text-sm text-odyn-gray">Heterogeneous GPU Cluster Management Dashboard</p>
          </div>

          <form onSubmit={handleLogin} className="space-y-4">
            <div className="space-y-1">
              <label className="text-xs font-semibold text-odyn-gray uppercase tracking-wider">API Access Key</label>
              <div className="relative">
                <input
                  type="password"
                  placeholder="Enter x-api-key..."
                  value={apiKeyInput}
                  onChange={(e) => setApiKeyInput(e.target.value)}
                  className="w-full bg-odyn-dark border border-gray-800 focus:border-odyn-teal outline-none py-3 pl-10 pr-4 text-sm text-white rounded-lg transition-colors placeholder-gray-600"
                  required
                />
                <Key className="absolute left-3 top-3.5 h-4 w-4 text-gray-500" />
              </div>
            </div>

            {authError && (
              <div className="bg-odyn-red/10 border border-odyn-red/30 p-3 rounded-lg flex items-start gap-2 text-xs text-odyn-red">
                <AlertTriangle className="h-4 w-4 shrink-0 mt-0.5" />
                <span>{authError}</span>
              </div>
            )}

            <button
              type="submit"
              className="w-full bg-odyn-teal hover:bg-odyn-teal/90 text-black py-3 text-sm font-semibold rounded-lg transition-all flex items-center justify-center gap-2 shadow-lg shadow-odyn-teal/20"
            >
              <Server className="h-4 w-4" />
              <span>Connect to Cluster</span>
            </button>
          </form>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-odyn-dark text-gray-100 flex flex-col font-sans">
      {/* Rate limit warning banner */}
      {rateLimitTimeout !== null && (
        <div className="bg-odyn-red text-white py-2 px-4 text-center text-xs font-semibold flex items-center justify-center gap-2 animate-pulse">
          <AlertTriangle className="h-4 w-4" />
          <span>Rate limit hit. Cooling down... Retrying in {rateLimitTimeout}s</span>
        </div>
      )}

      {/* Header */}
      <header className="bg-odyn-card border-b border-gray-800 px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Layers className="h-6 w-6 text-odyn-teal" />
          <div>
            <h1 className="text-lg font-bold tracking-tight text-white leading-none">Odyn Cross-OEM</h1>
            <span className="text-[10px] text-odyn-gray uppercase font-semibold tracking-wider">Multi-Tenant Management Console</span>
          </div>
        </div>

        <div className="flex items-center gap-4">
          <div className="bg-odyn-dark border border-gray-800 px-3 py-1.5 rounded-lg flex items-center gap-2 text-xs">
            <span className="h-2 w-2 rounded-full bg-odyn-green animate-ping"></span>
            <span className="text-odyn-gray">Connected with API Key</span>
          </div>
          <button
            onClick={handleLogout}
            className="bg-gray-800 hover:bg-odyn-red/20 hover:text-odyn-red p-2 rounded-lg transition-colors"
            title="Log Out"
          >
            <LogOut className="h-4 w-4" />
          </button>
        </div>
      </header>

      {/* Main Content Dashboard layout */}
      <main className="flex-1 p-6 grid grid-cols-1 lg:grid-cols-4 gap-6 max-w-7xl w-full mx-auto">
        {/* Left Column: Workload Submission Panel (3 Cards) */}
        <div className="lg:col-span-3 space-y-6">
          {/* Metrics Row */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div className="bg-odyn-card border border-gray-800 p-4 rounded-xl space-y-1">
              <span className="text-xs text-odyn-gray uppercase font-semibold">Total Session Jobs</span>
              <div className="text-2xl font-bold text-white">{jobs.length}</div>
            </div>
            <div className="bg-odyn-card border border-gray-800 p-4 rounded-xl space-y-1">
              <span className="text-xs text-odyn-gray uppercase font-semibold">Active Jobs</span>
              <div className="text-2xl font-bold text-odyn-teal">
                {jobs.filter(j => ["RUNNING", "PENDING"].includes(j.status)).length}
              </div>
            </div>
            <div className="bg-odyn-card border border-gray-800 p-4 rounded-xl space-y-1">
              <span className="text-xs text-odyn-gray uppercase font-semibold">Succeeded</span>
              <div className="text-2xl font-bold text-odyn-green">
                {jobs.filter(j => ["SUCCEEDED", "COMPLETED"].includes(j.status)).length}
              </div>
            </div>
            <div className="bg-odyn-card border border-gray-800 p-4 rounded-xl space-y-1">
              <span className="text-xs text-odyn-gray uppercase font-semibold">Failed / Stopped</span>
              <div className="text-2xl font-bold text-odyn-red">
                {jobs.filter(j => ["FAILED", "STOPPED", "CANCELLED"].includes(j.status)).length}
              </div>
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            {/* Card 1: Online Chat */}
            <div className="bg-odyn-card border border-gray-800 p-5 rounded-xl flex flex-col justify-between space-y-4">
              <div className="space-y-3">
                <div className="flex items-center justify-between border-b border-gray-800 pb-2">
                  <div className="flex items-center gap-2">
                    <Database className="h-4 w-4 text-odyn-teal" />
                    <h3 className="font-semibold text-white">Online Chat</h3>
                  </div>
                  <span className={`text-[10px] uppercase font-bold px-2 py-0.5 rounded-full ${
                    chatState === "Running" ? "bg-odyn-green/10 text-odyn-green" : 
                    chatState === "Submitting" ? "bg-odyn-teal/10 text-odyn-teal animate-pulse" : 
                    chatState === "Failed" ? "bg-odyn-red/10 text-odyn-red" : "bg-gray-800 text-odyn-gray"
                  }`}>{chatState}</span>
                </div>

                <div className="space-y-1">
                  <label className="text-[10px] text-odyn-gray font-bold uppercase tracking-wider">User Prompt</label>
                  <input
                    type="text"
                    placeholder="Ask something..."
                    value={chatPrompt}
                    onChange={(e) => setChatPrompt(e.target.value)}
                    className="w-full bg-odyn-dark border border-gray-800 focus:border-odyn-teal outline-none px-3 py-2 text-sm text-white rounded-lg transition-colors placeholder-gray-700"
                    disabled={chatState === "Running" || chatState === "Submitting"}
                  />
                </div>

                {chatResponse && (
                  <div className="bg-odyn-dark border border-gray-800 p-3 rounded-lg text-xs font-mono max-h-[160px] overflow-y-auto text-gray-300">
                    {chatResponse}
                  </div>
                )}

                {chatError && (
                  <div className="text-xs text-odyn-red bg-odyn-red/5 border border-odyn-red/10 p-2 rounded">
                    {chatError}
                  </div>
                )}
              </div>

              {/* Streaming metrics display */}
              {chatMetrics.latency !== null && (
                <div className="grid grid-cols-2 gap-2 border-t border-gray-800 pt-3 text-[10px] font-mono text-odyn-gray">
                  <div>TTFT: <span className="text-white">{chatMetrics.ttft?.toFixed(0) || "-"}ms</span></div>
                  <div>TPS: <span className="text-white">{chatMetrics.tps?.toFixed(1) || "-"} t/s</span></div>
                  <div>Latency: <span className="text-white">{((chatMetrics.latency || 0) / 1000).toFixed(2)}s</span></div>
                  <div>Tokens: <span className="text-white">{chatMetrics.output_tokens || 0}</span></div>
                  {chatMetrics.served_by && (
                    <div className="col-span-2">Routed to: <span className="text-odyn-teal">{chatMetrics.served_by}</span></div>
                  )}
                </div>
              )}

              <div className="pt-2">
                {chatState === "Running" || chatState === "Submitting" ? (
                  <button
                    onClick={cancelChatStream}
                    className="w-full bg-odyn-red hover:bg-odyn-red/90 text-white py-2 text-xs font-bold rounded-lg transition-all flex items-center justify-center gap-2"
                  >
                    <StopCircle className="h-4 w-4" />
                    <span>Cancel Stream</span>
                  </button>
                ) : (
                  <button
                    onClick={submitOnlineChat}
                    className="w-full bg-odyn-teal hover:bg-odyn-teal/90 text-black py-2 text-xs font-bold rounded-lg transition-all flex items-center justify-center gap-2 shadow-lg shadow-odyn-teal/10"
                    disabled={rateLimitTimeout !== null}
                  >
                    <Play className="h-4 w-4" />
                    <span>Submit Request</span>
                  </button>
                )}
              </div>
            </div>

            {/* Card 2: Offline Batch */}
            <div className="bg-odyn-card border border-gray-800 p-5 rounded-xl flex flex-col justify-between space-y-4">
              <div className="space-y-3">
                <div className="flex items-center justify-between border-b border-gray-800 pb-2">
                  <div className="flex items-center gap-2">
                    <Server className="h-4 w-4 text-odyn-teal" />
                    <h3 className="font-semibold text-white">Offline Batch</h3>
                  </div>
                  <span className={`text-[10px] uppercase font-bold px-2 py-0.5 rounded-full ${
                    batchState === "Running" ? "bg-odyn-teal/10 text-odyn-teal animate-pulse" : 
                    batchState === "Succeeded" ? "bg-odyn-green/10 text-odyn-green" : 
                    batchState === "Failed" ? "bg-odyn-red/10 text-odyn-red" : "bg-gray-800 text-odyn-gray"
                  }`}>{batchState}</span>
                </div>

                <div className="space-y-1">
                  <label className="text-[10px] text-odyn-gray font-bold uppercase tracking-wider">Prompts (One per line)</label>
                  <textarea
                    rows={4}
                    placeholder="Prompt 1&#10;Prompt 2..."
                    value={batchPrompts}
                    onChange={(e) => setBatchPrompts(e.target.value)}
                    className="w-full bg-odyn-dark border border-gray-800 focus:border-odyn-teal outline-none px-3 py-2 text-xs text-white rounded-lg transition-colors placeholder-gray-700 resize-none font-mono"
                    disabled={batchState === "Submitting" || batchState === "Running"}
                  />
                </div>

                {batchError && (
                  <div className="text-xs text-odyn-red bg-odyn-red/5 border border-odyn-red/10 p-2 rounded">
                    {batchError}
                  </div>
                )}
              </div>

              <div className="pt-2">
                <button
                  onClick={submitOfflineBatch}
                  className="w-full bg-odyn-teal hover:bg-odyn-teal/90 text-black py-2 text-xs font-bold rounded-lg transition-all flex items-center justify-center gap-2"
                  disabled={batchState === "Running" || batchState === "Submitting" || rateLimitTimeout !== null}
                >
                  <PlayCircle className="h-4 w-4" />
                  <span>Submit Batch</span>
                </button>
              </div>
            </div>

            {/* Card 3: Ray Jobs */}
            <div className="bg-odyn-card border border-gray-800 p-5 rounded-xl flex flex-col justify-between space-y-4">
              <div className="space-y-3">
                <div className="flex items-center justify-between border-b border-gray-800 pb-2">
                  <div className="flex items-center gap-2">
                    <Cpu className="h-4 w-4 text-odyn-teal" />
                    <h3 className="font-semibold text-white">Ray Job SDK</h3>
                  </div>
                  <span className={`text-[10px] uppercase font-bold px-2 py-0.5 rounded-full ${
                    rayState === "Running" ? "bg-odyn-teal/10 text-odyn-teal animate-pulse" : 
                    rayState === "Succeeded" ? "bg-odyn-green/10 text-odyn-green" : 
                    rayState === "Failed" ? "bg-odyn-red/10 text-odyn-red" : "bg-gray-800 text-odyn-gray"
                  }`}>{rayState}</span>
                </div>

                <div className="grid grid-cols-2 gap-2">
                  <div className="space-y-1">
                    <label className="text-[10px] text-odyn-gray font-bold uppercase tracking-wider">Job Type</label>
                    <select
                      value={rayType}
                      onChange={(e) => setRayType(e.target.value as any)}
                      className="w-full bg-odyn-dark border border-gray-800 outline-none text-xs text-white rounded-lg p-2"
                      disabled={rayState === "Running" || rayState === "Submitting"}
                    >
                      <option value="chat_completion">ChatCompletion</option>
                      <option value="preprocess">Preprocess</option>
                    </select>
                  </div>
                  <div className="space-y-1">
                    <label className="text-[10px] text-odyn-gray font-bold uppercase tracking-wider">Input Path</label>
                    <input
                      type="text"
                      value={rayInput}
                      onChange={(e) => setRayInput(e.target.value)}
                      className="w-full bg-odyn-dark border border-gray-800 outline-none text-xs text-white rounded-lg p-2"
                      disabled={rayState === "Running" || rayState === "Submitting"}
                    />
                  </div>
                </div>

                <div className="space-y-1">
                  <label className="text-[10px] text-odyn-gray font-bold uppercase tracking-wider">Output Path</label>
                  <input
                    type="text"
                    value={rayOutput}
                    onChange={(e) => setRayOutput(e.target.value)}
                    className="w-full bg-odyn-dark border border-gray-800 outline-none text-xs text-white rounded-lg p-2"
                    disabled={rayState === "Running" || rayState === "Submitting"}
                  />
                </div>

                {rayError && (
                  <div className="text-xs text-odyn-red bg-odyn-red/5 border border-odyn-red/10 p-2 rounded">
                    {rayError}
                  </div>
                )}
              </div>

              <div className="pt-2">
                <button
                  onClick={submitRayJob}
                  className="w-full bg-odyn-teal hover:bg-odyn-teal/90 text-black py-2 text-xs font-bold rounded-lg transition-all flex items-center justify-center gap-2"
                  disabled={rayState === "Running" || rayState === "Submitting" || rateLimitTimeout !== null}
                >
                  <PlayCircle className="h-4 w-4" />
                  <span>Submit Ray Job</span>
                </button>
              </div>
            </div>
          </div>

          {/* Job History / Active Job Tracking list */}
          <div className="bg-odyn-card border border-gray-800 rounded-xl p-5 space-y-4">
            <div className="flex items-center justify-between border-b border-gray-800 pb-2">
              <h3 className="font-semibold text-white flex items-center gap-2">
                <Terminal className="h-4 w-4 text-odyn-teal" />
                <span>Job History & Real-Time Tracking</span>
              </h3>
              <span className="text-xs text-odyn-gray">Session scoped only</span>
            </div>

            {jobs.length === 0 ? (
              <div className="text-center py-8 text-xs text-gray-500 font-mono">
                No jobs submitted in current session.
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-left text-xs font-mono">
                  <thead>
                    <tr className="border-b border-gray-800 text-odyn-gray uppercase text-[10px] font-bold">
                      <th className="py-2">Job ID</th>
                      <th className="py-2">Type</th>
                      <th className="py-2">Status</th>
                      <th className="py-2">Submitted</th>
                      <th className="py-2">Details</th>
                      <th className="py-2">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {jobs.map((job) => (
                      <tr key={job.id} className="border-b border-gray-800/50 hover:bg-gray-800/10 transition-colors">
                        <td className="py-3 text-white truncate max-w-[120px]" title={job.id}>{job.id}</td>
                        <td className="py-3 uppercase text-odyn-gray">{job.type.replace("_", " ")}</td>
                        <td className="py-3">
                          <span className={`px-2 py-0.5 rounded text-[10px] font-bold uppercase ${
                            ["SUCCEEDED", "COMPLETED"].includes(job.status) ? "bg-odyn-green/10 text-odyn-green" : 
                            ["RUNNING", "PENDING"].includes(job.status) ? "bg-odyn-teal/10 text-odyn-teal animate-pulse" : 
                            "bg-odyn-red/10 text-odyn-red"
                          }`}>{job.status}</span>
                        </td>
                        <td className="py-3 text-gray-400">{job.submittedAt}</td>
                        <td className="py-3 text-gray-300 max-w-[160px] truncate" title={job.details}>{job.details}</td>
                        <td className="py-3">
                          {job.type === "ray_job" ? (
                            <button
                              onClick={() => fetchJobLogs(job.id)}
                              className="text-[10px] font-bold bg-odyn-teal/10 hover:bg-odyn-teal/20 text-odyn-teal border border-odyn-teal/30 px-2.5 py-1 rounded"
                            >
                              View Logs
                            </button>
                          ) : (
                            <span className="text-gray-600">-</span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>

        {/* Right Column: Node Observability Panel */}
        <div className="space-y-6">
          <div className="bg-odyn-card border border-gray-800 rounded-xl p-5 space-y-4 h-fit">
            <div className="flex items-center justify-between border-b border-gray-800 pb-2">
              <h3 className="font-semibold text-white flex items-center gap-2">
                <Server className="h-4 w-4 text-odyn-teal" />
                <span>Heterogeneous Hardware Status</span>
              </h3>
              <RefreshCw className="h-3 w-3 text-odyn-teal animate-spin" />
            </div>

            {nodePollingError && (
              <div className="text-xs text-odyn-red bg-odyn-red/5 border border-odyn-red/10 p-2 rounded">
                {nodePollingError}
              </div>
            )}

            <div className="space-y-5">
              {nodes.length === 0 && (
                <div className="text-[11px] text-odyn-gray">No telemetry yet. Provide an API key to poll /v1/nodes.</div>
              )}

              {nodes.length > 0 && (nodes[0].queue_depth ?? 0) > 0 && (
                <div className="text-[11px] text-yellow-300 bg-yellow-900/10 border border-yellow-600/30 rounded p-2 font-mono">
                  {nodes[0].queue_depth} workload{(nodes[0].queue_depth ?? 0) > 1 ? "s" : ""} queued awaiting node capacity
                </div>
              )}

              {nodes.map((node) => {
                const isOffline = node.status === "OFFLINE" || node.status === "FAILED";
                const isRespawning = node.status === "RESPAWNING" || node.status === "WARMING";
                const isRadeon = node.name.includes("Radeon") || node.name.includes("radeon");
                const ramPercent = node.memory_total_mib > 0 ? (node.memory_used_mib / node.memory_total_mib) * 100 : 0;
                const [title, subtitle] = node.name.includes(" — ")
                  ? node.name.split(" — ")
                  : [node.name, (node.raw_status ?? node.status).toUpperCase()];
                const slaTarget = node.sla_p95_target_ms ?? 0;
                const slaBreached = node.sla_ok === false;

                const cardTheme = isOffline
                  ? "bg-odyn-red/5 border-odyn-red/20"
                  : isRespawning
                    ? "bg-yellow-900/10 border-yellow-600/30"
                    : "bg-odyn-dark border-gray-800";

                const badgeTheme = isOffline
                  ? "bg-odyn-red/10 text-odyn-red"
                  : isRespawning
                    ? "bg-yellow-900/20 text-yellow-300"
                    : "bg-odyn-green/10 text-odyn-green";

                return (
                  <div 
                    key={node.name} 
                    className={`border p-4 rounded-xl space-y-3 transition-colors ${cardTheme}`}
                  >
                    <div className="flex items-start justify-between">
                      <div>
                        <h4 className="text-sm font-bold text-white">{title}</h4>
                        <span className="text-[10px] text-odyn-gray font-semibold uppercase">{subtitle}</span>
                      </div>
                      <div className="flex flex-col items-end gap-1">
                        <span className={`text-[9px] font-bold px-2 py-0.5 rounded-full ${badgeTheme}`}>{node.status}</span>
                        {node.role && node.role !== "unmanaged" && (
                          <span className={`text-[9px] font-bold px-2 py-0.5 rounded-full uppercase ${
                            node.role === "active" ? "bg-odyn-teal/10 text-odyn-teal" :
                            node.role === "standby" ? "bg-gray-800 text-odyn-gray" :
                            "bg-odyn-red/10 text-odyn-red"
                          }`}>{node.role}</span>
                        )}
                      </div>
                    </div>

                    {/* SLA status */}
                    {slaTarget > 0 && (
                      <div className={`flex justify-between items-center text-[10px] font-mono rounded p-2 border ${
                        slaBreached
                          ? "text-odyn-red bg-odyn-red/10 border-odyn-red/10"
                          : "text-odyn-green bg-odyn-green/5 border-odyn-green/10"
                      }`}>
                        <span>SLA p95 ≤ {(slaTarget / 1000).toFixed(1)}s</span>
                        <span className="font-bold">{slaBreached ? "BREACHED" : "WITHIN SLA"}</span>
                      </div>
                    )}

                    {/* Progress bars */}
                    <div className="space-y-2 text-xs font-mono">
                      {/* GPU Util */}
                      <div className="space-y-1">
                        <div className="flex justify-between text-[10px]">
                          <span className="text-odyn-gray">GPU Utilisation</span>
                          <span className="text-white">{isOffline ? "0.0" : node.gpu_utilisation.toFixed(1)}%</span>
                        </div>
                        <div className="w-full bg-gray-800 h-1.5 rounded-full overflow-hidden">
                          <div 
                            className="bg-odyn-teal h-full transition-all duration-500" 
                            style={{ width: `${isOffline ? 0 : node.gpu_utilisation}%` }}
                          ></div>
                        </div>
                      </div>

                      {/* Memory/GRAM Util */}
                      <div className="space-y-1">
                        <div className="flex justify-between text-[10px]">
                          <span className="text-odyn-gray">{isRadeon ? "GRAM Alloc" : "System RAM Alloc"}</span>
                          <span className="text-white">
                            {isOffline ? "0" : (node.memory_used_mib / 1024).toFixed(1)} / {(node.memory_total_mib / 1024).toFixed(0)} GB
                          </span>
                        </div>
                        <div className="w-full bg-gray-800 h-1.5 rounded-full overflow-hidden">
                          <div 
                            className="bg-odyn-green h-full transition-all duration-500" 
                            style={{ width: `${isOffline ? 0 : ramPercent}%` }}
                          ></div>
                        </div>
                      </div>
                    </div>

                    <div className="grid grid-cols-2 gap-2 text-[10px] font-mono text-odyn-gray">
                      <div>
                        <span className="text-white block">Jobs</span>
                        {node.jobs_total} total / {node.jobs_failed} failed
                      </div>
                      <div>
                        <span className="text-white block">Respawns</span>
                        {node.respawns} · inflight {node.inflight}
                      </div>
                      <div>
                        <span className="text-white block">Latency (ms)</span>
                        p50 {node.latency_p50_ms.toFixed(1)} · p95 {node.latency_p95_ms.toFixed(1)} · p99 {node.latency_p99_ms.toFixed(1)}
                      </div>
                      <div>
                        <span className="text-white block">Last Check</span>
                        {node.last_check_iso}
                      </div>
                    </div>

                    {node.last_error && (
                      <div className="text-[10px] text-odyn-red bg-odyn-red/10 border border-odyn-red/10 rounded p-2">
                        Last error: {node.last_error}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      </main>

      {/* Ray Job logs modal block */}
      {selectedLogsJobId && (
        <div className="fixed inset-0 bg-black/80 backdrop-blur-sm flex items-center justify-center p-4 z-50 animate-fadeIn">
          <div className="bg-odyn-card border border-gray-800 rounded-xl max-w-3xl w-full flex flex-col max-h-[80vh] shadow-2xl">
            <div className="border-b border-gray-800 px-6 py-4 flex items-center justify-between">
              <div>
                <h3 className="font-bold text-white flex items-center gap-2">
                  <Terminal className="h-4 w-4 text-odyn-teal" />
                  <span>Ray Job Log Console</span>
                </h3>
                <span className="text-xs text-odyn-gray">Job ID: {selectedLogsJobId}</span>
              </div>
              <button
                onClick={() => { setSelectedLogsJobId(null); setSelectedLogs(null); }}
                className="text-odyn-gray hover:text-white font-semibold text-sm"
              >
                Close (ESC)
              </button>
            </div>

            <div className="flex-1 overflow-y-auto p-6 bg-odyn-dark text-xs font-mono text-gray-300 whitespace-pre-wrap leading-relaxed">
              {logsLoading ? (
                <div className="flex flex-col items-center justify-center py-12 gap-3">
                  <RefreshCw className="h-6 w-6 text-odyn-teal animate-spin" />
                  <span className="text-odyn-gray">Fetching job log streams...</span>
                </div>
              ) : selectedJobLogs ? (
                selectedJobLogs
              ) : (
                <span className="text-gray-600">No log streams returned for this job.</span>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default App;
