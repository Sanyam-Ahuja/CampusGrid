"use client";

import { useState, useCallback, useEffect } from "react";
import { useSession } from "next-auth/react";
import { useRouter } from "next/navigation";
import { useDropzone } from "react-dropzone";
import { motion, AnimatePresence } from "motion/react";
import { Upload, CheckCircle2, CircleDashed, File, XCircle, Shield, Cpu, Zap, HardDrive, X, Copy, CheckCircle } from "lucide-react";
import { CampuGridAPI } from "@/lib/api";
import { useJobStream } from "@/lib/ws";
import { useQuery } from "@tanstack/react-query";

type SubmitState = "idle" | "uploading" | "detecting" | "ready" | "needs_dockerfile" | "failed";

export default function SubmitPage() {
  const { data: session } = useSession();
  const router = useRouter();
  const [submitState, setSubmitState] = useState<SubmitState>("idle");
  const [jobId, setJobId] = useState<string | null>(null);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [jobProfile, setJobProfile] = useState<any>(null);
  const [requiresNetwork, setRequiresNetwork] = useState(false);
  const [pauseDetail, setPauseDetail] = useState<string>("");
  const [errorDetail, setErrorDetail] = useState<string>("");
  const [resolving, setResolving] = useState(false);

  const { messages } = useJobStream(jobId, session?.backend_jwt || "");

  const api = new CampuGridAPI(session?.backend_jwt || "");

  const { data: clusterStats } = useQuery({
    queryKey: ["cluster-stats-submit", session?.backend_jwt],
    queryFn: () => api.getClusterStats(),
    enabled: !!session?.backend_jwt,
  });

  const onDrop = useCallback(async (acceptedFiles: File[]) => {
    if (acceptedFiles.length === 0 || !session?.backend_jwt) return;

    setSubmitState("uploading");
    
    // Simulate upload progress
    const interval = setInterval(() => {
      setUploadProgress(p => Math.min(p + 10, 95));
    }, 150);

    try {
      const res = await api.submitJob(acceptedFiles, undefined, requiresNetwork);
      setJobId(res.job_id);
      clearInterval(interval);
      setUploadProgress(100);
      setTimeout(() => setSubmitState("detecting"), 500);
    } catch (e) {
      console.error(e);
      setSubmitState("idle");
      clearInterval(interval);
      setUploadProgress(0);
    }
  }, [session, requiresNetwork]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({ onDrop });

  useEffect(() => {
    if (messages.length === 0) return;
    const lastMsg = messages[messages.length - 1];

    if (lastMsg.type === "pipeline_complete" && lastMsg.profile) {
      setJobProfile(lastMsg.profile);
      setSubmitState("ready");
    } else if (lastMsg.type === "detection_step" && lastMsg.step === "needs_dockerfile") {
      // Pipeline paused — the user must supply a Dockerfile or authorize AI generation.
      setPauseDetail(lastMsg.detail || "Could not auto-detect this workload.");
      setSubmitState("needs_dockerfile");
    } else if (lastMsg.type === "detection_step" && lastMsg.step === "failed") {
      setErrorDetail(lastMsg.detail || "Pipeline failed.");
      setSubmitState("failed");
    }
  }, [messages]);

  // Fallback REST API polling to escape "Connecting to pipeline..." if WebSocket fails or misses messages
  useEffect(() => {
    if (submitState !== "detecting" || !jobId || !session?.backend_jwt) return;

    let active = true;
    const interval = setInterval(async () => {
      try {
        const job = await api.getJob(jobId);
        if (!active) return;

        if (job.status === "needs_dockerfile") {
          setPauseDetail(job.profile?.error_details || "Dockerfile required for this custom workload.");
          setSubmitState("needs_dockerfile");
        } else if (job.status === "failed") {
          setErrorDetail(job.profile?.error_details || "AI Pipeline analysis failed.");
          setSubmitState("failed");
        } else if (job.status !== "analyzing") {
          setJobProfile(job.profile);
          setSubmitState("ready");
        }
      } catch (e) {
        console.error("Pipeline fallback polling error:", e);
      }
    }, 2000);

    return () => {
      active = false;
      clearInterval(interval);
    };
  }, [submitState, jobId, session?.backend_jwt, api]);

  const handleResolve = useCallback(async (opts: { dockerfile?: File; useAi?: boolean }) => {
    if (!jobId) return;
    setResolving(true);
    try {
      await api.resolveDockerfile(jobId, opts);
      // Clear consumed messages by re-entering detecting state.
      setSubmitState("detecting");
    } catch (e) {
      console.error(e);
      setErrorDetail("Failed to submit resolution. Please try again.");
      setSubmitState("failed");
    } finally {
      setResolving(false);
    }
  }, [jobId, api]);

  const handleLaunch = () => {
    if (jobId) {
      router.push(`/monitor/${jobId}`);
    }
  };

  return (
    <div className="p-8 relative min-h-screen">
      {/* 3D Background Graphics */}
      <div className="absolute inset-0 pointer-events-none overflow-hidden z-0">
        <motion.div
          className="absolute top-28 right-24 w-44 h-44 border-2 border-emerald-500/40 rounded-full shadow-lg shadow-emerald-500/25"
          animate={{ rotateX: [0, 180, 360], rotateY: [0, 360] }}
          transition={{ duration: 16, repeat: Infinity, ease: "linear" }}
          style={{ transform: "perspective(1000px)", transformStyle: "preserve-3d" }}
        />
        <motion.div
           className="absolute bottom-28 right-56 w-38 h-38 border-2 border-emerald-500/20 rounded-lg"
           animate={{ rotateZ: [0, 360] }}
           transition={{ duration: 24, repeat: Infinity, ease: "linear" }}
           style={{ transform: "perspective(1000px)", transformStyle: "preserve-3d" }}
        />
      </div>

      <div className="relative z-10 mb-8">
        <h1 className="text-3xl font-bold text-white mb-2">Submit New Job</h1>
        <p className="text-slate-400">Configure and submit your compute job to the network.</p>
      </div>

      <div className="relative z-10 grid grid-cols-1 lg:grid-cols-3 gap-8">
        {/* Main Area */}
        <div className="lg:col-span-2 space-y-6">
          <AnimatePresence mode="wait">
            {submitState === "idle" && (
              <motion.div
                key="dropzone"
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, scale: 0.95 }}
                className="bg-slate-900/30 backdrop-blur-xl border border-slate-800 rounded-xl p-8"
              >
                <div
                  {...getRootProps()}
                  className={`border-2 border-dashed rounded-lg p-20 text-center transition-all cursor-pointer ${
                    isDragActive ? "border-emerald-500 bg-emerald-500/5" : "border-slate-700 hover:border-emerald-500/50"
                  }`}
                >
                  <input {...getInputProps()} />
                  <Upload className={`w-16 h-16 mx-auto mb-6 transition-colors ${isDragActive ? "text-emerald-400" : "text-slate-600"}`} />
                  <h3 className="text-2xl font-bold text-white mb-3">
                    {isDragActive ? "Release to upload" : "Drop your files here"}
                  </h3>
                  <p className="text-slate-400 max-w-sm mx-auto mb-8">
                    Upload .blend, .py, .ipynb or archives (.zip, .tar.gz). Our pipeline will automatically detect the best hardware.
                  </p>
                  
                  <div className="flex justify-center gap-4">
                    <label className="flex items-center gap-3 cursor-pointer py-2 px-4 bg-slate-800/50 rounded-lg border border-slate-700 hover:bg-slate-800 transition-colors" onClick={e => e.stopPropagation()}>
                      <input 
                        type="checkbox" 
                        checked={requiresNetwork} 
                        onChange={e => setRequiresNetwork(e.target.checked)}
                        className="w-4 h-4 rounded accent-emerald-500" 
                      />
                      <span className="text-sm text-slate-300">Public Internet Required</span>
                    </label>
                  </div>
                </div>
              </motion.div>
            )}

            {(submitState === "uploading" || submitState === "detecting") && (
              <motion.div
                key="processing"
                initial={{ opacity: 0, scale: 0.95 }}
                animate={{ opacity: 1, scale: 1 }}
                exit={{ opacity: 0, scale: 1.05 }}
                className="bg-slate-900/30 backdrop-blur-xl border border-slate-800 rounded-xl p-10"
              >
                <div className="flex flex-col items-center mb-10 text-center">
                  <div className="relative w-24 h-24 mb-6 flex items-center justify-center">
                    <div className="absolute inset-0 border-4 border-white/5 rounded-full" />
                    <motion.div 
                      className="absolute inset-0 border-4 border-emerald-500 rounded-full border-t-transparent"
                      animate={{ rotate: 360 }}
                      transition={{ duration: 1.5, repeat: Infinity, ease: "linear" }}
                    />
                    <Cpu className="w-10 h-10 text-emerald-400" />
                  </div>
                  <h2 className="text-2xl font-bold text-white mb-2">
                    {submitState === "uploading" ? "Broadcasting to Network..." : "AI Detection Active"}
                  </h2>
                  <p className="text-slate-400">
                    {submitState === "uploading" 
                      ? `Uploading artifacts: ${uploadProgress}%` 
                      : "Decoding workload type and topology..."}
                  </p>
                </div>

                <div className="space-y-3 bg-black/20 p-6 rounded-lg border border-slate-800 font-mono text-sm">
                   {messages.filter(m => m.type === "detection_step").map((step, idx, arr) => (
                     <div key={idx} className="flex items-start gap-3">
                        {idx === arr.length - 1 ? (
                          <CircleDashed className="animate-spin text-emerald-500 mt-1 flex-shrink-0" size={14} />
                        ) : (
                          <CheckCircle2 className="text-green-500 mt-1 flex-shrink-0" size={14} />
                        )}
                        <span className={idx === arr.length - 1 ? "text-white" : "text-slate-500"}>
                          {step.detail || step.step}
                        </span>
                     </div>
                   ))}
                   {messages.length === 0 && (
                      <div className="text-slate-600 italic animate-pulse font-sans">Connecting to pipeline...</div>
                   )}
                </div>
              </motion.div>
            )}

            {submitState === "ready" && jobProfile && (
              <motion.div
                key="ready"
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                className="bg-slate-900/30 backdrop-blur-xl border border-slate-800 rounded-xl p-8"
              >
                <div className="flex items-center justify-between mb-8 pb-6 border-b border-slate-800">
                  <div>
                    <h2 className="text-2xl font-bold text-white flex items-center gap-3">
                      <CheckCircle2 className="text-green-500" size={32} />
                      Analysis Complete
                    </h2>
                    <p className="text-slate-400 mt-1">Found optimal hardware configuration.</p>
                  </div>
                  <div className="bg-green-500/10 border border-green-500/20 px-4 py-2 rounded-lg text-center">
                    <div className="text-[10px] font-bold text-green-500 uppercase tracking-widest">Confidence</div>
                    <div className="text-2xl font-black text-green-500">{(jobProfile.confidence * 100).toFixed(0)}%</div>
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-8 mb-8">
                  <div className="space-y-4">
                     <div>
                        <div className="text-xs font-bold text-slate-500 uppercase tracking-widest mb-1">Workload</div>
                        <div className="text-xl font-bold text-white capitalize">{jobProfile.type.replace("_", " ")}</div>
                     </div>
                     <div>
                        <div className="text-xs font-bold text-slate-500 uppercase tracking-widest mb-1">Framework</div>
                        <div className="text-xl font-bold text-white capitalize">{jobProfile.framework || "Automatic"}</div>
                     </div>
                  </div>
                  <div className="space-y-4">
                     <div>
                        <div className="text-xs font-bold text-slate-500 uppercase tracking-widest mb-1">Main Artifact</div>
                        <div className="text-xl font-bold text-emerald-400 flex items-center gap-2">
                           <File size={20} />
                           {jobProfile.entry_file}
                        </div>
                     </div>
                     <div>
                        <div className="text-xs font-bold text-slate-500 uppercase tracking-widest mb-1">Resources</div>
                        <div className="flex gap-2 mt-1">
                          {jobProfile.gpu_required && <span className="px-2 py-1 bg-emerald-500 text-white text-[10px] font-bold rounded">GPU</span>}
                          <span className="px-2 py-1 bg-slate-800 text-slate-300 text-[10px] font-bold rounded">{jobProfile.resources.cpu_cores} vCPU</span>
                          <span className="px-2 py-1 bg-slate-800 text-slate-300 text-[10px] font-bold rounded">{jobProfile.resources.ram_gb}GB RAM</span>
                        </div>
                     </div>
                  </div>
                </div>

                <div className="flex gap-4">
                  <button onClick={() => setSubmitState("idle")} className="flex-1 py-4 bg-slate-800 text-slate-300 rounded-lg font-bold hover:bg-slate-700 transition-colors">
                    Back
                  </button>
                  <button onClick={handleLaunch} className="flex-[2] py-4 bg-emerald-500 text-white rounded-lg font-black hover:bg-emerald-400 transition-colors shadow-lg shadow-emerald-500/30 flex items-center justify-center gap-2">
                    🚀 Launch Workload
                  </button>
                </div>
              </motion.div>
            )}

            {submitState === "needs_dockerfile" && (
              <motion.div
                key="needs_dockerfile"
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                className="bg-slate-900/30 backdrop-blur-xl border border-yellow-500/30 rounded-xl p-8"
              >
                <div className="flex items-center gap-3 mb-4">
                  <Shield className="text-yellow-400" size={28} />
                  <h2 className="text-2xl font-bold text-white">Action Needed</h2>
                </div>
                <p className="text-slate-300 whitespace-pre-line mb-6">{pauseDetail}</p>

                <div className="space-y-4">
                  <label className="block">
                    <span className="text-xs font-bold text-slate-500 uppercase tracking-widest">Upload a Dockerfile</span>
                    <input
                      type="file"
                      disabled={resolving}
                      onChange={(e) => {
                        const f = e.target.files?.[0];
                        if (f) handleResolve({ dockerfile: f });
                      }}
                      className="mt-2 block w-full text-sm text-slate-400 file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border-0 file:bg-slate-800 file:text-slate-200 hover:file:bg-slate-700"
                    />
                  </label>

                  <div className="flex items-center gap-3">
                    <div className="flex-1 h-px bg-slate-800" />
                    <span className="text-xs text-slate-600 uppercase">or</span>
                    <div className="flex-1 h-px bg-slate-800" />
                  </div>

                  <button
                    disabled={resolving}
                    onClick={() => handleResolve({ useAi: true })}
                    className="w-full py-4 bg-emerald-500 text-white rounded-lg font-black hover:bg-emerald-400 transition-colors disabled:opacity-50"
                  >
                    {resolving ? "Submitting…" : "🤖 Authorize AI to configure it"}
                  </button>
                  <button
                    disabled={resolving}
                    onClick={() => setSubmitState("idle")}
                    className="w-full py-3 bg-slate-800 text-slate-300 rounded-lg font-bold hover:bg-slate-700 transition-colors"
                  >
                    Cancel
                  </button>
                </div>
              </motion.div>
            )}

            {submitState === "failed" && (
              <motion.div
                key="failed"
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                className="bg-slate-900/30 backdrop-blur-xl border border-red-500/30 rounded-xl p-8"
              >
                <div className="flex items-center gap-3 mb-4">
                  <XCircle className="text-red-400" size={28} />
                  <h2 className="text-2xl font-bold text-white">Job Failed</h2>
                </div>
                <p className="text-slate-300 whitespace-pre-line mb-6">{errorDetail}</p>
                <button
                  onClick={() => { setSubmitState("idle"); setUploadProgress(0); }}
                  className="w-full py-4 bg-slate-800 text-slate-300 rounded-lg font-bold hover:bg-slate-700 transition-colors"
                >
                  Start Over
                </button>
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        {/* Sidebar */}
        <div className="space-y-6">
          <div className="bg-slate-900/30 backdrop-blur-xl border border-slate-800 rounded-xl p-6">
            <h3 className="text-lg font-bold text-white mb-4">Network Status</h3>
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2 text-slate-400 text-sm">
                  <Cpu size={18} className="text-emerald-400" />
                  Online Nodes
                </div>
                <span className="text-white font-bold">{clusterStats?.active_nodes ?? 0}</span>
              </div>
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2 text-slate-400 text-sm">
                  <Zap size={18} className="text-yellow-400" />
                  Jobs Today
                </div>
                <span className="text-white font-bold">{clusterStats?.jobs_completed_today ?? 0}</span>
              </div>
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2 text-slate-400 text-sm">
                  <HardDrive size={18} className="text-purple-400" />
                  GPU Hours Today
                </div>
                <span className="text-white font-bold">{(clusterStats?.total_gpu_hours_today ?? 0).toFixed?.(1) ?? 0}</span>
              </div>
            </div>
          </div>

          <div className="bg-gradient-to-br from-emerald-500/20 to-emerald-500/5 border border-emerald-500/30 rounded-xl p-6">
            <h3 className="text-lg font-bold text-white mb-2">Estimated Saving</h3>
            <div className="text-4xl font-black text-white">~85%</div>
            <p className="text-emerald-400 text-xs mt-1">Compared to hyperscalers</p>
          </div>
        </div>
      </div>
    </div>
  );
}
