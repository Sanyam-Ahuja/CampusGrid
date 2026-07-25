"use client";

import { useSession } from "next-auth/react";
import { useParams, useRouter } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import { CampuGridAPI } from "@/lib/api";
import { useJobStream } from "@/lib/ws";
import { motion, AnimatePresence } from "motion/react";
import { Activity, CheckCircle2, AlertTriangle, ArrowLeft, Cpu, Thermometer, Battery, Terminal, HardDrive, Zap, Clock } from "lucide-react";
import Link from "next/link";
import { useState, useEffect } from "react";

export default function MonitorPage() {
  const params = useParams();
  const jobId = params.jobId as string;
  const { data: session } = useSession();
  const router = useRouter();

  const api = new CampuGridAPI(session?.backend_jwt || "");

  const { data: jobInfo, isLoading } = useQuery({
    queryKey: ["job-detail", jobId],
    queryFn: () => api.getJob(jobId),
    enabled: !!session?.backend_jwt && !!jobId,
    refetchInterval: 5000,
  });

  const { messages } = useJobStream(jobId, session?.backend_jwt || "");
  
  const [liveChunks, setLiveChunks] = useState<any[]>([]);
  const [jobStatus, setJobStatus] = useState<string>("queued");
  const [logs, setLogs] = useState<any[]>([]);

  useEffect(() => {
    if (jobInfo?.chunks) {
      setLiveChunks([...jobInfo.chunks].sort((a, b) => a.chunk_index - b.chunk_index));
      setJobStatus(jobInfo.status);
    }
  }, [jobInfo]);

  useEffect(() => {
    if (messages.length > 0) {
      const lastMsg = messages[messages.length - 1];
      
      if (lastMsg.type === "chunk_status") {
        setLiveChunks(prev => prev.map(c => 
          c.id === lastMsg.chunk_id 
            ? { ...c, status: lastMsg.status, progress: lastMsg.progress || c.progress, telemetry: lastMsg.telemetry } 
            : c
        ));
      } else if (lastMsg.type === "job_complete") {
        setJobStatus("completed");
        setTimeout(() => router.push(`/results/${jobId}`), 3000);
      } else if (lastMsg.type === "log") {
        setLogs(prev => [...prev, lastMsg].slice(-50));
      }
    }
  }, [messages, jobId, router]);

  if (isLoading || !jobInfo) {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <div className="text-center">
          <div className="w-16 h-16 border-4 border-emerald-500/20 border-t-emerald-500 rounded-full animate-spin mx-auto mb-4" />
          <p className="text-slate-500 font-mono text-sm tracking-widest uppercase">Initializing Telemetry...</p>
        </div>
      </div>
    );
  }

  const completedChunks = liveChunks.filter(c => c.status === "completed").length;
  const progressPercent = liveChunks.length > 0 ? (completedChunks / liveChunks.length) * 100 : 0;
  
  // Aggregate telemetry from active chunks
  const activeChunks = liveChunks.filter(c => c.status === "running");
  const avgGpuLoad = activeChunks.length > 0 
    ? activeChunks.reduce((acc, c) => acc + (c.telemetry?.gpu_load || 0), 0) / activeChunks.length 
    : 0;

  return (
    <div className="p-8 relative min-h-screen">
      {/* 3D Background Graphics */}
      <div className="absolute inset-0 pointer-events-none overflow-hidden z-0">
        <motion.div
          className="absolute top-32 right-20 w-48 h-48 border-2 border-emerald-500/30 rounded-full shadow-lg shadow-emerald-500/20"
          animate={{ rotateY: [0, 360], scale: [1, 1.15, 1] }}
          transition={{ duration: 18, repeat: Infinity, ease: "linear" }}
          style={{ transform: "perspective(1000px)", transformStyle: "preserve-3d" }}
        />
        <motion.div
           className="absolute bottom-20 right-40 w-36 h-36 border-2 border-emerald-500/25 rounded-lg"
           animate={{ rotateZ: [0, 360], rotateX: [0, 180, 360] }}
           transition={{ duration: 22, repeat: Infinity, ease: "linear" }}
           style={{ transform: "perspective(1000px)", transformStyle: "preserve-3d" }}
        />
      </div>

      <div className="relative z-10 mb-8">
        <Link href="/monitor" className="flex items-center gap-2 text-slate-500 hover:text-emerald-400 transition-colors mb-4 group w-fit">
          <ArrowLeft size={16} className="group-hover:-translate-x-1 transition-transform" />
          <span className="text-sm font-bold uppercase tracking-widest">Back to Monitor</span>
        </Link>
        <div className="flex justify-between items-end">
          <div>
            <div className="flex items-center gap-3 mb-2">
              <h1 className="text-3xl font-bold text-white">Workload: {jobId.substring(0, 12)}...</h1>
              <span className={`px-3 py-1 rounded-full text-[10px] font-black uppercase tracking-widest border ${
                jobStatus === "running" ? "bg-emerald-500/20 text-emerald-400 border-emerald-500/30" :
                jobStatus === "completed" ? "bg-green-500/20 text-green-400 border-green-500/30" :
                "bg-yellow-500/20 text-yellow-400 border-yellow-500/30"
              }`}>
                {jobStatus}
              </span>
            </div>
            <p className="text-slate-400 font-mono text-sm">{jobInfo.input_path}</p>
          </div>
          <div className="text-right">
             <div className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1">Global Progress</div>
             <div className="text-3xl font-black text-white">{progressPercent.toFixed(1)}%</div>
          </div>
        </div>
      </div>

      {/* Live Stats Grid */}
      <div className="relative z-10 grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
        <StatCard
          icon={<Activity className="w-6 h-6" />}
          label="Status"
          value={jobStatus.charAt(0).toUpperCase() + jobStatus.slice(1)}
          iconBg="bg-emerald-500/20"
          iconColor="text-emerald-400"
        />
        <StatCard
          icon={<Cpu className="w-6 h-6" />}
          label="Avg GPU Load"
          value={`${avgGpuLoad.toFixed(0)}%`}
          iconBg="bg-emerald-500/20"
          iconColor="text-emerald-400"
        />
        <StatCard
          icon={<HardDrive className="w-6 h-6" />}
          label="Completed"
          value={`${completedChunks}/${liveChunks.length}`}
          iconBg="bg-emerald-500/20"
          iconColor="text-emerald-400"
        />
        <StatCard
          icon={<Clock className="w-6 h-6" />}
          label="Elapsed Time"
          value="2h 15m"
          iconBg="bg-emerald-500/20"
          iconColor="text-emerald-400"
        />
      </div>

      <div className="relative z-10 grid grid-cols-1 lg:grid-cols-3 gap-8">
        {/* Performance & Chunks */}
        <div className="lg:col-span-2 space-y-6">
          <motion.div
            className="bg-slate-900/30 backdrop-blur-xl border border-slate-800 rounded-xl p-6"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
          >
            <h2 className="text-xl font-bold text-white mb-6 flex items-center gap-2">
              <Zap className="text-emerald-500" size={20} />
              Resource Distribution
            </h2>
            
            <div className="space-y-4">
              {liveChunks.map((chunk) => (
                <div key={chunk.id} className="p-4 bg-slate-800/30 border border-slate-800 rounded-lg hover:border-emerald-500/30 transition-all">
                  <div className="flex items-center justify-between mb-3">
                    <div className="flex items-center gap-3">
                      <div className={`w-2 h-2 rounded-full ${
                        chunk.status === "running" ? "bg-emerald-500 animate-pulse" :
                        chunk.status === "completed" ? "bg-green-500" :
                        "bg-slate-700"
                      }`} />
                      <span className="text-xs font-bold text-white uppercase tracking-widest">Chunk #{chunk.chunk_index}</span>
                      <span className="text-[10px] text-slate-500 font-mono">Node ID: {chunk.node_id?.substring(0, 8) || "WAITING"}</span>
                    </div>
                    <span className="text-xs font-black text-emerald-400">{chunk.progress || 0}%</span>
                  </div>
                  <div className="w-full h-1.5 bg-slate-900 rounded-full overflow-hidden">
                    <motion.div
                      className="h-full bg-gradient-to-r from-emerald-500 to-emerald-400"
                      initial={{ width: "0%" }}
                      animate={{ width: `${chunk.progress || 0}%` }}
                      transition={{ duration: 1 }}
                    />
                  </div>
                  {chunk.status === "running" && chunk.telemetry && (
                    <div className="mt-3 flex gap-6 text-[10px] font-bold uppercase tracking-widest">
                       <div className="flex items-center gap-1.5 text-slate-400">
                          <Cpu size={12} className="text-emerald-500" />
                          GPU {chunk.telemetry.gpu_load}%
                       </div>
                       <div className="flex items-center gap-1.5 text-slate-400">
                          <Thermometer size={12} className="text-yellow-500" />
                          TEMP {chunk.telemetry.temp}°C
                       </div>
                    </div>
                  )}
                </div>
              ))}
            </div>
          </motion.div>
        </div>

        {/* Live Logs */}
        <div className="space-y-6">
          <motion.div
            className="bg-slate-900/30 backdrop-blur-xl border border-slate-800 rounded-xl p-6 h-[500px] flex flex-col"
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0 }}
          >
            <h2 className="text-xl font-bold text-white mb-6 flex items-center gap-2">
              <Terminal className="text-emerald-500" size={20} />
              Live Logs
            </h2>
            <div className="flex-1 bg-black/40 rounded-lg p-4 font-mono text-xs overflow-y-auto space-y-2 border border-slate-800 shadow-inner custom-scrollbar">
               {logs.length === 0 ? (
                 <div className="text-slate-700 italic animate-pulse">Waiting for telemetry heartbeat...</div>
               ) : (
                 logs.map((log, idx) => (
                   <div key={idx} className="flex gap-3">
                      <span className="text-slate-600 shrink-0">[{new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}]</span>
                      <span className={log.level === "error" ? "text-red-400" : "text-emerald-400/80"}>
                        {log.log || log.message}
                      </span>
                   </div>
                 ))
               )}
            </div>
          </motion.div>

          <div className="bg-gradient-to-br from-emerald-500/20 to-emerald-500/10 border border-emerald-500/30 rounded-xl p-6">
            <h3 className="text-lg font-bold text-white mb-2">Network Savings</h3>
            <div className="text-4xl font-black text-white">₹1,240</div>
            <p className="text-emerald-400 text-xs mt-1">Saved vs Cloud Providers</p>
          </div>
        </div>
      </div>
    </div>
  );
}

function StatCard({ icon, label, value, iconBg, iconColor }: any) {
  return (
    <motion.div
      className="bg-slate-900/30 backdrop-blur-xl border border-slate-800 rounded-xl p-6"
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5 }}
      whileHover={{ y: -4, boxShadow: "0 10px 30px rgba(0,0,0,0.3)" }}
    >
      <div className="flex items-center justify-between mb-4">
        <span className="text-slate-400 text-sm font-medium">{label}</span>
        <div className={`p-2 rounded-lg ${iconBg} ${iconColor}`}>
          {icon}
        </div>
      </div>
      <div className="text-3xl font-black text-white">{value}</div>
    </motion.div>
  );
}
