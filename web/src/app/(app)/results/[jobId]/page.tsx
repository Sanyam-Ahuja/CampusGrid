"use client";

import { useSession } from "next-auth/react";
import { useParams } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import { CampuGridAPI } from "@/lib/api";
import { ArrowLeft, CheckCircle2, Download, FileText, CheckCircle, Zap, Clock, CreditCard, HardDrive, Play, Monitor } from "lucide-react";
import Link from "next/link";
import { format } from "date-fns";
import { motion } from "motion/react";

export default function ResultsPage() {
  const params = useParams();
  const jobId = params.jobId as string;
  const { data: session } = useSession();

  const api = new CampuGridAPI(session?.backend_jwt || "");

  const { data: job, isLoading } = useQuery({
    queryKey: ["job-results-detail", jobId],
    queryFn: () => api.getJob(jobId),
    enabled: !!session?.backend_jwt && !!jobId,
  });

  if (isLoading || !job) {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <div className="text-center">
          <div className="w-16 h-16 border-4 border-emerald-500/20 border-t-emerald-500 rounded-full animate-spin mx-auto mb-4" />
          <p className="text-slate-500 font-mono text-sm tracking-widest uppercase">Fetching Artifact Metadata...</p>
        </div>
      </div>
    );
  }

  const totalChunks = job.chunks?.length || 0;
  const completedChunks = job.chunks?.filter((c: any) => c.status === "completed").length || 0;
  const totalGpuHours = job.chunks?.reduce((sum: number, c: any) => sum + (c.gpu_hours || 0), 0) || 0;
  const totalCost = job.actual_cost || job.chunks?.reduce((sum: number, c: any) => sum + (c.cost || 0), 0) || 0;

  const startTimes = job.chunks?.map((c: any) => c.started_at).filter(Boolean).map((t: string) => new Date(t).getTime()) || [];
  const endTimes = job.chunks?.map((c: any) => c.completed_at).filter(Boolean).map((t: string) => new Date(t).getTime()) || [];
  const wallClockMs = startTimes.length > 0 && endTimes.length > 0 ? Math.max(...endTimes) - Math.min(...startTimes) : 0;
  const wallClockStr = wallClockMs > 0
    ? `${Math.floor(wallClockMs / 3600000).toString().padStart(2, "0")}:${Math.floor((wallClockMs % 3600000) / 60000).toString().padStart(2, "0")}:${Math.floor((wallClockMs % 60000) / 1000).toString().padStart(2, "0")}`
    : "—";

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
      </div>

      <div className="relative z-10 mb-8">
        <Link href="/results" className="flex items-center gap-2 text-slate-500 hover:text-emerald-400 transition-colors mb-4 group w-fit">
          <ArrowLeft size={16} className="group-hover:-translate-x-1 transition-transform" />
          <span className="text-sm font-bold uppercase tracking-widest">Back to Results</span>
        </Link>
        
        <div className="flex justify-between items-center bg-slate-900/30 backdrop-blur-xl border border-slate-800 p-8 rounded-3xl group transition-all hover:border-emerald-500/30">
          <div className="flex items-center gap-6">
             <div className="w-16 h-16 bg-emerald-500/20 rounded-2xl flex items-center justify-center text-emerald-400 shadow-lg shadow-emerald-500/20">
                <CheckCircle2 size={32} />
             </div>
             <div>
                <h1 className="text-3xl font-black text-white">Execution Complete</h1>
                <p className="text-slate-400 font-mono text-sm mt-1">
                  Job {job.id.substring(0, 8)} • {job.updated_at ? format(new Date(job.updated_at), "MMM d, h:mm a") : "—"}
                </p>
             </div>
          </div>
          
          {job.presigned_url ? (
            <a href={job.presigned_url} target="_blank" rel="noopener noreferrer">
              <button className="flex items-center gap-3 bg-emerald-500 text-white font-black py-4 px-10 rounded-2xl hover:bg-emerald-400 transition-all shadow-xl shadow-emerald-500/20 scale-100 hover:scale-105 active:scale-95">
                <Download size={20} />
                Download Output
              </button>
            </a>
          ) : (
            <button disabled className="flex items-center gap-3 bg-slate-800 text-slate-500 font-black py-4 px-10 rounded-2xl cursor-not-allowed opacity-50">
              Artifacts Finalizing...
            </button>
          )}
        </div>
      </div>

      <div className="relative z-10 grid grid-cols-1 lg:grid-cols-3 gap-8">
        {/* Preview Container */}
        <div className="lg:col-span-2 space-y-6">
          <div className="bg-slate-950/50 rounded-3xl border border-slate-800 overflow-hidden shadow-2xl relative aspect-video flex items-center justify-center group">
             {job.presigned_url ? (
                <>
                  {job.output_path?.endsWith(".mp4") ? (
                    <video 
                      src={job.presigned_url} 
                      controls 
                      autoPlay 
                      loop 
                      className="w-full h-full object-contain"
                    />
                  ) : job.output_path?.match(/\.(png|jpg|jpeg|webp)$/i) ? (
                    <img 
                      src={job.presigned_url} 
                      alt="Output" 
                      className="w-full h-full object-contain"
                    />
                  ) : (
                    <div className="text-center space-y-4">
                       <FileText size={64} className="text-emerald-500/50 mx-auto" />
                       <p className="text-slate-400 font-mono text-sm max-w-xs">{job.output_path || "Archive download ready"}</p>
                    </div>
                  )}
                </>
             ) : (
                <div className="text-center space-y-6">
                   <div className="w-20 h-20 bg-emerald-500/10 rounded-full flex items-center justify-center mx-auto border border-emerald-500/20">
                      <Zap size={32} className="text-emerald-500 animate-pulse" />
                   </div>
                   <div>
                      <h3 className="text-white font-bold text-xl">Assembling Artifacts</h3>
                      <p className="text-slate-500 text-sm mt-1 max-w-sm">Completed chunks are being merged into the final output. This usually takes a few seconds.</p>
                   </div>
                </div>
             )}
          </div>
        </div>

        {/* Execution Summary */}
        <div className="space-y-6">
           <div className="bg-slate-900/30 backdrop-blur-xl border border-slate-800 rounded-3xl p-8">
              <h2 className="text-xl font-bold text-white mb-6 uppercase tracking-widest text-[10px]">Workload Summary</h2>
              
              <div className="space-y-6">
                 <div className="flex justify-between items-center">
                    <span className="text-slate-500 font-bold uppercase tracking-widest text-[10px]">Total Cost</span>
                    <span className="text-2xl font-black text-white">${(totalCost).toFixed(2)}</span>
                 </div>
                 
                 <div className="h-px bg-slate-800" />
                 
                 <div className="grid grid-cols-2 gap-4">
                    <SummaryItem label="Wall Clock" value={wallClockStr} icon={<Clock size={12} />} />
                    <SummaryItem label="GPU Hours" value={`${totalGpuHours.toFixed(1)}h`} icon={<Zap size={12} />} />
                    <SummaryItem label="Chunks" value={`${completedChunks}/${totalChunks}`} icon={<HardDrive size={12} />} />
                    <SummaryItem label="Success Rate" value="100%" icon={<CheckCircle size={12} />} />
                 </div>

                 <div className="h-px bg-slate-800" />

                 <div className="space-y-3">
                    <div className="flex justify-between text-[10px] font-black text-slate-500 uppercase tracking-widest">
                       <span>Grid Economy Savings</span>
                       <span className="text-emerald-400">84% SAVED</span>
                    </div>
                    <div className="w-full h-1.5 bg-slate-800 rounded-full overflow-hidden">
                       <div className="h-full bg-emerald-500 w-[84%]" />
                    </div>
                 </div>
              </div>
           </div>

           <div className="bg-slate-900/30 backdrop-blur-xl border border-slate-800 rounded-3xl p-6">
              <button className="w-full group flex items-center justify-between p-4 bg-white/5 border border-white/5 rounded-2xl hover:bg-emerald-500 transition-all">
                 <div className="flex items-center gap-3">
                    <Terminal size={18} className="text-emerald-400 group-hover:text-white" />
                    <span className="text-sm font-bold text-white">Download Logs</span>
                 </div>
                 <Download size={16} className="text-slate-500 group-hover:text-white" />
              </button>
           </div>
        </div>
      </div>
    </div>
  );
}

function SummaryItem({ label, value, icon }: any) {
  return (
    <div>
       <div className="flex items-center gap-1.5 text-[10px] text-slate-500 font-bold uppercase tracking-widest mb-1">
          {icon}
          {label}
       </div>
       <div className="text-white font-black">{value}</div>
    </div>
  );
}

function Terminal({ size, className }: any) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={className}>
      <polyline points="4 17 10 11 4 5" />
      <line x1="12" y1="19" x2="20" y2="19" />
    </svg>
  );
}
