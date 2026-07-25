use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{StreamExt, SinkExt};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tauri::Emitter;
use tokio::sync::Mutex;

/// Read real GPU telemetry via nvidia-smi
fn read_gpu_telemetry() -> (i32, i32, i32) {
    if let Ok(output) = std::process::Command::new("nvidia-smi")
        .args(["--query-gpu=utilization.gpu,temperature.gpu,utilization.memory", "--format=csv,noheader,nounits"])
        .output()
    {
        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let parts: Vec<&str> = stdout.trim().split(", ").collect();
            if parts.len() >= 3 {
                let gpu_load = parts[0].parse::<i32>().unwrap_or(0);
                let temp = parts[1].parse::<i32>().unwrap_or(0);
                let vram = parts[2].parse::<i32>().unwrap_or(0);
                return (gpu_load, temp, vram);
            }
        }
    }
    (0, 0, 0)
}

type WsStream = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
type WsWriter = futures_util::stream::SplitSink<WsStream, Message>;

pub async fn connect_and_listen(app_handle: tauri::AppHandle, node_id: String, auth_token: String) {
    let base_url = option_env!("CAMPUGRID_WS_URL").unwrap_or("ws://localhost:8000");
    let url = format!("{}/api/v1/ws/node/{}?token={}", base_url, node_id, auth_token);

    use tauri::Manager;
    use std::sync::atomic::Ordering;

    // Decoupled channels to route all outgoing WebSocket messages safely.
    // This allows active tasks to queue messages even during network reconnects.
    let (tx_out, mut rx_out) = tokio::sync::mpsc::unbounded_channel::<Message>();

    // A shared pointer to the currently active connection's writer.
    let active_writer: Arc<Mutex<Option<Arc<Mutex<WsWriter>>>>> = Arc::new(Mutex::new(None));
    let active_writer_for_router = active_writer.clone();

    // Spawn a persistent task to forward queued messages to the currently active WebSocket.
    tokio::spawn(async move {
        while let Some(msg) = rx_out.recv().await {
            let opt_w = {
                let lock = active_writer_for_router.lock().await;
                lock.clone()
            };
            if let Some(w) = opt_w {
                let mut w_lock = w.lock().await;
                if let Err(e) = w_lock.send(msg).await {
                    println!("Error sending message through active WebSocket: {}", e);
                }
            }
        }
    });

    loop {
        // Break this background daemon task if the node logged out
        if let Some(state) = app_handle.try_state::<crate::AppState>() {
            if !state.is_logged_in.load(Ordering::SeqCst) {
                println!("Terminating WebSocket thread due to logout.");
                break;
            }
        }
        println!("Attempting to connect to {}", url);
        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                println!("WebSocket connected!");
                let _ = app_handle.emit("ws_status", json!({ "status": "connected" }));

                let (write, mut read) = ws_stream.split();
                let write_shared = Arc::new(Mutex::new(write));

                // Publish this connection's writer as active
                {
                    let mut lock = active_writer.lock().await;
                    *lock = Some(write_shared.clone());
                }

                // ── Heartbeat task ───────────────────────────────────────────────────
                let heartbeat_app = app_handle.clone();
                let heartbeat_node = node_id.clone();
                let heartbeat_tx = tx_out.clone();

                let heartbeat_handle = tokio::spawn(async move {
                    // Brief delay then send an immediate heartbeat so the server sees us fast
                    tokio::time::sleep(Duration::from_millis(300)).await;

                    loop {
                        // Terminate heartbeat loop if logged out
                        let mut available = true;
                        if let Some(state) = heartbeat_app.try_state::<crate::AppState>() {
                            if !state.is_logged_in.load(Ordering::SeqCst) {
                                break;
                            }
                            let active = state.is_active.load(Ordering::SeqCst);
                            let busy = state.is_busy.load(Ordering::SeqCst);
                            available = active && !busy;
                        }

                        let (gpu_load, temp, vram) = read_gpu_telemetry();

                        let hb = json!({
                            "type": "heartbeat",
                            "node_id": heartbeat_node,
                            "available": available,
                            "resources": {
                                "gpu_load": gpu_load,
                                "temp": temp,
                                "vram_percent": vram
                            }
                        });

                        if let Err(e) = heartbeat_tx.send(Message::Text(hb.to_string().into())) {
                            println!("Heartbeat send error: {}", e);
                            break;
                        }

                        // Also update the local Tauri UI
                        let _ = heartbeat_app.emit("telemetry", json!({
                            "gpu_load": gpu_load,
                            "temp": temp,
                            "vram_percent": vram
                        }));

                        tokio::time::sleep(Duration::from_secs(10)).await;
                    }
                });

                // ── Main inbound message loop ────────────────────────────────────────
                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(&text) {
                                let msg_type = json_val["type"].as_str().unwrap_or("");
                                match msg_type {
                                    "job_dispatch" | "chunk_dispatch" => {
                                        let _ = app_handle.emit("job_dispatch", json_val.clone());

                                        if let Some(state) = app_handle.try_state::<crate::AppState>() {
                                            state.is_busy.store(true, Ordering::SeqCst);
                                        }

                                        let chunk_id = json_val["chunk_id"]
                                            .as_str().unwrap_or("unknown").to_string();
                                        let job_id = json_val["job_id"]
                                            .as_str().unwrap_or("").to_string();
                                        let spec = json_val["spec"].clone();
                                        let env_vars = spec["env_vars"].clone();
                                        let image_str = spec["image"]
                                            .as_str().unwrap_or("").to_string();

                                        let chunk_start = spec["chunk_start"].as_i64().unwrap_or(0);
                                        let chunk_end = spec["chunk_end"].as_i64().unwrap_or(0);

                                        let node_id_done = node_id.clone();
                                        let app_h = app_handle.clone();
                                        let app_h_b = app_h.clone();
                                        let chunk_id_b = chunk_id.clone();
                                        let rt_handle = tokio::runtime::Handle::current();

                                        let (log_tx, mut log_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
                                        
                                        // Spawn log streaming WebSocket forwarder
                                        let tx_out_logs = tx_out.clone();
                                        let job_id_logs = job_id.clone();
                                        let chunk_id_logs = chunk_id.clone();
                                        rt_handle.spawn(async move {
                                            while let Some(line) = log_rx.recv().await {
                                                // Parse progress from Blender's stdout (e.g. "Fra:53")
                                                let mut progress = None;
                                                if chunk_end > chunk_start {
                                                    if let Some(fra_idx) = line.find("Fra:") {
                                                        let sub = &line[fra_idx + 4..];
                                                        let mut num_str = String::new();
                                                        for c in sub.chars() {
                                                            if c.is_ascii_digit() {
                                                                num_str.push(c);
                                                            } else {
                                                                break;
                                                             }
                                                         }
                                                         if let Ok(frame) = num_str.parse::<i64>() {
                                                             let total = chunk_end - chunk_start + 1;
                                                             let rendered = (frame - chunk_start).max(0);
                                                             let pct = (rendered as f64 / total as f64 * 100.0) as i32;
                                                             progress = Some(pct.min(100));
                                                         }
                                                     }
                                                 }

                                                 // If progress updated, send status update to update the UI progress bar
                                                 if let Some(pct) = progress {
                                                     let progress_payload = serde_json::json!({
                                                         "type": "chunk_status",
                                                         "job_id": job_id_logs,
                                                         "chunk_id": chunk_id_logs,
                                                         "status": "running",
                                                         "progress": pct
                                                     });
                                                     let _ = tx_out_logs.send(Message::Text(progress_payload.to_string().into()));
                                                 }

                                                let log_payload = serde_json::json!({
                                                    "type": "log",
                                                    "job_id": job_id_logs,
                                                    "chunk_id": chunk_id_logs,
                                                    "log": line
                                                });
                                                let _ = tx_out_logs.send(Message::Text(log_payload.to_string().into()));
                                            }
                                        });

                                        // Spawn telemetry sampling WebSocket forwarder
                                        let tx_out_telemetry = tx_out.clone();
                                        let job_id_telemetry = job_id.clone();
                                        let chunk_id_telemetry = chunk_id.clone();
                                        let app_handle_telemetry = app_handle.clone();
                                        rt_handle.spawn(async move {
                                            tokio::time::sleep(Duration::from_millis(1000)).await;
                                            loop {
                                                let busy = if let Some(state) = app_handle_telemetry.try_state::<crate::AppState>() {
                                                    state.is_busy.load(Ordering::SeqCst)
                                                } else {
                                                    false
                                                };
                                                if !busy {
                                                    break;
                                                }

                                                let (gpu_load, temp, vram) = read_gpu_telemetry();
                                                let telemetry_payload = serde_json::json!({
                                                    "type": "chunk_status",
                                                    "job_id": job_id_telemetry,
                                                    "chunk_id": chunk_id_telemetry,
                                                    "status": "running",
                                                    "telemetry": {
                                                        "gpu_load": gpu_load,
                                                        "temp": temp,
                                                        "vram_percent": vram
                                                    }
                                                });
                                                let _ = tx_out_telemetry.send(Message::Text(telemetry_payload.to_string().into()));
                                                tokio::time::sleep(Duration::from_secs(2)).await;
                                            }
                                        });

                                        let tx_out_done = tx_out.clone();
                                        tokio::task::spawn_blocking(move || {
                                            let mut success = false;

                                            if !image_str.is_empty() {
                                                println!("Pulling Docker image: {}", image_str);
                                                match crate::docker_manager::pull_image(&image_str) {
                                                    Ok(_) => {
                                                        println!("Running workload for chunk {}", chunk_id);
                                                        let net_mode = spec["network_mode"].as_str().unwrap_or("none");
                                                        match crate::docker_manager::run_workload(
                                                            &spec, net_mode, &env_vars, &chunk_id
                                                        ) {
                                                            Ok(c_id) => {
                                                                println!("Container {}", c_id);
                                                                success = crate::docker_manager::stream_logs_and_wait(&c_id, &app_h, &chunk_id, log_tx)
                                                                    .unwrap_or(false);
                                                                println!("Done (ok={})", success);
                                                            }
                                                            Err(err) => {
                                                                eprintln!("Error running workload: {}", err);
                                                                let _ = app_h.emit("chunk_log", serde_json::json!({
                                                                    "chunk_id": chunk_id,
                                                                    "log": format!("Error running workload: {}", err)
                                                                }));
                                                            }
                                                        }
                                                    }
                                                    Err(err) => {
                                                        eprintln!("Error pulling image: {}", err);
                                                        let _ = app_h.emit("chunk_log", serde_json::json!({
                                                            "chunk_id": chunk_id,
                                                            "log": format!("Error pulling image: {}", err)
                                                        }));
                                                    }
                                                }
                                            } else {
                                                println!("No image for chunk {}, marking complete", chunk_id);
                                                success = true;
                                            }

                                            if let Some(state) = app_h_b.try_state::<crate::AppState>() {
                                                state.is_busy.store(false, Ordering::SeqCst);
                                            }

                                            let final_status = if success { "completed" } else { "failed" };
                                            let status_msg = json!({
                                                "type": "chunk_status",
                                                "chunk_id": chunk_id,
                                                "job_id": job_id,
                                                "node_id": node_id_done,
                                                "status": final_status
                                            });

                                            let _ = app_h_b.emit("job_status_update", json!({
                                                "chunk_id": chunk_id_b,
                                                "status": final_status
                                            }));

                                            rt_handle.spawn(async move {
                                                let _ = tx_out_done.send(Message::Text(status_msg.to_string().into()));
                                            });
                                        });
                                    }
                                    "job_cancel" => {
                                        let chunk_id = json_val["chunk_id"]
                                            .as_str().unwrap_or("").to_string();
                                        if !chunk_id.is_empty() {
                                            println!("Cancelling chunk {}", chunk_id);
                                            let _ = crate::docker_manager::cancel_workload(&chunk_id);
                                        }
                                        if let Some(state) = app_handle.try_state::<crate::AppState>() {
                                            state.is_busy.store(false, Ordering::SeqCst);
                                        }
                                    }
                                    _ => {
                                        println!("Received msg type: {}", msg_type);
                                    }
                                }
                            }
                        }
                        Ok(Message::Close(_)) => {
                            println!("WebSocket closed by server.");
                            break;
                        }
                        Err(e) => {
                            println!("WebSocket error: {}", e);
                            break;
                        }
                        _ => {}
                    }
                }

                // Unpublish active connection writer and cleanup
                {
                    let mut lock = active_writer.lock().await;
                    *lock = None;
                }
                heartbeat_handle.abort();
            }
            Err(e) => {
                println!("Failed to connect: {}", e);
            }
        }

        let _ = app_handle.emit("ws_status", json!({ "status": "disconnected" }));
        println!("Reconnecting in 5 seconds...");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
