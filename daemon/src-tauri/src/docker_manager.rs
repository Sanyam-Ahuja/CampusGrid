use std::process::{Command, Stdio};
use std::io::{BufRead, BufReader};
use tauri::Emitter;
use serde_json::Value;

pub fn is_docker_installed() -> bool {
    Command::new("docker")
        .arg("--version")
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false)
}



pub fn pull_image(image: &str) -> Result<(), String> {
    // Check if the image exists locally first
    let check = Command::new("docker")
        .arg("image")
        .arg("inspect")
        .arg(image)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
        
    if let Ok(status) = check {
        if status.success() {
            println!("Image {} found locally, skipping remote pull to prevent TLS/Network timeouts", image);
            return Ok(());
        }
    }

    // Try pulling if it's completely missing
    let status = Command::new("docker")
        .arg("pull")
        .arg(image)
        .status()
        .map_err(|e| format!("Failed to execute docker pull: {}", e))?;

    if status.success() {
        Ok(())
    } else {
        println!("Warning: Docker pull failed for {}, attempting to proceed without pulling", image);
        Ok(()) // Mask error so docker run can attempt local fallback anyways
    }
}

/// Force-stop and remove the container for a chunk (used on job cancellation).
pub fn cancel_workload(chunk_id: &str) -> Result<(), String> {
    let container_name = format!("campugrid_chunk_{}", chunk_id);
    let _ = Command::new("docker").args(["rm", "-f", &container_name]).output();
    Ok(())
}

pub fn run_workload(
    spec: &Value,
    network_mode: &str,
    env_vars: &Value,
    chunk_id: &str,
) -> Result<String, String> {
    let image = spec["image"]
        .as_str()
        .ok_or("Missing image in generic spec")?;
    
    let command = spec["command"]
        .as_str()
        .unwrap_or(""); // Can be empty if entrypoint is sufficient

    let container_name = format!("campugrid_chunk_{}", chunk_id);
    
    // Force remove any old container with the same name to prevent instant crash 
    // when resubmitting or retrying chunks
    let _ = Command::new("docker").args(["rm", "-f", &container_name]).output();

    let mut cmd = Command::new("docker");
    cmd.arg("run").arg("-d"); // Run detached (removed --rm so we can fetch logs after completion)

    // Robust GPU Passthrough detection (Docker vs Podman support on Silverblue)
    let gpu_status = crate::gpu_setup::check_gpu_setup();
    let is_podman_engine = if let Ok(host) = std::env::var("DOCKER_HOST") {
        host.to_lowercase().contains("podman")
    } else {
        false
    } || if let Ok(output) = Command::new("docker").arg("--version").output() {
        String::from_utf8_lossy(&output.stdout).to_lowercase().contains("podman")
    } else {
        false
    } || if let Ok(output) = Command::new("docker").arg("info").output() {
        String::from_utf8_lossy(&output.stdout).to_lowercase().contains("podman")
    } else {
        false
    };

    if gpu_status.nvidia_gpu_detected || gpu_status.fully_ready {
        if is_podman_engine {
            cmd.arg("--device").arg("nvidia.com/gpu=all");
            println!("Podman GPU passthrough enabled (--device nvidia.com/gpu=all)");
        } else {
            cmd.arg("--gpus").arg("all");
            println!("Docker GPU passthrough enabled (--gpus all)");
        }
    } else {
        // Fallback: If GPU is requested, try to pass it anyway in case nvidia-smi failed inside a sandbox/toolbox environment
        let spec_gpu = spec["gpu_required"].as_bool().unwrap_or(false);
        if spec_gpu {
            if is_podman_engine {
                cmd.arg("--device").arg("nvidia.com/gpu=all");
                println!("GPU requested: Attempting Podman GPU passthrough (--device nvidia.com/gpu=all)");
            } else {
                cmd.arg("--gpus").arg("all");
                println!("GPU requested: Attempting Docker GPU passthrough (--gpus all)");
            }
        }
    }

    // Tier 2 Security Sandboxing (Relaxed for Freedom)
    // cmd.arg("--security-opt=no-new-privileges:true"); // Removed to allow dynamic package installation
    cmd.arg("--pids-limit=2048"); // Increased to allow more complex workloads
    cmd.arg("--dns=8.8.8.8");     // Ensure reliable name resolution for downloads
    cmd.arg("--cap-add=NET_ADMIN"); // Expanded freedom: allow network administrative tasks
    cmd.arg("--cap-add=NET_RAW");   // Expanded freedom: allow raw sockets (e.g. ping)
    
    // Resource Ceilings (Static for MVP, dynamically allocated from Settings in Prod)
    // We strictly enforce 16g RAM so the kernel kills it instead of bleeding host memory over 32g
    cmd.arg("--memory=16g");
    cmd.arg("--shm-size=8g"); // Essential for GPU and multiprocessing to avoid crashing
    cmd.arg("--cpus=4");

    let requires_public = spec["requires_public_network"].as_bool().unwrap_or(false);

    // Default to bridge/public for "freedom" if none specified
    if network_mode == "campugrid_overlay" || network_mode == "host" {
        cmd.arg("--network=host"); // host mode: container sees host's localhost directly
    } else if network_mode == "bridge" || requires_public || network_mode == "none" || network_mode.is_empty() {
        cmd.arg("--network=bridge");
        cmd.arg("--add-host=host.docker.internal:host-gateway");
    } else {
        cmd.arg("--network").arg(network_mode);
    }

    // Pass environment variables
    if let Some(envs) = env_vars.as_object() {
        for (k, v) in envs {
            if let Some(v_str) = v.as_str() {
                cmd.arg("-e").arg(format!("{}={}", k, v_str));
            }
        }
    }

    // Override entrypoint to sh so we can run multi-command pipelines
    // regardless of what the image's default ENTRYPOINT is (e.g. ikester/blender sets blender as entrypoint)
    cmd.arg("--name").arg(&container_name);
    cmd.arg("-e").arg("AUDIODEV=null"); // Suppress ALSA/OSS audio errors in headless Blender
    cmd.arg("--entrypoint").arg("sh");
    cmd.arg(image);

    if !command.is_empty() {
        // Dynamic curl injection: if curl is missing, try to install it using common package managers
        let wrapped_command = format!(
            "if ! command -v curl >/dev/null 2>&1; then \
                echo 'Utility [curl] not found. Attempting dynamic installation...'; \
                (apt-get update && apt-get install -y --no-install-recommends curl || apk add curl || yum install -y curl) >/dev/null 2>&1; \
             fi; \
             {}", 
            command
        );
        // Pass as sh -c "command" so shell operators (&&, URLs with &, quotes) are preserved
        cmd.arg("-c").arg(wrapped_command);
    }

    let output = cmd.output().map_err(|e| format!("Docker run failed: {}", e))?;

    if output.status.success() {
        let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(container_id)
    } else {
        Err(String::from_utf8_lossy(&output.stderr).to_string())
    }
}

pub fn stream_logs_and_wait(container_id: &str, app_handle: &tauri::AppHandle, chunk_id: &str) -> Result<bool, String> {
    let mut child = Command::new("docker")
        .arg("logs")
        .arg("-f")
        .arg(container_id)
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|e| format!("Failed to start docker logs: {}", e))?;

    if let Some(stdout) = child.stdout.take() {
        let app_handle_clone = app_handle.clone();
        let chunk_clone = chunk_id.to_string();
        std::thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                if let Ok(l) = line {
                    let _ = app_handle_clone.emit("chunk_log", serde_json::json!({
                        "chunk_id": chunk_clone,
                        "log": l
                    }));
                }
            }
        });
    }

    let status = Command::new("docker")
        .arg("wait")
        .arg(container_id)
        .status()
        .map_err(|e| format!("Docker wait failed: {}", e))?;
    
    // Check if the container actually succeeded
    let output = Command::new("docker")
        .arg("inspect")
        .arg(container_id)
        .arg("--format={{.State.ExitCode}}")
        .output()
        .unwrap();
    let exit_code = String::from_utf8_lossy(&output.stdout).trim().to_string();
    
    // Manually delete the container after we finish inspecting it
    let _ = Command::new("docker").arg("rm").arg(container_id).output();
    
    Ok(status.success() && exit_code == "0")
}
