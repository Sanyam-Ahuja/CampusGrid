mod hw_detector;
mod docker_manager;
mod gpu_setup;
mod websocket;

use tauri::{AppHandle, Manager};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::fs;
use serde::Serialize;

pub struct AppState {
    pub is_active: Arc<AtomicBool>,
    pub is_logged_in: Arc<AtomicBool>,
    // True while a workload container is running on this node. Drives heartbeat
    // availability so the server doesn't dispatch a second chunk to a busy node.
    pub is_busy: Arc<AtomicBool>,
}

/// Check if credentials file exists and has content
fn load_credentials(app: &tauri::App) -> Option<(String, String)> {
    let dir = app.path().app_data_dir().unwrap_or_default();
    let node_path = dir.join("node_id.txt");
    let token_path = dir.join("auth_token.txt");

    let node_id = fs::read_to_string(&node_path).ok()?.trim().to_string();
    let token = fs::read_to_string(&token_path).ok()?.trim().to_string();

    if node_id.is_empty() || token.is_empty() {
        return None;
    }

    Some((node_id, token))
}

#[derive(Serialize)]
struct AuthResult {
    success: bool,
    token: Option<String>,
    error: Option<String>,
}

#[derive(Serialize)]
struct RegisterResult {
    success: bool,
    node_id: Option<String>,
    node_token: Option<String>,
    error: Option<String>,
}

#[tauri::command]
fn get_hardware_profile() -> hw_detector::HardwareProfile {
    hw_detector::get_hardware_profile()
}

#[tauri::command]
fn get_docker_status() -> bool {
    docker_manager::is_docker_installed()
}

#[tauri::command]
fn check_gpu_setup() -> gpu_setup::GpuSetupStatus {
    gpu_setup::check_gpu_setup()
}

#[tauri::command]
fn install_gpu_toolkit() -> Result<String, String> {
    let status = gpu_setup::check_gpu_setup();
    gpu_setup::install_toolkit(&status.distro_family)
}

#[tauri::command]
fn configure_docker_gpu() -> Result<String, String> {
    gpu_setup::configure_docker_gpu()
}

#[tauri::command]
fn toggle_active(state: tauri::State<AppState>) -> bool {
    let current = state.is_active.load(Ordering::SeqCst);
    state.is_active.store(!current, Ordering::SeqCst);
    !current
}

#[tauri::command]
fn is_node_active(state: tauri::State<AppState>) -> bool {
    state.is_active.load(Ordering::SeqCst)
}

#[tauri::command]
fn has_credentials(app_handle: AppHandle) -> bool {
    let dir = app_handle.path().app_data_dir().unwrap_or_default();
    let node_path = dir.join("node_id.txt");
    let token_path = dir.join("auth_token.txt");

    if let (Ok(node_id), Ok(token)) = (fs::read_to_string(&node_path), fs::read_to_string(&token_path)) {
        !node_id.trim().is_empty() && !token.trim().is_empty()
    } else {
        false
    }
}

#[tauri::command]
fn save_credentials(app_handle: AppHandle, node_id: String, token: String) -> Result<(), String> {
    let dir = app_handle.path().app_data_dir().unwrap_or_default();
    fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    fs::write(dir.join("node_id.txt"), node_id.trim()).map_err(|e| e.to_string())?;
    fs::write(dir.join("auth_token.txt"), token.trim()).map_err(|e| e.to_string())?;
    Ok(())
}

#[tauri::command]
fn clear_credentials(app_handle: AppHandle, state: tauri::State<AppState>) -> Result<(), String> {
    let dir = app_handle.path().app_data_dir().unwrap_or_default();
    let _ = fs::remove_file(dir.join("node_id.txt"));
    let _ = fs::remove_file(dir.join("auth_token.txt"));
    state.is_logged_in.store(false, Ordering::SeqCst);
    state.is_active.store(false, Ordering::SeqCst);
    Ok(())
}

/// Authenticate with the CampuGrid server (login or register)
#[tauri::command]
async fn authenticate(
    email: String,
    password: String,
    name: Option<String>,
    is_register: bool,
) -> AuthResult {
    let base_url = option_env!("CAMPUGRID_API_URL").unwrap_or("http://localhost:8000/api/v1");
    let client = reqwest::Client::new();

    let result = if is_register {
        let body = serde_json::json!({
            "email": email,
            "name": name.unwrap_or_else(|| email.split('@').next().unwrap_or("contributor").to_string()),
            "password": password,
            "role": "both"
        });
        client.post(format!("{}/auth/register", base_url))
            .json(&body)
            .send()
            .await
    } else {
        let body = serde_json::json!({
            "email": email,
            "password": password,
        });
        client.post(format!("{}/auth/login", base_url))
            .json(&body)
            .send()
            .await
    };

    match result {
        Ok(resp) => {
            if resp.status().is_success() {
                match resp.json::<serde_json::Value>().await {
                    Ok(json) => {
                        if let Some(token) = json["access_token"].as_str() {
                            AuthResult {
                                success: true,
                                token: Some(token.to_string()),
                                error: None,
                            }
                        } else {
                            AuthResult {
                                success: false,
                                token: None,
                                error: Some("No token in response".to_string()),
                            }
                        }
                    }
                    Err(e) => AuthResult {
                        success: false,
                        token: None,
                        error: Some(format!("Failed to parse response: {}", e)),
                    },
                }
            } else {
                let error_text = resp.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                AuthResult {
                    success: false,
                    token: None,
                    error: Some(format!("Auth failed: {}", error_text)),
                }
            }
        }
        Err(e) => AuthResult {
            success: false,
            token: None,
            error: Some(format!("Connection failed: {}. Is the server running?", e)),
        },
    }
}

/// Auto-register this machine as a contributor node using detected hardware
#[tauri::command]
async fn auto_register_node(
    user_token: String,
    hw_profile: serde_json::Value,
) -> RegisterResult {
    let base_url = option_env!("CAMPUGRID_API_URL").unwrap_or("http://localhost:8000/api/v1");
    let client = reqwest::Client::new();

    // Get hostname from OS
    let hostname = gethostname::gethostname()
        .to_str()
        .unwrap_or("unknown-host")
        .to_string();

    let body = serde_json::json!({
        "hostname": hostname,
        "cpu_cores": hw_profile["cpu_cores"].as_u64().unwrap_or(4),
        "ram_gb": hw_profile["ram_gb"].as_f64().unwrap_or(8.0),
        "gpu_model": hw_profile["gpu_model"].as_str().unwrap_or("Unknown GPU"),
        "gpu_vram_gb": hw_profile["gpu_vram_gb"].as_f64().unwrap_or(0.0),
        "cuda_version": hw_profile["cuda_version"].as_str(),
        "os": hw_profile["os"].as_str().unwrap_or("unknown"),
    });

    match client
        .post(format!("{}/nodes/register", base_url))
        .bearer_auth(&user_token)
        .json(&body)
        .send()
        .await
    {
        Ok(resp) => {
            if resp.status().is_success() {
                match resp.json::<serde_json::Value>().await {
                    Ok(json) => {
                        let node_id = json["node_id"].as_str().map(|s| s.to_string());
                        let node_token = json["node_token"].as_str().map(|s| s.to_string());

                        if node_id.is_some() && node_token.is_some() {
                            RegisterResult {
                                success: true,
                                node_id,
                                node_token,
                                error: None,
                            }
                        } else {
                            RegisterResult {
                                success: false,
                                node_id: None,
                                node_token: None,
                                error: Some("Missing node_id or node_token in response".to_string()),
                            }
                        }
                    }
                    Err(e) => RegisterResult {
                        success: false,
                        node_id: None,
                        node_token: None,
                        error: Some(format!("Failed to parse: {}", e)),
                    },
                }
            } else {
                let error_text = resp.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                RegisterResult {
                    success: false,
                    node_id: None,
                    node_token: None,
                    error: Some(format!("Registration failed: {}", error_text)),
                }
            }
        }
        Err(e) => RegisterResult {
            success: false,
            node_id: None,
            node_token: None,
            error: Some(format!("Connection failed: {}", e)),
        },
    }
}

#[tauri::command]
fn restart_websocket(app_handle: AppHandle, state: tauri::State<AppState>) {
    let dir = app_handle.path().app_data_dir().unwrap_or_default();

    if let (Ok(node_id), Ok(token)) = (
        fs::read_to_string(dir.join("node_id.txt")),
        fs::read_to_string(dir.join("auth_token.txt")),
    ) {
        let node_id = node_id.trim().to_string();
        let token = token.trim().to_string();

        if !node_id.is_empty() && !token.is_empty() {
            // Unset current login first to kill the old loop
            state.is_logged_in.store(false, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(100)); // Brief pause for graceful kill
            state.is_logged_in.store(true, Ordering::SeqCst);
            
            let handle = app_handle.clone();
            tauri::async_runtime::spawn(async move {
                websocket::connect_and_listen(handle, node_id, token).await;
            });
        }
    }
}

#[tauri::command]
async fn fetch_node_history(app_handle: AppHandle) -> Result<serde_json::Value, String> {
    let dir = app_handle.path().app_data_dir().unwrap_or_default();
    let node_id = fs::read_to_string(dir.join("node_id.txt")).unwrap_or_default().trim().to_string();
    let token = fs::read_to_string(dir.join("auth_token.txt")).unwrap_or_default().trim().to_string();
    
    if node_id.is_empty() || token.is_empty() {
        return Err("No credentials".to_string());
    }
    
    let base_url = option_env!("CAMPUGRID_API_URL").unwrap_or("http://localhost:8000/api/v1");
    let client = reqwest::Client::new();
    
    let res = client.get(format!("{}/nodes/me/{}/history", base_url, node_id))
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("HTTP Error: {}", e))?;
        
    let json = res.json::<serde_json::Value>()
        .await
        .map_err(|e| format!("JSON Parse Error: {}", e))?;
        
    Ok(json)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            let app_handle = app.handle().clone();

            // Try to load existing credentials
            let (node_id, auth_token) = load_credentials(app)
                .unwrap_or_else(|| ("".to_string(), "".to_string()));

            let node_id_clone = node_id.clone();
            let token_clone = auth_token.clone();

            // Ensure the state represents credentials
            let logged_in = !node_id.is_empty() && !auth_token.is_empty();

            app.manage(AppState {
                // Contribute by default once logged in; the dashboard toggle pauses it.
                is_active: Arc::new(AtomicBool::new(true)),
                is_logged_in: Arc::new(AtomicBool::new(logged_in)),
                is_busy: Arc::new(AtomicBool::new(false)),
            });

            // Start websocket loop only if credentials exist
            if logged_in {
                tauri::async_runtime::spawn(async move {
                    websocket::connect_and_listen(app_handle, node_id_clone, token_clone).await;
                });
            }

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            get_hardware_profile,
            get_docker_status,
            check_gpu_setup,
            install_gpu_toolkit,
            configure_docker_gpu,
            toggle_active,
            is_node_active,
            has_credentials,
            save_credentials,
            clear_credentials,
            authenticate,
            auto_register_node,
            restart_websocket,
            fetch_node_history
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
