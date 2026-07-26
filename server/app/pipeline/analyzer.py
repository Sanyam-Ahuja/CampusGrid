"""Step 2: Deep Context Analysis."""

import ast
import io
import logging
import zipfile
from dataclasses import dataclass

from app.core.config import get_settings
from app.pipeline.detector import FileDetection
from app.services.minio_service import minio_service

logger = logging.getLogger(__name__)


@dataclass
class Resources:
    vram_gb: float
    ram_gb: float
    cpu_cores: int


@dataclass
class JobProfile:
    type: str               # 'render', 'data', 'ml_training', 'simulation'
    framework: str | None   # 'pytorch', 'blender', 'openfoam', etc.
    gpu_required: bool
    resources: Resources
    split_params: dict      # e.g. {frame_start, frame_end}
    confidence: float
    entry_file: str
    imports: list[str] = None
    # MinIO key of a user-supplied Dockerfile, if one was uploaded alongside the
    # workload. When set, the orchestrator uses its FROM base + RUN lines as the
    # container image + setup instead of the catalog/Gemini path.
    custom_dockerfile: str | None = None
    requires_public_network: bool = False


# ── Analyzers ──────────────────────────────────────────────────

def analyze_blend(job_id: str, file_keys: list[str]) -> JobProfile:
    """Analyze a Blender project without external readers like blend-file-reader."""
    # Since we can't reliably parse .blend binary in Python natively without a huge custom module,
    # we'll assume standard parameters: Frames 1-250, CYCLES, GPU required.
    # We are returning a strong confidence profile so the splitter can handle it.
    blend_file = next(k for k in file_keys if k.endswith('.blend'))

    return JobProfile(
        type="render",
        framework="blender",
        gpu_required=True,
        resources=Resources(vram_gb=4.0, ram_gb=8.0, cpu_cores=4),
        split_params={"frame_start": 1, "frame_end": 250, "minio_key": blend_file}, # Mock defaults until binary parsing
        confidence=0.8,
        entry_file=blend_file,
    )


def analyze_python(job_id: str, file_keys: list[str]) -> JobProfile:
    """Analyze Python scripts using AST to extract imports safely."""
    # Find the main entry point. file_keys are full MinIO object keys like
    # "{job_id}/train.py", so match on the basename rather than the whole key.
    py_files = [k for k in file_keys if k.endswith('.py')]
    entry_file = None
    for preferred in ("train.py", "main.py"):
        match = next((k for k in py_files if k.split("/")[-1] == preferred), None)
        if match:
            entry_file = match
            break
    if not entry_file and py_files:
        entry_file = py_files[0]

    if not entry_file:
        raise ValueError("No entry_file found for Python workload")

    # Download the script from MinIO to read AST
    settings = get_settings()
    script_content = minio_service.download_bytes(settings.BUCKET_JOB_INPUTS, entry_file)

    try:
        tree = ast.parse(script_content)
    except SyntaxError:
        raise ValueError("Invalid Python syntax in entry_file")

    # Extract all imports
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                imports.add(n.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.add(node.module.split('.')[0])

    # Detect framework based on imports
    framework = None
    gpu_required = False

    if "torch" in imports:
        framework = "pytorch"
        # Deep inspect if `.cuda()` is called
        if b".cuda()" in script_content or b".to('cuda" in script_content or b"device='cuda" in script_content:
            gpu_required = True
    elif "tensorflow" in imports:
        framework = "tensorflow"
        gpu_required = True # Usually tensorflow implies GPU if asked for grid compute
    elif "jax" in imports:
        framework = "jax"
        gpu_required = True
    elif "pandas" in imports or "polars" in imports:
        framework = "python-data"

    return JobProfile(
        type="ml_training" if framework in ["pytorch", "tensorflow", "jax"] else "data",
        framework=framework,
        gpu_required=gpu_required,
        resources=Resources(vram_gb=8.0 if gpu_required else 0.0, ram_gb=16.0, cpu_cores=8),
        split_params={"minio_key": entry_file}, # By default ml_training local_sgd doesn't need split ranges here
        confidence=0.9,
        entry_file=entry_file,
        imports=list(imports)
    )


def analyze_zip(job_id: str, file_keys: list[str]) -> JobProfile:
    """Analyze files inside a zip archive (supports ML/Python and Blender Render)."""
    zip_key = next((k for k in file_keys if k.endswith('.zip')), None)
    if not zip_key:
        zip_key = file_keys[0]

    settings = get_settings()
    zip_bytes = minio_service.download_bytes(settings.BUCKET_JOB_INPUTS, zip_key)

    py_files = {}
    blend_files = []
    manifest_files = {}
    cpp_files = []
    rust_files = []
    go_files = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for info in z.infolist():
            if info.is_dir():
                continue
            name = info.filename
            filename_lower = name.split("/")[-1].lower()

            if filename_lower == "cmakelists.txt":
                manifest_files["CMakeLists.txt"] = name
            elif filename_lower == "makefile":
                manifest_files["Makefile"] = name
            elif filename_lower == "cargo.toml":
                manifest_files["Cargo.toml"] = name
            elif filename_lower == "go.mod":
                manifest_files["go.mod"] = name

            if name.endswith(('.cpp', '.cc', '.c', '.cxx', '.cu', '.h', '.hpp')):
                cpp_files.append(name)
            elif name.endswith('.rs'):
                rust_files.append(name)
            elif name.endswith('.go'):
                go_files.append(name)
            elif filename_lower.endswith('.py'):
                py_files[name] = z.read(name)
            elif filename_lower.endswith('.blend'):
                blend_files.append(name)

    # 1. Check for Blender files first
    if blend_files:
        blend_files.sort(key=lambda f: f.count('/'))
        entry_file = blend_files[0]
        
        return JobProfile(
            type="render",
            framework="blender",
            gpu_required=True,
            resources=Resources(vram_gb=4.0, ram_gb=8.0, cpu_cores=4),
            split_params={"frame_start": 1, "frame_end": 250, "minio_key": zip_key},
            confidence=0.9,
            entry_file=entry_file,
        )

    # 1.5 Check for Compiled Language Build Manifests (C/C++, Rust, Go)
    compiled_framework = None
    compiled_entry_file = None

    if "CMakeLists.txt" in manifest_files:
        compiled_framework = "cpp"
        # Find a suitable entry/main source file for context
        for f in cpp_files:
            f_lower = f.lower()
            if "main.cpp" in f_lower or "main.cc" in f_lower or "main.c" in f_lower or "llama.cpp" in f_lower:
                compiled_entry_file = f
                break
        if not compiled_entry_file and cpp_files:
            compiled_entry_file = cpp_files[0]
        if not compiled_entry_file:
            compiled_entry_file = manifest_files["CMakeLists.txt"]

    elif "Makefile" in manifest_files:
        compiled_framework = "cpp"
        for f in cpp_files:
            f_lower = f.lower()
            if "main.cpp" in f_lower or "main.cc" in f_lower or "main.c" in f_lower or "llama.cpp" in f_lower:
                compiled_entry_file = f
                break
        if not compiled_entry_file and cpp_files:
            compiled_entry_file = cpp_files[0]
        if not compiled_entry_file:
            compiled_entry_file = manifest_files["Makefile"]

    elif "Cargo.toml" in manifest_files:
        compiled_framework = "rust"
        for f in rust_files:
            if f.endswith("main.rs"):
                compiled_entry_file = f
                break
        if not compiled_entry_file and rust_files:
            compiled_entry_file = rust_files[0]
        if not compiled_entry_file:
            compiled_entry_file = manifest_files["Cargo.toml"]

    elif "go.mod" in manifest_files:
        compiled_framework = "go"
        for f in go_files:
            if f.endswith("main.go"):
                compiled_entry_file = f
                break
        if not compiled_entry_file and go_files:
            compiled_entry_file = go_files[0]
        if not compiled_entry_file:
            compiled_entry_file = manifest_files["go.mod"]

    if compiled_framework:
        gpu_required = False
        # Heuristically check if CUDA/GPU is mentioned in filenames or files inside the zip
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            for info in z.infolist():
                if "cuda" in info.filename.lower() or info.filename.endswith(".cu"):
                    gpu_required = True
                    break
        return JobProfile(
            type="data",
            framework=compiled_framework,
            gpu_required=gpu_required,
            resources=Resources(vram_gb=8.0 if gpu_required else 0.0, ram_gb=16.0, cpu_cores=8),
            split_params={"minio_key": zip_key},
            confidence=0.9,
            entry_file=compiled_entry_file,
        )

    # 2. Check for Python files
    if not py_files:
        raise ValueError("No valid Python or Blender files found inside the zip archive")

    # Find the main entry point
    entry_file = None
    for name in py_files.keys():
        if name.endswith("train.py"):
            entry_file = name
            break
        elif name.endswith("main.py"):
            entry_file = name

    # Fallback to the first python file
    if not entry_file:
        entry_file = list(py_files.keys())[0]

    script_content = py_files[entry_file]

    try:
        tree = ast.parse(script_content)
    except SyntaxError:
        raise ValueError(f"Invalid Python syntax in {entry_file} inside zip")

    # Extract all imports
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                imports.add(n.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.add(node.module.split('.')[0])

    framework = None
    gpu_required = False

    if "torch" in imports:
        framework = "pytorch"
        if b".cuda()" in script_content or b".to('cuda" in script_content or b"device='cuda" in script_content:
            gpu_required = True
    elif "tensorflow" in imports:
        framework = "tensorflow"
        gpu_required = True
    elif "jax" in imports:
        framework = "jax"
        gpu_required = True
    elif "pandas" in imports or "polars" in imports:
        framework = "python-data"

    # Make the assumption that if they upload a zip file with python and requirements
    # we should pass it to the generator if it's custom, or default to general framework
    if not framework:
        # Fallback for generic python
        framework = "python"
        
    return JobProfile(
        type="ml_training" if framework in ["pytorch", "tensorflow", "jax"] else "data",
        framework=framework,
        gpu_required=gpu_required,
        resources=Resources(vram_gb=8.0 if gpu_required else 0.0, ram_gb=16.0, cpu_cores=8),
        split_params={"minio_key": zip_key}, 
        confidence=0.9,
        entry_file=entry_file,
        imports=list(imports)
    )


def analyze_simulation(job_id: str, file_keys: list[str], detections: list[FileDetection]) -> JobProfile:
    """Analyze simulation workloads — OpenFOAM, LAMMPS, GROMACS."""
    # Detect framework from file patterns
    framework = None
    entry_file = None
    gpu_required = False
    split_params = {}

    for key in file_keys:
        filename = key.split("/")[-1].lower()

        # OpenFOAM detection
        if filename in ["controldict", "fvschemes", "fvsolution"] or "/system/" in key.lower():
            framework = "openfoam"
            entry_file = key
            split_params = {"case_dir": "/".join(key.split("/")[:-2])}  # parent of system/
            break

        # LAMMPS detection
        if filename.endswith(".lammps") or filename.startswith("in."):
            framework = "lammps"
            entry_file = key
            gpu_required = True  # LAMMPS GPU package is common
            split_params = {"input_file": filename}
            break

        # GROMACS detection
        if filename.endswith(".tpr") or filename.endswith(".mdp"):
            framework = "gromacs"
            entry_file = key
            if filename.endswith(".tpr"):
                split_params = {"tpr_file": filename}
            break

    if not framework:
        # Fallback: check for common simulation file extensions
        for key in file_keys:
            filename = key.split("/")[-1].lower()
            if any(filename.endswith(ext) for ext in [".msh", ".cas", ".geo", ".stl"]):
                framework = "openfoam"  # Default to OpenFOAM for mesh files
                entry_file = key
                break

    if not framework:
        raise ValueError("Could not determine simulation framework")

    split_params["minio_key"] = entry_file or file_keys[0]
    return JobProfile(
        type="simulation",
        framework=framework,
        gpu_required=gpu_required,
        resources=Resources(
            vram_gb=4.0 if gpu_required else 0.0,
            ram_gb=16.0,
            cpu_cores=8,
        ),
        split_params=split_params,
        confidence=0.85,
        entry_file=entry_file or file_keys[0],
    )


# ── Dispatcher ─────────────────────────────────────────────────

def analyze_files(job_id: str, file_keys: list[str], detections: list[FileDetection]) -> JobProfile:
    """Determine the primary workload type, attaching a custom Dockerfile if present."""
    # A user-supplied Dockerfile augments a normally-detected workload: we still
    # need to know what KIND of job this is (render/ml/data/sim) so we can split
    # and assemble it. The Dockerfile only changes the base image + setup.
    dockerfile_key = next((k for k in file_keys if k.split("/")[-1] == "Dockerfile"), None)

    # Detect against everything except the Dockerfile itself.
    workload_keys = [k for k in file_keys if k.split("/")[-1] != "Dockerfile"]
    profile = _detect_profile(job_id, workload_keys, detections)

    if dockerfile_key:
        logger.info(f"Custom Dockerfile attached for job {job_id}: {dockerfile_key}")
        profile.custom_dockerfile = dockerfile_key
    return profile


def _detect_profile(job_id: str, file_keys: list[str], detections: list[FileDetection]) -> JobProfile:
    """Heuristic workload-type detection from file signatures and extensions."""
    for det in detections:
        # Blender files often use zstd compression which obscures their 'BLENDER' magic bytes.
        # We accept them even with 0.5 confidence (extension only).
        if det.file_type == "blender" and det.confidence >= 0.5:
            return analyze_blend(job_id, file_keys)

        elif det.file_type == "python_script" and det.confidence > 0.8:
            return analyze_python(job_id, file_keys)

        elif det.file_type == "zip_based" and det.confidence > 0.5:
            return analyze_zip(job_id, file_keys)

    # Check for simulation files by extension patterns
    sim_extensions = {
        ".lammps", ".tpr", ".mdp", ".msh", ".cas", ".geo",
    }
    sim_names = {"controldict", "fvschemes", "fvsolution", "blockmeshdict"}

    for key in file_keys:
        filename = key.split("/")[-1].lower()
        ext = "." + filename.split(".")[-1] if "." in filename else ""

        if ext in sim_extensions or filename in sim_names or filename.startswith("in."):
            return analyze_simulation(job_id, file_keys, detections)

    # Check for data files (CSV, Parquet)
    data_extensions = {".csv", ".parquet", ".tsv", ".json", ".jsonl"}
    for key in file_keys:
        ext = "." + key.split(".")[-1].lower() if "." in key else ""
        if ext in data_extensions:
            # If there's also a Python script, it's a data processing job
            py_keys = [k for k in file_keys if k.endswith(".py")]
            if py_keys:
                return analyze_python(job_id, file_keys)

    raise ValueError("Could not determine a valid JobProfile from uploaded files")
