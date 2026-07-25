"""Tier 2/3 AI Pipeline - Gemini Workload Classifier.

When local heuristics fail to identify the project profile (e.g. nested zip structures
or unknown project layouts), this classifier queries Gemini with the project file tree
and configuration files to classify the workload.
"""

import io
import json
import logging
import zipfile
from dataclasses import dataclass

from google import genai

from app.core.config import get_settings
from app.pipeline.analyzer import JobProfile, Resources
from app.services.minio_service import minio_service

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass
class GeminiClassifier:
    def __init__(self):
        self.client = genai.Client(api_key=settings.GEMINI_API_KEY)
        self.model_id = settings.GEMINI_MODEL

    async def classify(self, file_keys: list[str]) -> JobProfile:
        """Classify a project using Gemini by inspecting the file tree and config contents."""
        zip_key = next((k for k in file_keys if k.endswith('.zip')), None)
        
        file_tree = []
        configs_content = {}
        
        if zip_key:
            # Download the zip archive and inspect its contents
            zip_bytes = minio_service.download_bytes(settings.BUCKET_JOB_INPUTS, zip_key)
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
                for info in z.infolist():
                    if info.is_dir():
                        continue
                    file_tree.append(info.filename)
                    
                    # Extract contents of small configuration/setup files to aid classification
                    filename_lower = info.filename.split("/")[-1].lower()
                    if filename_lower in ["requirements.txt", "package.json", "cargo.toml", "setup.py", "makefile", "dockerfile"]:
                        try:
                            # Keep config files preview small (first 4KB)
                            content = z.read(info.filename)[:4096]
                            configs_content[info.filename] = content.decode('utf-8', errors='ignore')
                        except Exception:
                            pass
        else:
            # Flat files uploaded
            for key in file_keys:
                filename = key.split("/")[-1]
                file_tree.append(filename)
                filename_lower = filename.lower()
                if filename_lower in ["requirements.txt", "package.json", "cargo.toml", "setup.py", "makefile", "dockerfile"]:
                    try:
                        content = minio_service.download_bytes(settings.BUCKET_JOB_INPUTS, key)[:4096]
                        configs_content[filename] = content.decode('utf-8', errors='ignore')
                    except Exception:
                        pass

        # Construct a detailed classifier prompt for Gemini
        prompt = f"""You are an expert AI workload classifier for a distributed GPU/CPU compute grid.
Your task is to analyze the uploaded project's file list and configuration files to categorize it.

PROJECT FILE LIST:
{json.dumps(file_tree, indent=2)}

CONFIGURATION FILES CONTENT:
{json.dumps(configs_content, indent=2)}

Categorize the workload according to these strict rules:
1. type: MUST be one of:
   - "render" (e.g. Blender files, graphics rendering, 3D scenes)
   - "ml_training" (e.g. PyTorch, TensorFlow, JAX model training)
   - "data" (e.g. general Python/Node.js/Go scripts, data processing, web scrapers)
   - "simulation" (e.g. OpenFOAM, LAMMPS, GROMACS scientific simulations)
   - "other" (for anything else)
2. framework: The exact name of the tool, library, or language runtime (e.g. "blender", "pytorch", "tensorflow", "nodejs", "go", "python", "openfoam", "c++", etc.).
3. gpu_required: Set to true if this workload is typically GPU-intensive (e.g. Blender Cycles rendering, deep learning training) or explicitly references CUDA/GPUs. Otherwise false.
4. entry_file: The relative file path to the main file or executable that should be executed to start the job (e.g. "main.blend", "train.py", "index.js", "main.go").
5. resources: Recommend baseline resources. Match this JSON schema: {{"vram_gb": float, "ram_gb": float, "cpu_cores": int}}.

Output ONLY valid JSON matching this schema:
{{
  "type": "render",
  "framework": "blender",
  "gpu_required": true,
  "entry_file": "path/to/main.blend",
  "resources": {{
    "vram_gb": 4.0,
    "ram_gb": 8.0,
    "cpu_cores": 4
  }}
}}
"""

        logger.info("Triggering Gemini AI Workload Classifier...")
        response = await self.client.aio.models.generate_content(
            model=self.model_id,
            contents=prompt,
        )
        
        text = response.text.replace('```json', '').replace('```', '').strip()
        result = json.loads(text)
        
        logger.info(f"Gemini Classifier result: {result}")
        
        # Parse fields from the classifier result
        gpu_req = bool(result.get("gpu_required", False))
        res_data = result.get("resources", {})
        
        minio_key = zip_key if zip_key else file_keys[0]
        
        # Build split parameters based on type
        split_params = {"minio_key": minio_key}
        if result.get("type") == "render":
            split_params["frame_start"] = 1
            split_params["frame_end"] = 250
            
        return JobProfile(
            type=result.get("type", "other"),
            framework=result.get("framework", None),
            gpu_required=gpu_req,
            resources=Resources(
                vram_gb=float(res_data.get("vram_gb", 4.0 if gpu_req else 0.0)),
                ram_gb=float(res_data.get("ram_gb", 8.0)),
                cpu_cores=int(res_data.get("cpu_cores", 4)),
            ),
            split_params=split_params,
            confidence=0.95,
            entry_file=result.get("entry_file", file_keys[0]),
        )
