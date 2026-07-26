"""Tier 3 AI Pipeline - Gemini base-image + dependency resolution.

For unknown codebases we ask Gemini to pick a real, pullable base image and the
shell commands needed to install dependencies. The commands run inside that base
image at container start (CatalogEntry.setup_commands), so there is no registry
or Kaniko build to manage.

For compiled-language workloads (C/C++/CMake, Rust/Cargo, Go), Gemini generates
a Python wrapper script that performs the build and run steps inside the container.
"""

import json
import logging
from dataclasses import dataclass

from google import genai

from app.core.config import get_settings
from app.pipeline.analyzer import JobProfile

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass
class GenerationResult:
    base_image: str
    setup_commands: str
    needs_wrapper: bool = False
    wrapper_script: str | None = None
    reasoning: str = ""


class DockerfileGenerator:
    def __init__(self):
        self.client = genai.Client(api_key=settings.GEMINI_API_KEY)
        self.model_id = settings.GEMINI_MODEL

    async def generate(
        self,
        source_code: str,
        requirements_txt: str | None,
        profile: JobProfile,
        build_files: dict[str, str] | None = None,
    ) -> GenerationResult:
        """Pick a base image + dependency-install commands for an unknown codebase.

        For compiled languages, also generates a Python wrapper script that handles
        the build and run steps.

        Args:
            source_code: The entry file source code (first 3000 chars used in prompt).
            requirements_txt: Optional requirements.txt content.
            profile: The JobProfile with entry_file, framework, gpu_required, etc.
            build_files: Optional dict mapping manifest filenames to their contents
                         (e.g. {"CMakeLists.txt": "...", "Cargo.toml": "..."}).
        """
        if build_files is None:
            build_files = {}

        prompt = f"""You configure containers for a distributed compute platform that runs
arbitrary, unmodified GitHub repositories submitted by real users. The final command
executed inside the container is ALWAYS: `python {{entry_file}}` — this is fixed and
cannot be changed. Your job is to make that constraint work even when the actual
workload is written in a compiled or non-Python language.

CONTEXT PROVIDED:

Detected entry_file (from classifier): {profile.entry_file}
Framework detected: {profile.framework}
GPU required: {profile.gpu_required}

Build/dependency manifests found in the repo (empty string if absent):
--- CMakeLists.txt ---
{build_files.get('CMakeLists.txt', '(not found)')}
--- Cargo.toml ---
{build_files.get('Cargo.toml', '(not found)')}
--- Makefile ---
{build_files.get('Makefile', '(not found)')}
--- go.mod ---
{build_files.get('go.mod', '(not found)')}
--- package.json ---
{build_files.get('package.json', '(not found)')}
--- requirements.txt ---
{requirements_txt or '(not found)'}

Entry file source (first 3000 chars, may be non-Python — read it to find the
actual run command: CLI flags, model paths, arguments the repo's own README
or Makefile would normally pass):
{source_code[:3000]}

YOUR TASK — decide ONE of two paths:

PATH A — entry_file is already valid, directly-runnable Python (`python
{profile.entry_file}` works with no build step). Set needs_wrapper=false,
wrapper_script=null. This is the common case — do not invent a wrapper you
don't need.

PATH B — entry_file requires a build step (C/C++/CMake, Rust/Cargo, Go, or
any compiled/non-Python language), OR the real run command needs CLI
arguments the platform can't inject on its own. Set needs_wrapper=true and
write a COMPLETE, standalone Python 3 script as wrapper_script that:
  1. Uses subprocess.run([...], check=True, cwd="/input") for every build
     and run step — never os.system, never shell=True.
  2. Performs the FULL build pipeline implied by the manifest present
     (e.g. CMakeLists.txt -> `cmake -B build -DCMAKE_BUILD_TYPE=Release`
     then `cmake --build build -j$(nproc)`; Cargo.toml -> `cargo build
     --release`; go.mod -> `go build -o app .`).
  3. Runs the resulting binary/script with whatever arguments the source
     or README implies are required (input paths, model paths, flags).
     Never invent flags you can't justify from the provided context.
  4. Reads any input data ONLY from /input/ (already populated by the
     platform) and writes ALL results to /output/ (already exists) —
     these paths are fixed, do not assume any other location.
  5. Exits non-zero (let the subprocess exception propagate) on any
     build or run failure — do not swallow errors, the platform needs
     the real failure signal.
  6. Includes no interactive prompts, no `input()`, no assumptions of a
     TTY.

RULES THAT APPLY TO BOTH PATHS:
- base_image MUST be real and pullable. Use
  "nvidia/cuda:12.1.1-cudnn8-devel-ubuntu22.04" if GPU is needed AND a
  build step is required (note: devel, not runtime — compilers need the
  full toolkit); "nvidia/cuda:12.1.1-cudnn8-runtime-ubuntu22.04" if GPU
  is needed with no build step; "python:3.11-slim" otherwise.
- setup_commands is ONE shell string joined with && covering apt/pip/cargo/
  go toolchain installation only — never include the build or run steps
  themselves, those belong in wrapper_script (Path B) or are handled by
  the platform (Path A).
- Map import names to PyPI names (cv2 -> opencv-python, PIL -> pillow,
  sklearn -> scikit-learn). For OpenCV also install libgl1 libglib2.0-0.

ADDITIONAL RULES FOR PATH B (needs_wrapper=true):
- Regardless of base_image, setup_commands MUST install the compiler
  toolchain required by the detected manifest:
    * CMakeLists.txt -> apt-get install -y build-essential cmake
    * Cargo.toml     -> curl https://sh.rustup.rs -sSf | sh -s -- -y
                        (then source $HOME/.cargo/env in each subsequent
                        &&-chained command that needs cargo/rustc)
    * go.mod         -> apt-get install -y golang-go
    * Makefile       -> apt-get install -y build-essential
- If multiple manifests exist, install ALL required toolchains.
- If base_image is a CUDA image, it may lack python3; include
  apt-get install -y python3 python3-pip && ln -sf /usr/bin/python3 /usr/bin/python
  before any pip commands.
- setup_commands must NOT perform the actual build (cmake, cargo build,
  go build) — only install the toolchain. The wrapper_script does the build.

Output ONLY valid JSON matching this schema exactly, no markdown fences:
{{
  "base_image": "string",
  "setup_commands": "string",
  "needs_wrapper": false,
  "wrapper_script": null,
  "reasoning": "one sentence on why you chose this path"
}}"""

        logger.info(f"Triggering Gemini base-image generation for framework={profile.framework}")

        try:
            response = await self.client.aio.models.generate_content(
                model=self.model_id,
                contents=prompt,
            )
            text = response.text.replace('```json', '').replace('```', '').strip()
            result = json.loads(text)
        except Exception as e:
            logger.error(f"Gemini API failure during generation: {e}")
            raise ValueError("Failed to ask Gemini to generate container config")

        base_image = (result.get("base_image") or "").strip()
        if not base_image:
            base_image = (
                "nvidia/cuda:12.1.1-cudnn8-runtime-ubuntu22.04"
                if profile.gpu_required else "python:3.11-slim"
            )
        setup_commands = (result.get("setup_commands") or "").strip()

        needs_wrapper = result.get("needs_wrapper", False)
        wrapper_script = result.get("wrapper_script")
        reasoning = result.get("reasoning", "")

        # Validate wrapper script requirement
        if needs_wrapper:
            if not wrapper_script or not wrapper_script.strip():
                raise ValueError(
                    "Gemini indicated needs_wrapper=true but provided no wrapper_script. "
                    "Cannot proceed with broken container configuration."
                )
            logger.info(f"Gemini generated wrapper script for compiled workload: {reasoning}")

        logger.info(f"Gemini chose base={base_image} setup={setup_commands!r} needs_wrapper={needs_wrapper}")
        return GenerationResult(
            base_image=base_image,
            setup_commands=setup_commands,
            needs_wrapper=needs_wrapper,
            wrapper_script=wrapper_script,
            reasoning=reasoning,
        )
