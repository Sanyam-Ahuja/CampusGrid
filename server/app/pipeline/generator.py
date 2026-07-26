"""Tier 3 AI Pipeline - Gemini base-image + dependency resolution.

For unknown codebases we ask Gemini to pick a real, pullable base image and the
shell commands needed to install dependencies. For compiled or complex workloads,
Gemini generates a Python wrapper script that handles the full build and run steps.
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
        error_log: str | None = None,
    ) -> GenerationResult:
        """Ask Gemini to figure out how to make a container run this codebase."""
        if build_files is None:
            build_files = {}

        # Derive the project subdirectory (e.g. "llamacpp-test/foo.cpp" -> "/input/llamacpp-test")
        entry_parts = profile.entry_file.replace("\\", "/").split("/")
        project_dir = "/input/" + entry_parts[0] if len(entry_parts) > 1 else "/input"

        manifests = "\n".join(
            f"--- {name} ---\n{content}"
            for name, content in build_files.items()
            if content and content.strip() != "(not found)"
        ) or "(none found)"

        prompt = f"""You are configuring a Docker container for a distributed compute platform.
A user has uploaded a code repository and wants it to run on GPU compute nodes.

PLATFORM FACTS (non-negotiable):
- The zip archive is extracted to /input/ before the container starts.
- The project itself lives at: {project_dir}
- /output/ already exists and is where results should be written.
- The container will execute: python {profile.entry_file}
  (This is fixed. If the entry file is not Python, write a wrapper script
   that the platform can call as `python _campugrid_wrapper.py`.)

WHAT WE KNOW ABOUT THIS JOB:
- Detected framework: {profile.framework}
- GPU required: {profile.gpu_required}
- Entry file: {profile.entry_file}

BUILD MANIFESTS FOUND IN THE REPO:
{manifests}

REQUIREMENTS.TXT:
{requirements_txt or '(not found)'}

ENTRY FILE SOURCE (first 3000 chars):
{source_code[:3000]}
"""

        if error_log:
            prompt += f"""
PREVIOUS ATTEMPT FAILED — here is everything from that attempt:
<previous_attempt>
{error_log}
</previous_attempt>

Figure out what went wrong and fix it. You have full freedom.
"""

        prompt += f"""
YOUR JOB:
Choose a Docker base image and setup commands that will make this code run.
You have complete freedom — choose whatever base image, install whatever you need,
do whatever it takes. The only constraint is:

  The platform will call: python {profile.entry_file}
  If that won't work (e.g. it's a C++ binary, Rust project, etc.), set
  needs_wrapper=true and write a Python 3 wrapper script at _campugrid_wrapper.py
  that does the build and runs the result. The platform will then call:
  python _campugrid_wrapper.py

In the wrapper script:
- You can use subprocess, os, shutil — anything you want.
- The project is at {project_dir}
- Input data is under /input/, write output to /output/
- Let errors propagate (don't swallow exceptions)
- No interactive prompts

Output ONLY valid JSON, no markdown:
{{
  "base_image": "a real, pullable Docker image tag",
  "setup_commands": "shell commands to run before the entry file, joined with &&",
  "needs_wrapper": false,
  "wrapper_script": null,
  "reasoning": "one sentence"
}}"""

        logger.info(f"Triggering Gemini container config generation for framework={profile.framework}")

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

        if needs_wrapper:
            if not wrapper_script or not wrapper_script.strip():
                raise ValueError(
                    "Gemini indicated needs_wrapper=true but provided no wrapper_script."
                )
            logger.info(f"Gemini generated wrapper: {reasoning}")

        logger.info(f"Gemini chose base={base_image} needs_wrapper={needs_wrapper} setup={setup_commands!r}")
        return GenerationResult(
            base_image=base_image,
            setup_commands=setup_commands,
            needs_wrapper=needs_wrapper,
            wrapper_script=wrapper_script,
            reasoning=reasoning,
        )
