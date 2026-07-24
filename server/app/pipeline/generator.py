"""Tier 3 AI Pipeline - Gemini base-image + dependency resolution.

For unknown codebases we ask Gemini to pick a real, pullable base image and the
shell commands needed to install dependencies. The commands run inside that base
image at container start (CatalogEntry.setup_commands), so there is no registry
or Kaniko build to manage.
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


class DockerfileGenerator:
    def __init__(self):
        self.client = genai.Client(api_key=settings.GEMINI_API_KEY)
        self.model_id = settings.GEMINI_MODEL

    async def generate(
        self,
        source_code: str,
        requirements_txt: str | None,
        profile: JobProfile,
    ) -> GenerationResult:
        """Pick a base image + dependency-install commands for an unknown codebase."""

        prompt = f"""You configure containers for a distributed GPU compute platform.
Pick a base image and the shell commands to install everything the code needs.

RULES:
- Base image MUST be a real, publicly pullable image. Use
  "nvidia/cuda:12.1.1-cudnn8-runtime-ubuntu22.04" if GPU is needed, else "python:3.11-slim".
- The base may not have pip/python preinstalled (CUDA images don't): if you pick a
  CUDA base, your commands MUST install python3 + pip first
  (apt-get update && apt-get install -y python3 python3-pip && ln -sf /usr/bin/python3 /usr/bin/python).
- Map import names to PyPI names (cv2 -> opencv-python, PIL -> pillow, sklearn -> scikit-learn).
- For OpenCV also: apt-get install -y libgl1 libglib2.0-0.
- Combine everything into ONE shell string joined with &&. Do NOT include the user's
  run command — we inject that separately.

GPU required: {profile.gpu_required}
Framework detected: {profile.framework}
Imports detected: {profile.imports or 'infer from source'}

requirements.txt provided:
{requirements_txt or 'None — infer from imports inside source code'}

source code summary (first 3000 chars):
{source_code[:3000]}

Output ONLY valid JSON matching this schema:
{{"base_image": "python:3.11-slim", "setup_commands": "pip install numpy pandas"}}"""

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
        logger.info(f"Gemini chose base={base_image} setup={setup_commands!r}")
        return GenerationResult(base_image=base_image, setup_commands=setup_commands)
