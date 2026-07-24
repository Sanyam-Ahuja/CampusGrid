"""Tier 2 AI Pipeline - Gemini Dockerfile generation."""

import json
import logging
from dataclasses import dataclass

from google import genai

from app.core.config import get_settings
from app.pipeline.catalog import CatalogEntry

logger = logging.getLogger(__name__)
settings = get_settings()

@dataclass
class AdaptationResult:
    needs_adaptation: bool
    image: str | None = None
    # Shell commands (e.g. "pip install opencv-python wandb") to run inside the
    # base image at container start. No Kaniko/registry build required.
    commands: str | None = None
    compatible: bool = True
    conflicts: list[str] | None = None


class DockerConfigVerifier:
    def __init__(self):
        self.client = genai.Client(api_key=settings.GEMINI_API_KEY)
        self.model_id = settings.GEMINI_MODEL

    async def verify_and_adapt(
        self,
        catalog_entry: CatalogEntry,
        user_imports: list[str] | None,
        requirements_txt: str | None = None
    ) -> AdaptationResult:
        """Check if catalog works with user's code via Gemini AI."""

        user_imports = user_imports or []
        pre_installed = {pkg.split("==")[0] for pkg in catalog_entry.preinstalled_packages}

        # If no user imports are provided or they are all preinstalled, we are fine
        missing = set(user_imports) - pre_installed
        # Also, exclude standard standard lib imports just to be safe, but actually Gemini can figure that out

        if not missing and not requirements_txt:
            return AdaptationResult(needs_adaptation=False, image=catalog_entry.image)

        logger.info(f"Triggering Gemini AI due to unrecognized imports: {missing}")

        prompt = f"""You are a strict Docker dependency resolver for a GPU grid scale platform.

Base image: {catalog_entry.image}
Pre-installed packages: {json.dumps(catalog_entry.preinstalled_packages)}

Student code requires these ADDITIONAL packages inferred from imports: {json.dumps(list(missing))}
Student's requirements.txt: {requirements_txt or 'Not provided'}

Tasks:
1. Ignore standard python libraries like `sys`, `os`, `json`, etc.
2. Ensure you map python package names to PyPi correctly e.g., 'cv2' -> 'opencv-python'.
3. Ignore anything that is already inside `Pre-installed packages`.
6. IMPORTANT: If a module name is very generic (e.g., 'models', 'utils', 'config', 'data', 'train', 'src'), ASSUME it is a local file or folder inside the student's project! DO NOT fail compatibility for these. Treat them as successfully resolved local imports and exclude them from the pip string.
7. If yes, generate an apt-get/pip string.

Output ONLY valid JSON matching this schema:
{{"compatible": true, "commands": "pip install opencv-python wandb", "conflicts": []}}
or
{{"compatible": false, "commands": null, "conflicts": ["reason logic fails"]}}
"""

        try:
            response = await self.client.aio.models.generate_content(
                model=self.model_id,
                contents=prompt,
            )
            # Find JSON block
            text = response.text.replace('```json', '').replace('```', '').strip()
            result = json.loads(text)
        except Exception as e:
            logger.error(f"Gemini API failure: {e}")
            return AdaptationResult(needs_adaptation=True, compatible=False, conflicts=[str(e)])

        if result.get("compatible"):
            cmds = (result.get("commands") or "").strip()

            # If no actual commands were needed (e.g., they were all stdlib), just use base image
            if not cmds:
                return AdaptationResult(needs_adaptation=False, image=catalog_entry.image)

            # Run the install commands inside the real base image at container
            # start. No registry, no Kaniko — the image stays pullable.
            logger.info(f"Adapter resolved extra deps via runtime install: {cmds}")
            return AdaptationResult(
                needs_adaptation=True,
                image=catalog_entry.image,
                commands=cmds,
            )

        else:
            return AdaptationResult(
                needs_adaptation=True,
                conflicts=result.get("conflicts", ["Unknown exact dependency collision"]),
                compatible=False
            )
