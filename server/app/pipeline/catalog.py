"""Step 3: Tier 1 Docker Config Matcher."""

from dataclasses import dataclass

from app.pipeline.analyzer import JobProfile


@dataclass
class CatalogEntry:
    image: str
    entrypoint_template: str
    env_vars: list[str]
    gpu_required: bool
    preinstalled_packages: list[str]
    tested: bool
    # Extra shell run inside the container before the workload (e.g. the
    # `pip install ...` produced by the Tier-2 adapter or Tier-3 generator).
    # The splitter prepends this to every chunk command, so we never need a
    # registry or a Kaniko build — the image stays a real, pullable base image.
    setup_commands: str = ""


# Generic entrypoint for Gemini-generated / custom-Dockerfile Python jobs:
# pull input, run the detected script, push output. Dependency installs are
# injected via CatalogEntry.setup_commands.
GENERIC_PYTHON_ENTRYPOINT = (
    "apt-get install -y unzip curl -qq 2>/dev/null || true "
    "&& curl -sL '{INPUT_URL}' -o /tmp/input_archive "
    "&& mkdir -p /input "
    "&& (unzip -o /tmp/input_archive -d /input 2>/dev/null || cp /tmp/input_archive /input/{INPUT}) "
    "&& mkdir -p /output/checkpoints "
    "&& cd /input && python {INPUT} "
    "&& tar -czf /tmp/output.tar.gz -C /output . "
    "&& curl -T /tmp/output.tar.gz '{UPLOAD_URL}'"
)


CATALOG: dict[tuple, CatalogEntry] = {
    ("render", "blender", True): CatalogEntry(
        image="lscr.io/linuxserver/blender:latest",
        entrypoint_template=(
            "export PATH=\"/usr/bin:/usr/local/bin:$PATH\" "
            "&& mkdir -p /input "
            "&& curl -sL '{INPUT_URL}' -o /tmp/downloaded_file "
            "&& python3 -c \"\n"
            "import zipfile, os, shutil\n"
            "src = '/tmp/downloaded_file'\n"
            "print('Downloaded file size:', os.path.getsize(src))\n"
            "if zipfile.is_zipfile(src):\n"
            "    print('Valid zipfile detected. Unzipping...')\n"
            "    with zipfile.ZipFile(src) as z: z.extractall('/input')\n"
            "else:\n"
            "    print('Not a zipfile. First 200 bytes:')\n"
            "    try:\n"
            "        with open(src, 'rb') as f: print(f.read(200))\n"
            "    except Exception as e: print(e)\n"
            "    os.makedirs(os.path.dirname('/input/{INPUT}'), exist_ok=True)\n"
            "    shutil.copy(src, '/input/{INPUT}')\n"
            "\" "
            "&& cd /input "
            "&& blender -b --enable-autoexec '{INPUT}' "
            "--python-expr \"import base64; exec(base64.b64decode(b'aW1wb3J0IGJweQpzY2VuZSA9IGJweS5jb250ZXh0LnNjZW5lCnNjZW5lLnJlbmRlci5lbmdpbmUgPSAnQ1lDTEVTJwp0cnk6CiAgICBzY2VuZS5yZW5kZXIuaW1hZ2Vfc2V0dGluZ3MuZmlsZV9mb3JtYXQgPSAnUE5HJwpleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICBwcmludCgnQ2FtcHVHcmlkIFdhcm5pbmc6IEZhaWxlZCB0byBzZXQgZmlsZV9mb3JtYXQgdG8gUE5HOicsIGUpCiAgICB0cnk6CiAgICAgICAgcHJpbnQoJ0FsbG93ZWQgZm9ybWF0czonLCBsaXN0KHNjZW5lLnJlbmRlci5pbWFnZV9zZXR0aW5ncy5ibF9ybmEucHJvcGVydGllc1snZmlsZV9mb3JtYXQnXS5lbnVtX2l0ZW1zLmtleXMoKSkpCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKdHJ5OgogICAgc2NlbmUucmVuZGVyLmltYWdlX3NldHRpbmdzLmNvbG9yX21vZGUgPSAnUkdCQScKZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgcHJpbnQoJ0NhbXB1R3JpZCBXYXJuaW5nOiBGYWlsZWQgdG8gc2V0IGNvbG9yX21vZGUgdG8gUkdCQTonLCBlKQoKcHJlZnMgPSBiaHkuY29udGV4dC5wcmVmZXJlbmNlcy5hZGRvbnNbJ2N5Y2xlcyddLnByZWZlcmVuY2VzCmNob3NlbiA9IE5vbmUKZm9yIHQgaW4gKCdPUFRJWCcsICdDVURBJywgJ0hJUCcsICdPTkVBUEknLCAnTUVUQUwnKToKICAgIHRyeToKICAgICAgICBwcmVmcy5jb21wdXRlX2RldmljZV90eXBlID0gdAogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBjb250aW51ZQogICAgdHJ5OgogICAgICAgIHByZWZzLmdldF9kZXZpY2VzKCkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgZ3B1cyA9IFtkIGZvciBkIGluIHByZWZzLmRldmljZXMgaWYgZC50eXBlID09IHRdCiAgICBpZiBncHVzOgogICAgICAgIGZvciBkIGluIHByZWZzLmRldmljZXM6CiAgICAgICAg---ICAgIGQudXNlID0gKGQudHlwZSA9PSB0KQogICAgICAgIGNob3NlbiA9IHQKICAgICAgICBicmVhawppZiBjaG9zZW46CiAgICBzY2VuZS5jeWNsZXMuZGV2aWNlID0gJ0dQVScKICAgIHByaW50KCdDYW1wdUdyaWQ6IEN5Y2xlcyBHUFUgZW5hYmxlZCB2aWEgJyArIGNob3NlbikKZWxzZToKICAgIHNjZW5lLmN5Y2xlcy5kZXZpY2UgPSAnQ1BVJwogICAgcHJpbnQoJ0NhbXB1R3JpZDogbm8gR1BVIGNvbXB1dGUgZGV2aWNlIGZvdW5kLCByZW5kZXJpbmcgb24gQ1BVJyk=').decode('utf-8'))\" "
            "-o /tmp/frame_#### -s {CHUNK_START} -e {CHUNK_END} -a "
            "&& tar -czf /tmp/output.tar.gz /tmp/frame_* "
            "&& curl -T /tmp/output.tar.gz '{UPLOAD_URL}'"
        ),
        env_vars=["INPUT", "CHUNK_START", "CHUNK_END", "OUTPUT_PATH"],
        gpu_required=True,
        preinstalled_packages=[],
        tested=True,
    ),
    ("ml_training", "pytorch", True): CatalogEntry(
        image="pytorch/pytorch:2.2.0-cuda12.1-cudnn8-runtime",
        entrypoint_template=(
            "apt-get install -y unzip curl -qq 2>/dev/null || true "
            "&& curl -sL '{INPUT_URL}' -o /tmp/input_archive "
            "&& mkdir -p /input "
            "&& (unzip -o /tmp/input_archive -d /input 2>/dev/null || cp /tmp/input_archive /input/{INPUT}) "
            "&& mkdir -p /output/checkpoints "
            "&& cd /input && python {INPUT} "
            "&& tar -czf /tmp/output.tar.gz -C /output . "
            "&& curl -T /tmp/output.tar.gz '{UPLOAD_URL}'"
        ),
        env_vars=["INPUT", "OUTPUT_PATH", "SYNC_MODE", "CHECKPOINT_INTERVAL", "JOB_ID", "CHUNK_ID"],
        gpu_required=True,
        preinstalled_packages=[
            "torch==2.2.0", "torchvision==0.17.0", "numpy==1.26.4",
            "pandas==2.2.0"
        ],
        tested=True,
    ),
    ("ml_training", "tensorflow", True): CatalogEntry(
        image="tensorflow/tensorflow:2.16.1-gpu",
        entrypoint_template=(
            "apt-get install -y unzip curl -qq 2>/dev/null || true "
            "&& curl -sL '{INPUT_URL}' -o /tmp/input_archive "
            "&& mkdir -p /input "
            "&& (unzip -o /tmp/input_archive -d /input 2>/dev/null || cp /tmp/input_archive /input/{INPUT}) "
            "&& mkdir -p /output/checkpoints "
            "&& cd /input && python {INPUT} "
            "&& tar -czf /tmp/output.tar.gz -C /output . "
            "&& curl -T /tmp/output.tar.gz '{UPLOAD_URL}'"
        ),
        env_vars=["INPUT", "OUTPUT_PATH", "SYNC_MODE", "JOB_ID"],
        gpu_required=True,
        preinstalled_packages=[
            "tensorflow==2.16.1", "numpy==1.26.4", "pandas==2.2.0",
            "keras==3.0.0",
        ],
        tested=False,
    ),
    ("ml_training", "jax", True): CatalogEntry(
        image="google-deepmind/jax:latest",
        entrypoint_template=(
            "apt-get install -y unzip curl -qq 2>/dev/null || true "
            "&& curl -sL '{INPUT_URL}' -o /tmp/input_archive "
            "&& mkdir -p /input "
            "&& (unzip -o /tmp/input_archive -d /input 2>/dev/null || cp /tmp/input_archive /input/{INPUT}) "
            "&& mkdir -p /output/checkpoints "
            "&& cd /input && python {INPUT} "
            "&& tar -czf /tmp/output.tar.gz -C /output . "
            "&& curl -T /tmp/output.tar.gz '{UPLOAD_URL}'"
        ),
        env_vars=["INPUT", "OUTPUT_PATH", "JOB_ID"],
        gpu_required=True,
        preinstalled_packages=[
            "jax==0.4.25", "jaxlib==0.4.25", "flax==0.8.1",
            "optax==0.1.9", "numpy==1.26.4",
        ],
        tested=False,
    ),
    ("data", "python-data", False): CatalogEntry(
        image="python:3.11-slim",
        entrypoint_template=(
            "apt-get install -y unzip curl -qq 2>/dev/null || true "
            "&& curl -sL '{INPUT_URL}' -o /tmp/input_archive "
            "&& mkdir -p /input "
            "&& (unzip -o /tmp/input_archive -d /input 2>/dev/null || cp /tmp/input_archive /input/{INPUT}) "
            "&& mkdir -p /output "
            "&& pip install pandas numpy scipy -q "
            "&& cd /input && python {INPUT} "
            "&& tar -czf /tmp/output.tar.gz -C /output . "
            "&& curl -T /tmp/output.tar.gz '{UPLOAD_URL}'"
        ),
        env_vars=["INPUT", "OUTPUT_PATH", "CHUNK_START", "CHUNK_END"],
        gpu_required=False,
        preinstalled_packages=[
            "pandas==2.2.0", "numpy==1.26.4", "scipy==1.12.0"
        ],
        tested=True,
    ),
    # ── Simulation Images ──────────────────────────────────────
    ("simulation", "openfoam", False): CatalogEntry(
        image="campugrid/openfoam:2312",
        entrypoint_template="cd /workspace/case && decomposePar -force && mpirun -np 1 simpleFoam -parallel",
        env_vars=["MPI_RANK", "MPI_SIZE", "PROCESSOR_DIR"],
        gpu_required=False,
        preinstalled_packages=["openfoam-2312", "openmpi"],
        tested=False,
    ),
    ("simulation", "lammps", True): CatalogEntry(
        image="campugrid/lammps:gpu",
        entrypoint_template="lmp -in /workspace/{INPUT} -partition {CHUNK_END}x1",
        env_vars=["INPUT", "MPI_RANK", "MPI_SIZE"],
        gpu_required=True,
        preinstalled_packages=["lammps", "openmpi"],
        tested=False,
    ),
    ("simulation", "gromacs", False): CatalogEntry(
        image="campugrid/gromacs:2024",
        entrypoint_template="gmx mdrun -s /workspace/{INPUT} -dd {CHUNK_END} 1 1",
        env_vars=["INPUT", "MPI_RANK", "MPI_SIZE"],
        gpu_required=False,
        preinstalled_packages=["gromacs", "openmpi"],
        tested=False,
    ),
}


def parse_dockerfile(text: str) -> tuple[str | None, str]:
    """Extract (base_image, setup_commands) from a user-supplied Dockerfile.

    We only honour FROM (base image) and RUN (install steps). COPY/ENTRYPOINT/CMD
    are ignored because CampuGrid injects its own input handling and run wrapper.
    Line continuations (trailing backslash) are joined.
    """
    # Join backslash line-continuations into single logical lines.
    logical: list[str] = []
    buf = ""
    for raw in text.splitlines():
        line = raw.rstrip()
        if line.endswith("\\"):
            buf += line[:-1] + " "
            continue
        buf += line
        logical.append(buf)
        buf = ""
    if buf:
        logical.append(buf)

    base_image: str | None = None
    runs: list[str] = []
    for line in logical:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        upper = stripped.upper()
        if upper.startswith("FROM ") and base_image is None:
            # FROM image[:tag] [AS stage]
            parts = stripped.split()
            if len(parts) >= 2:
                base_image = parts[1]
        elif upper.startswith("RUN "):
            runs.append(stripped[4:].strip())

    setup_commands = " && ".join(r for r in runs if r)
    return base_image, setup_commands


def lookup(profile: JobProfile) -> CatalogEntry | None:
    """Return an exact matching CatalogEntry or None if verification is needed."""
    key = (profile.type, profile.framework, profile.gpu_required)

    # Standard lookup
    if key in CATALOG:
        return CATALOG[key]

    # Provide fallback options for ml_training CPU variants if GPU is not needed but we only have GPU containers
    fallback_key = (profile.type, profile.framework, True)
    if not profile.gpu_required and fallback_key in CATALOG:
        # Give them the GPU image but without passing --gpus device
        return CATALOG[fallback_key]

    return None
