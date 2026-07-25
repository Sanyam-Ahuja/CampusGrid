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


# ── Assembly Helpers ─────────────────────────────────────────────────────────
#
# When a render job runs on a SINGLE node, the container compiles frames to
# MP4 locally before uploading — zero server-side work.
#
# When a job is split across MULTIPLE nodes, a separate lightweight Assembly
# Chunk is dispatched to any free node via the grid (see splitter.py).

BLENDER_SINGLE_NODE_COMPILE = (
    # Step 1: Try to install ffmpeg into the Blender container (it's debian-based)
    "&& (apt-get update -qq && apt-get install -y ffmpeg -qq 2>/dev/null || true) "
    # Step 2: Compile to MP4. Both success and fallback paths write to /tmp/final_render.mp4
    # so that the final curl upload always finds the file.
    "&& ("
    "  ffmpeg -y -framerate 24 -pattern_type glob -i '/tmp/frame_*.png' "
    "    -c:v libopenh264 -pix_fmt yuv420p -profile:v high /tmp/final_render.mp4 2>&1 "
    "  || ffmpeg -y -framerate 24 -pattern_type glob -i '/tmp/frame_*.png' "
    "    -c:v libx264 -pix_fmt yuv420p /tmp/final_render.mp4 2>&1 "
    "  || tar -czf /tmp/final_render.mp4 /tmp/frame_*.png"  # fallback: frames tar at same path
    ")"
)



def build_assembly_command(chunk_download_urls: list[str], final_upload_url: str) -> str:
    """
    Build the shell command for an Assembly Chunk.

    The assembly chunk runs in a lightweight alpine container on any free node.
    It downloads all render chunk tar.gz files from MinIO, extracts PNGs,
    compiles them with ffmpeg, and uploads the final MP4 — all on the node.
    No server-side CPU work required.
    """
    lines = [
        "apk add --no-cache curl tar ffmpeg python3 2>/dev/null || "
        "  (apt-get update -qq && apt-get install -y curl tar ffmpeg python3 -qq 2>/dev/null) || true",
        "mkdir -p /tmp/campugrid_frames",
    ]

    for i, url in enumerate(chunk_download_urls):
        lines.append(f"curl -sL '{url}' -o /tmp/chunk_{i}.tar.gz 2>&1 | tail -2")
        lines.append(
            f"tar -xzf /tmp/chunk_{i}.tar.gz -C /tmp/campugrid_frames 2>/dev/null || true"
        )
        lines.append(f"rm -f /tmp/chunk_{i}.tar.gz")

    lines += [
        # Find all .png files (may be nested) and move them flat into frames dir
        "find /tmp/campugrid_frames -name '*.png' -exec mv {{}} /tmp/campugrid_frames/ \\; 2>/dev/null || true",
        "echo 'CampusGrid Assembly: frames collected:'",
        "ls /tmp/campugrid_frames/*.png 2>/dev/null | wc -l || echo 0",
        # Compile with ffmpeg
        "ffmpeg -y -framerate 24 -pattern_type glob -i '/tmp/campugrid_frames/*.png' "
        "  -c:v libopenh264 -pix_fmt yuv420p -profile:v high /tmp/final_render.mp4 2>&1 "
        "|| ffmpeg -y -framerate 24 -pattern_type glob -i '/tmp/campugrid_frames/*.png' "
        "  -c:v libx264 -pix_fmt yuv420p /tmp/final_render.mp4 2>&1 "
        "|| (tar -czf /tmp/final_render.tar.gz /tmp/campugrid_frames/*.png && "
        "    mv /tmp/final_render.tar.gz /tmp/final_render.mp4)",  # last-resort
        # Upload the final output
        f"curl --upload-file /tmp/final_render.mp4 '{final_upload_url}'",
        "echo 'CampusGrid Assembly: upload complete!'",
    ]

    return " && ".join(lines)




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
            "--python-expr \"import base64; exec(base64.b64decode(b'aW1wb3J0IGJweQpzY2VuZSA9IGJweS5jb250ZXh0LnNjZW5lCnNjZW5lLnJlbmRlci5lbmdpbmUgPSAnQ1lDTEVTJwpwcmVmcyA9IGJweS5jb250ZXh0LnByZWZlcmVuY2VzLmFkZG9uc1snY3ljbGVzJ10ucHJlZmVyZW5jZXMKY2hvc2VuID0gTm9uZQpmb3IgdCBpbiAoJ0NVREEnLCAnT1BUSVgnLCAnSElQJywgJ09ORUFQSScsICdNRVRBTCcpOgogICAgdHJ5OgogICAgICAgIHByZWZzLmNvbXB1dGVfZGV2aWNlX3R5cGUgPSB0CiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIGNvbnRpbnVlCiAgICB0cnk6CiAgICAgICAgcHJlZnMuZ2V0X2RldmljZXMoKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICBncHVz = W2QgZm9yIGQgaW4gcHJlZnMuZGV2aWNlcyBpZiBkLnR5cGUgPT0gdF0KICAgIGlmIGdwdXM6CiAgICAgICAgZm9yIGQgaW4gcHJlZnMuZGV2aWNlczoKICAgICAgICAgICAgZC51c2UgPSAoZC50eXBlID09IHQpCiAgICAgICAgY2hvc2VuID0gdAogICAgICAgIGJyZWFrCmlmIGNob3NlbjoKICAgIHNjZW5lLmN5Y2xlcy5kZXZpY2UgPSAnR1BVJwogICAgcHJpbnQoJ0NhbXB1R3JpZDogQ3ljbGVzIEdQVSBlbmFibGVkIHZpYSAnICsgY2hvc2VuKQplbHNlOgogICAgc2NlbmUuY3ljbGVzLmRldmljZSA9ICdDUFUnCiAgICBwcmludCgnQ2FtcHVHcmlkOiBubyBHUFUgY29tcHV0ZSBkZXZpY2UgZm91bmQsIHJlbmRlcmluZyBvbiBDUFUnKQ==').decode('utf-8'))\" "
            "-F PNG -o /tmp/frame_#### -s {CHUNK_START} -e {CHUNK_END} -a "
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
