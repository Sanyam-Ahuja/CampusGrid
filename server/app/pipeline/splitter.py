"""Step 6: Chunk computation based on specific workloads."""

import logging
from dataclasses import dataclass

from app.pipeline.analyzer import JobProfile, Resources

logger = logging.getLogger(__name__)


@dataclass
class ChunkSpec:
    chunk_index: int
    chunk_start: int | float
    chunk_end: int | float
    command: str
    env_vars: dict[str, str]
    resources: Resources
    network_mode: str = "none"
    requires_public_network: bool = False


def compute_chunks(profile: JobProfile, available_nodes: int, catalog_entry, requires_public_network: bool, job_id: str) -> list[ChunkSpec]:
    profile.requires_public_network = requires_public_network
    if profile.type == "render":
        chunks = split_render(profile, available_nodes, catalog_entry, job_id)
    elif profile.type == "ml_training":
        chunks = split_ml(profile, available_nodes, catalog_entry, job_id)
    elif profile.type == "data":
        chunks = split_data(profile, available_nodes, catalog_entry, job_id)
    elif profile.type == "simulation":
        chunks = split_simulation(profile, available_nodes, catalog_entry, job_id)
    else:
        chunks = []

    # Prepend any runtime dependency-install commands (Tier-2 adapter / Tier-3
    # generator / custom Dockerfile) so they run inside the container before the
    # workload. This is what lets us use real base images with no registry.
    setup = (getattr(catalog_entry, "setup_commands", "") or "").strip()
    if setup:
        for c in chunks:
            c.command = f"{setup} && {c.command}"
    return chunks

def split_render(profile: JobProfile, available_nodes: int, catalog_entry, job_id: str) -> list[ChunkSpec]:
    """Frame-range parallelism for rendering workloads."""
    start = int(profile.split_params.get("frame_start", 1))
    end = int(profile.split_params.get("frame_end", 250))
    total_frames = max(1, end - start + 1)

    # Simple chunking logic: map exactly to online devices.
    num_chunks = min(available_nodes, total_frames) if available_nodes > 0 else 1
    
    # If no devices are online yet, don't just create 1 massive chunk!
    # Instead, partition it into chunks of 50 frames to be queued up for nodes when they start.
    if available_nodes == 0 and total_frames > 50:
        num_chunks = max(1, total_frames // 50)

    frames_per_chunk = total_frames // num_chunks
    chunks = []

    for i in range(num_chunks):
        chunk_start = start + (i * frames_per_chunk)
        chunk_end = start + ((i + 1) * frames_per_chunk) - 1

        # Last chunk sweeps up remainder
        if i == num_chunks - 1:
            chunk_end = end

        from app.services.minio_service import minio_service
        from app.core.config import get_settings
        settings = get_settings()

        minio_key = profile.split_params.get("minio_key", profile.entry_file)
        input_url = minio_service.get_presigned_url(settings.BUCKET_JOB_INPUTS, minio_key, expiry_hours=4)
        output_key = f"{job_id}/chunk_{i}.tar.gz"
        upload_url = minio_service.get_presigned_upload_url(settings.BUCKET_JOB_OUTPUTS, output_key, expiry_hours=4)

        cmd = catalog_entry.entrypoint_template.replace("{INPUT}", profile.entry_file)
        cmd = cmd.replace("{INPUT_URL}", input_url).replace("{UPLOAD_URL}", upload_url)
        cmd = cmd.replace("{CHUNK_START}", str(chunk_start)).replace("{CHUNK_END}", str(chunk_end))

        chunks.append(ChunkSpec(
            chunk_index=i+1,
            chunk_start=chunk_start,
            chunk_end=chunk_end,
            command=cmd,
            env_vars={
                "CHUNK_START": str(chunk_start),
                "CHUNK_END": str(chunk_end),
            },
            resources=profile.resources,
            network_mode="host",  # host networking so container can reach localhost:9000 MinIO directly
            requires_public_network=getattr(profile, 'requires_public_network', False)
        ))

    return chunks


def split_ml(profile: JobProfile, available_nodes: int, catalog_entry, job_id: str) -> list[ChunkSpec]:
    """Data parallelism logic for ML. Every node gets same model script but separate data seeds."""
    num_chunks = min(available_nodes, 4) if available_nodes > 0 else 1

    chunks = []
    for i in range(num_chunks):
        from app.services.minio_service import minio_service
        from app.core.config import get_settings
        settings = get_settings()

        minio_key = profile.split_params.get("minio_key", profile.entry_file)
        input_url = minio_service.get_presigned_url(settings.BUCKET_JOB_INPUTS, minio_key, expiry_hours=4)
        output_key = f"{job_id}/chunk_{i}.tar.gz"
        upload_url = minio_service.get_presigned_upload_url(settings.BUCKET_JOB_OUTPUTS, output_key, expiry_hours=4)

        cmd = catalog_entry.entrypoint_template.replace("{INPUT}", profile.entry_file)
        cmd = cmd.replace("{INPUT_URL}", input_url).replace("{UPLOAD_URL}", upload_url)

        # We assign an implicit worker ID via CHUNK_START mapped as rank
        chunks.append(ChunkSpec(
            chunk_index=i+1,
            chunk_start=i,
            chunk_end=i,
            command=cmd,
            env_vars={
                "WORLD_SIZE": str(num_chunks),
                "RANK": str(i),
            },
            resources=profile.resources,
            # If DDP is requested, we need network. Else local_sgd is basically 'none' but needs periodic sync?
            # Actually local_sgd means we just run individually and merge later via MinIO.
            # We'll stick to 'campugrid_overlay' for standard ML if they need sync
            network_mode="campugrid_overlay",
            requires_public_network=getattr(profile, 'requires_public_network', False)
        ))

    return chunks


def split_data(profile: JobProfile, available_nodes: int, catalog_entry, job_id: str) -> list[ChunkSpec]:
    """Shard CSV/Parquet by byte ranges for map-reduce processing."""
    from app.services.minio_service import minio_service
    from app.core.config import get_settings
    settings = get_settings()

    file_size = profile.split_params.get("file_size", 0)
    if file_size <= 0:
        # Single-shard fallback: build the command the same way the multi-shard
        # path does (.replace, not .format) so the {INPUT_URL}/{UPLOAD_URL}
        # placeholders in the template don't blow up with KeyError.
        minio_key = profile.split_params.get("minio_key", profile.entry_file)
        input_url = minio_service.get_presigned_url(settings.BUCKET_JOB_INPUTS, minio_key, expiry_hours=4)
        output_key = f"{job_id}/chunk_0.tar.gz"
        upload_url = minio_service.get_presigned_upload_url(settings.BUCKET_JOB_OUTPUTS, output_key, expiry_hours=4)

        cmd = (catalog_entry.entrypoint_template
               .replace("{INPUT}", profile.entry_file)
               .replace("{CHUNK_START}", "0").replace("{CHUNK_END}", "0")
               .replace("{OUTPUT_PATH}", "/output")
               .replace("{INPUT_URL}", input_url).replace("{UPLOAD_URL}", upload_url))

        return [ChunkSpec(
            chunk_index=1, chunk_start=0, chunk_end=0,
            command=cmd,
            env_vars={"INPUT": profile.entry_file, "CHUNK_START": "0", "CHUNK_END": "0"},
            resources=profile.resources, network_mode="none",
            requires_public_network=getattr(profile, 'requires_public_network', False),
        )]

    num_chunks = min(available_nodes, 8) if available_nodes > 0 else 1
    target_shard_size = file_size // num_chunks

    chunks = []
    for i in range(num_chunks):
        start_byte = i * target_shard_size
        end_byte = (i + 1) * target_shard_size if i < num_chunks - 1 else file_size

        from app.services.minio_service import minio_service
        from app.core.config import get_settings
        settings = get_settings()

        minio_key = profile.split_params.get("minio_key", profile.entry_file)
        input_url = minio_service.get_presigned_url(settings.BUCKET_JOB_INPUTS, minio_key, expiry_hours=4)
        output_key = f"{job_id}/chunk_{i}.tar.gz"
        upload_url = minio_service.get_presigned_upload_url(settings.BUCKET_JOB_OUTPUTS, output_key, expiry_hours=4)

        cmd = catalog_entry.entrypoint_template.replace("{INPUT}", profile.entry_file).replace("{CHUNK_START}", str(start_byte)).replace("{CHUNK_END}", str(end_byte)).replace("{OUTPUT_PATH}", "/output")
        cmd = cmd.replace("{INPUT_URL}", input_url).replace("{UPLOAD_URL}", upload_url)

        chunks.append(ChunkSpec(
            chunk_index=i+1,
            chunk_start=start_byte,
            chunk_end=end_byte,
            command=cmd,
            env_vars={
                "INPUT": profile.entry_file,
                "CHUNK_START": str(start_byte),
                "CHUNK_END": str(end_byte),
            },
            resources=profile.resources,
            network_mode="none",
            requires_public_network=getattr(profile, 'requires_public_network', False)
        ))

    return chunks


def split_simulation(profile: JobProfile, available_nodes: int, catalog_entry, job_id: str) -> list[ChunkSpec]:
    """Domain decomposition for simulation workloads (OpenFOAM, LAMMPS, GROMACS).

    For OpenFOAM: maps to decomposePar processor directories.
    For LAMMPS: spatial decomposition via -partition flag.
    For GROMACS: domain decomposition via -dd flag.
    """
    # Simulations are typically limited to power-of-2 or factor-based decompositions
    num_chunks = min(available_nodes, 4) if available_nodes > 0 else 1

    # Prefer powers of 2 for MPI decomposition
    valid_decomps = [1, 2, 4, 8, 16]
    num_chunks = max(d for d in valid_decomps if d <= num_chunks)

    chunks = []
    for i in range(num_chunks):
        if profile.framework == "openfoam":
            cmd = "cd /workspace/case && decomposePar -force && mpirun -np 1 simpleFoam -parallel -case /workspace/case"
            env_vars = {
                "MPI_RANK": str(i),
                "MPI_SIZE": str(num_chunks),
                "PROCESSOR_DIR": f"processor{i}",
            }
        elif profile.framework == "lammps":
            input_file = profile.split_params.get("input_file", "in.lammps")
            cmd = f"lmp -in /workspace/{input_file} -partition {num_chunks}x1"
            env_vars = {
                "MPI_RANK": str(i),
                "MPI_SIZE": str(num_chunks),
            }
        elif profile.framework == "gromacs":
            tpr_file = profile.split_params.get("tpr_file", "topol.tpr")
            cmd = f"gmx mdrun -s /workspace/{tpr_file} -dd {num_chunks} 1 1"
            env_vars = {
                "MPI_RANK": str(i),
                "MPI_SIZE": str(num_chunks),
            }
        else:
            cmd = catalog_entry.entrypoint_template.replace("{INPUT}", profile.entry_file).replace("{CHUNK_START}", str(i)).replace("{CHUNK_END}", str(i)).replace("{OUTPUT_PATH}", "/output")
            env_vars = {"MPI_RANK": str(i), "MPI_SIZE": str(num_chunks)}
            
        from app.services.minio_service import minio_service
        from app.core.config import get_settings
        settings = get_settings()

        minio_key = profile.split_params.get("minio_key", profile.entry_file)
        input_url = minio_service.get_presigned_url(settings.BUCKET_JOB_INPUTS, minio_key, expiry_hours=4)
        output_key = f"{job_id}/chunk_{i}.tar.gz"
        upload_url = minio_service.get_presigned_upload_url(settings.BUCKET_JOB_OUTPUTS, output_key, expiry_hours=4)

        cmd = cmd.replace("{INPUT_URL}", input_url).replace("{UPLOAD_URL}", upload_url)

        chunks.append(ChunkSpec(
            chunk_index=i + 1,
            chunk_start=i,
            chunk_end=i,
            command=cmd,
            env_vars=env_vars,
            resources=profile.resources,
            network_mode="campugrid_overlay",  # MPI needs inter-node comms
            requires_public_network=getattr(profile, 'requires_public_network', False)
        ))

    return chunks
