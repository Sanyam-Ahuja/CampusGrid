"""Simulation Assembler — Domain reconstruction from subdomains.

After all simulation chunks complete, this assembler:
1. Downloads output data from each subdomain
2. Runs post-processing merge (reconstructPar for OpenFOAM, trajectory merge for LAMMPS/GROMACS)
3. Uploads merged results to MinIO
"""

import logging
import os

from asgiref.sync import async_to_sync
from sqlalchemy import select

import json
import redis.asyncio as aioredis
from app.celery_worker import celery_app as celery
from app.core.config import get_settings
from app.core.database import make_celery_session
from app.models.chunk import Chunk, ChunkStatus
from app.models.job import Job, JobStatus
from app.scheduler.network_manager import network_manager
from app.services.minio_service import minio_service

logger = logging.getLogger(__name__)
settings = get_settings()


async def process_sim_assembly_async(job_id: str):
    """Reconstruct simulation results from subdomain outputs."""

    async with make_celery_session() as session:
        job_result = await session.execute(select(Job).where(Job.id == job_id))
        job = job_result.scalar_one_or_none()
        if not job:
            return

        chunks_res = await session.execute(
            select(Chunk).where(Chunk.job_id == job_id).order_by(Chunk.chunk_index)
        )
        chunks = chunks_res.scalars().all()

        if any(c.status != ChunkStatus.COMPLETED for c in chunks):
            logger.warning(f"Sim assembler called but not all chunks complete for {job_id}")
            return

        job.status = JobStatus.ASSEMBLING
        framework = (job.profile or {}).get("framework", "unknown")
        await session.commit()

    r = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    await r.publish("job_updates", json.dumps({
        "type": "detection_step",
        "job_id": str(job_id),
        "step": "assembling",
        "detail": f"Reconstructing simulation domains ({framework})..."
    }))

    temp_dir = f"/tmp/campugrid_sim_assemble_{job_id}"
    os.makedirs(temp_dir, exist_ok=True)

    # Download all output shards from MinIO
    prefix = f"{job_id}/"
    output_keys = minio_service.list_objects(settings.BUCKET_JOB_OUTPUTS, prefix=prefix)

    local_files = []
    for key in sorted(output_keys):
        local_path = os.path.join(temp_dir, key.replace("/", "_"))
        try:
            bts = minio_service.download_bytes(settings.BUCKET_JOB_OUTPUTS, key)
            with open(local_path, "wb") as f:
                f.write(bts)
            local_files.append(local_path)
        except Exception as e:
            logger.error(f"Failed to fetch {key}: {e}")

    # Merge strategy depends on framework
    presigned = None
    final_key = None

    if framework == "openfoam":
        final_key, presigned = await _assemble_openfoam(job_id, temp_dir, local_files)
    elif framework == "lammps":
        final_key, presigned = await _assemble_lammps(job_id, temp_dir, local_files)
    elif framework == "gromacs":
        final_key, presigned = await _assemble_gromacs(job_id, temp_dir, local_files)
    else:
        # Generic: just package all outputs into a tar
        final_key, presigned = await _assemble_generic(job_id, temp_dir, local_files)

    # Update job
    async with make_celery_session() as session:
        job_result = await session.execute(select(Job).where(Job.id == job_id))
        job = job_result.scalar_one()
        job.status = JobStatus.COMPLETED
        job.output_path = final_key
        job.presigned_url = presigned
        await session.commit()

    # Teardown network
    await network_manager.teardown(job_id)

    await r.publish("job_updates", json.dumps({
        "type": "job_complete",
        "job_id": str(job_id),
        "status": "completed",
        "download_url": presigned,
        "message": f"Simulation complete ({framework}). Results available for download.",
    }))
    await r.aclose()

    logger.info(f"Simulation assembly complete for job {job_id}")

    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)


async def _assemble_openfoam(
    job_id: str, temp_dir: str, local_files: list[str]
) -> tuple[str | None, str | None]:
    """Reconstruct OpenFOAM case from decomposed subdomains.

    In production: run `reconstructPar` on the server (lightweight post-processing).
    MVP: Package all processor directories into a tar archive.
    """
    import tarfile

    tar_path = os.path.join(temp_dir, "openfoam_results.tar.gz")
    with tarfile.open(tar_path, "w:gz") as tar:
        for f in local_files:
            tar.add(f, arcname=os.path.basename(f))

    final_key = f"{job_id}/openfoam_results.tar.gz"
    with open(tar_path, "rb") as f:
        minio_service.upload_bytes(
            settings.BUCKET_JOB_OUTPUTS, final_key, f.read(),
            content_type="application/gzip",
        )

    presigned = minio_service.get_presigned_url(settings.BUCKET_JOB_OUTPUTS, final_key, expiry_hours=168)
    return final_key, presigned


async def _assemble_lammps(
    job_id: str, temp_dir: str, local_files: list[str]
) -> tuple[str | None, str | None]:
    """Merge LAMMPS trajectory files.

    LAMMPS dump files can be concatenated in timestep order.
    MVP: Package all outputs.
    """
    # Filter trajectory files (.lammpstrj, .dump)
    traj_files = [f for f in local_files if any(
        f.endswith(ext) for ext in [".lammpstrj", ".dump", ".data"]
    )]

    if traj_files:
        merged_path = os.path.join(temp_dir, "merged_trajectory.lammpstrj")
        with open(merged_path, "w") as out:
            for tf in sorted(traj_files):
                with open(tf) as inp:
                    out.write(inp.read())

        final_key = f"{job_id}/merged_trajectory.lammpstrj"
        with open(merged_path, "rb") as f:
            minio_service.upload_bytes(
                settings.BUCKET_JOB_OUTPUTS, final_key, f.read(),
                content_type="application/octet-stream",
            )
        presigned = minio_service.get_presigned_url(settings.BUCKET_JOB_OUTPUTS, final_key, expiry_hours=168)
        return final_key, presigned

    return await _assemble_generic(job_id, temp_dir, local_files)


async def _assemble_gromacs(
    job_id: str, temp_dir: str, local_files: list[str]
) -> tuple[str | None, str | None]:
    """Merge GROMACS trajectory files (.trr, .xtc).

    In production: use `gmx trjcat` for proper concatenation.
    MVP: Package all outputs.
    """
    return await _assemble_generic(job_id, temp_dir, local_files)


async def _assemble_generic(
    job_id: str, temp_dir: str, local_files: list[str]
) -> tuple[str | None, str | None]:
    """Generic fallback: tar.gz all output files."""
    import tarfile

    tar_path = os.path.join(temp_dir, "simulation_results.tar.gz")
    with tarfile.open(tar_path, "w:gz") as tar:
        for f in local_files:
            tar.add(f, arcname=os.path.basename(f))

    final_key = f"{job_id}/simulation_results.tar.gz"
    with open(tar_path, "rb") as f:
        minio_service.upload_bytes(
            settings.BUCKET_JOB_OUTPUTS, final_key, f.read(),
            content_type="application/gzip",
        )

    presigned = minio_service.get_presigned_url(settings.BUCKET_JOB_OUTPUTS, final_key, expiry_hours=168)
    return final_key, presigned


@celery.task(name="assembler.assemble_simulation")
def assemble_simulation(job_id: str):
    """Celery entry point for simulation assembly."""
    async_to_sync(process_sim_assembly_async)(job_id)
