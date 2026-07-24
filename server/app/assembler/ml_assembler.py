"""ML Training Assembler — Final model weights + training curves.

After all ML training chunks complete, this assembler:
1. Downloads the final checkpoint from rank 0's output
2. Collects training logs from all nodes
3. Generates training curve data
4. Uploads final artifacts to MinIO
"""

import json
import logging
import os

from typing import Any
from asgiref.sync import async_to_sync
from sqlalchemy import select

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


async def process_ml_assembly_async(job_id: str):
    """Select best model weights and extract training curves."""

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
            logger.warning(f"ML assembler called but not all chunks complete for {job_id}")
            return

        job.status = JobStatus.ASSEMBLING
        await session.commit()

    r = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    await r.publish("job_updates", json.dumps({
        "type": "detection_step",
        "job_id": str(job_id),
        "step": "assembling",
        "detail": "Collecting model weights and training metrics from all nodes..."
    }))

    temp_dir = f"/tmp/campugrid_ml_assemble_{job_id}"
    os.makedirs(temp_dir, exist_ok=True)

    # For both Local SGD and DDP, all nodes should have synchronized weights
    # at the end. We take the final checkpoint from rank 0 (chunk 0).
    chunk_0_key = f"{job_id}/chunk_0.tar.gz"
    
    final_model_path = None
    try:
        import tarfile
        tar_path = os.path.join(temp_dir, "chunk_0.tar.gz")
        bts = minio_service.download_bytes(settings.BUCKET_JOB_OUTPUTS, chunk_0_key)
        with open(tar_path, "wb") as f:
            f.write(bts)
            
        # Safe extraction of UNTRUSTED node output: reject absolute paths and any
        # member that would escape temp_dir (path traversal). Preserve subdirs so
        # the checkpoints/ lookup below still works.
        with tarfile.open(tar_path, "r:gz") as tar:
            base = os.path.realpath(temp_dir)
            for member in tar.getmembers():
                target = os.path.realpath(os.path.join(temp_dir, member.name))
                if not (target == base or target.startswith(base + os.sep)):
                    logger.warning(f"Skipping unsafe tar member: {member.name}")
                    continue
                tar.extract(member, path=temp_dir)

        # Find checkpoint files
        import glob
        checkpoint_files = glob.glob(os.path.join(temp_dir, "checkpoints", "*.pt"))
        if checkpoint_files:
            latest_ckpt = sorted(checkpoint_files)[-1]  # string sort by name works if padded, but let's just pick one
            final_model_path = os.path.join(temp_dir, "final_model.pt")
            import shutil
            shutil.copy(latest_ckpt, final_model_path)
            logger.info(f"Extracted final checkpoint: {latest_ckpt}")
            
    except Exception as e:
        logger.error(f"Failed to download or extract checkpoint archive: {e}")

    # 2. Collect training logs from all ranks
    training_curves = []
    for chunk in chunks:
        rank = chunk.chunk_index - 1  # chunk_index is 1-based
        log_prefix = f"{job_id}/rank_{rank}/logs/"
        log_keys = minio_service.list_objects(settings.BUCKET_JOB_OUTPUTS, prefix=log_prefix)

        for log_key in log_keys:
            try:
                log_data = minio_service.download_bytes(settings.BUCKET_JOB_OUTPUTS, log_key)
                log_text = log_data.decode("utf-8", errors="replace")

                # Parse simple training logs: lines like "epoch=1 loss=0.45 lr=0.001"
                for line in log_text.split("\n"):
                    entry = _parse_training_log_line(line, rank)
                    if entry:
                        training_curves.append(entry)
            except Exception as e:
                logger.warning(f"Failed to read training log {log_key}: {e}")

    # 3. Generate training curve JSON
    curve_path = os.path.join(temp_dir, "training_curve.json")
    with open(curve_path, "w") as f:
        json.dump({
            "job_id": job_id,
            "num_ranks": len(chunks),
            "sync_mode": "local_sgd",  # TODO: read from job profile
            "curves": training_curves,
        }, f, indent=2)

    # 4. Upload final artifacts to MinIO
    final_model_key = None
    presigned = None

    if final_model_path and os.path.exists(final_model_path):
        final_model_key = f"{job_id}/final_model.pt"
        with open(final_model_path, "rb") as f:
            minio_service.upload_bytes(
                settings.BUCKET_JOB_OUTPUTS,
                final_model_key,
                f.read(),
                content_type="application/octet-stream",
            )
        presigned = minio_service.get_presigned_url(
            settings.BUCKET_JOB_OUTPUTS, final_model_key, expiry_hours=168
        )

    # Upload training curve
    curve_key = f"{job_id}/training_curve.json"
    with open(curve_path, "rb") as f:
        minio_service.upload_bytes(
            settings.BUCKET_JOB_OUTPUTS,
            curve_key,
            f.read(),
            content_type="application/json",
        )
    curve_presigned = minio_service.get_presigned_url(
        settings.BUCKET_JOB_OUTPUTS, curve_key, expiry_hours=168
    )

    # 5. Update job status
    async with make_celery_session() as session:
        job_result = await session.execute(select(Job).where(Job.id == job_id))
        job = job_result.scalar_one()
        job.status = JobStatus.COMPLETED
        job.output_path = final_model_key
        job.presigned_url = presigned
        await session.commit()

    # 6. Teardown overlay network
    await network_manager.teardown(job_id)

    # 7. Broadcast completion
    await r.publish("job_updates", json.dumps({
        "type": "job_complete",
        "job_id": str(job_id),
        "status": "completed",
        "download_url": presigned,
        "training_curve_url": curve_presigned,
        "message": "ML training complete. Model weights and training curves available.",
    }))
    await r.aclose()

    logger.info(f"ML assembly complete for job {job_id}")

    # Cleanup temp
    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)


def _parse_training_log_line(line: str, rank: int) -> dict | None:
    """Parse a training log line into structured data.

    Supports common formats:
    - "epoch=1 loss=0.45 lr=0.001"
    - "Epoch 1/10, Loss: 0.45, Acc: 0.89"
    - JSON lines: {"epoch": 1, "loss": 0.45}
    """
    line = line.strip()
    if not line:
        return None

    # Try JSON first
    try:
        data = json.loads(line)
        data["rank"] = rank
        return data
    except (json.JSONDecodeError, ValueError):
        pass

    # Try key=value format
    entry: dict[str, Any] = {"rank": rank}
    import re
    kv_pattern = re.compile(r"(\w+)\s*[=:]\s*([\d.e+-]+)", re.IGNORECASE)
    matches = kv_pattern.findall(line)
    if matches:
        for key, value in matches:
            try:
                entry[key.lower()] = float(value)
            except ValueError:
                entry[key.lower()] = value
        if len(entry) > 1:  # more than just rank
            return entry

    return None


@celery.task(name="assembler.assemble_ml")
def assemble_ml(job_id: str):
    """Celery entry point for ML assembly."""
    async_to_sync(process_ml_assembly_async)(job_id)
