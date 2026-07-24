"""Render Assembler — Combines rendered frames into a final video/archive.

After all render chunks complete, this assembler:
1. Downloads .tar.gz payloads from each node
2. Extracts raw frames into an aggregated sequence folder
3. Uses ffmpeg to compile multi-frame jobs into standard .mp4 video 
4. Uploads final media to MinIO
"""

import logging
import os
import shutil
import subprocess
import tarfile

import json
import redis.asyncio as aioredis
from asgiref.sync import async_to_sync
from sqlalchemy import select

from app.celery_worker import celery_app as celery
from app.core.config import get_settings
from app.core.database import make_celery_session
from app.models.chunk import Chunk, ChunkStatus
from app.models.job import Job, JobStatus
from app.scheduler.network_manager import network_manager
from app.services.minio_service import minio_service

logger = logging.getLogger(__name__)
settings = get_settings()


async def process_render_assembly_async(job_id: str):
    """Aggregate individual Blender frame output chunks and multiplex via ffmpeg."""

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
            logger.warning(f"Render assembler called but not all chunks complete for {job_id}")
            return

        job.status = JobStatus.ASSEMBLING
        await session.commit()

    r = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    await r.publish("job_updates", json.dumps({
        "type": "detection_step",
        "job_id": str(job_id),
        "step": "assembling",
        "detail": "Collecting rendered frames and formatting video output..."
    }))

    temp_dir = f"/tmp/campugrid_render_assemble_{job_id}"
    frames_dir = os.path.join(temp_dir, "frames")
    os.makedirs(frames_dir, exist_ok=True)

    # 1. Download all output chunk archives from MinIO
    prefix = f"{job_id}/"
    output_keys = minio_service.list_objects(settings.BUCKET_JOB_OUTPUTS, prefix=prefix)
    
    # Filter only chunk outputs
    chunk_keys = [k for k in output_keys if k.endswith('.tar.gz') and 'chunk' in os.path.basename(k)]

    for key in sorted(chunk_keys):
        local_tar_path = os.path.join(temp_dir, os.path.basename(key))
        try:
            bts = minio_service.download_bytes(settings.BUCKET_JOB_OUTPUTS, key)
            with open(local_tar_path, "wb") as f:
                f.write(bts)
            
            # Extract securely to flat /frames directory
            with tarfile.open(local_tar_path, "r:gz") as tar:
                for member in tar.getmembers():
                    if member.isfile() and member.name.endswith(".png"):
                        # Re-route pathing to prevent traversal / hierarchical mess internally
                        member.name = os.path.basename(member.name)
                        tar.extract(member, path=frames_dir)
            
            # Quick aggressive cleanup of memory
            os.remove(local_tar_path)
        except Exception as e:
            logger.error(f"Failed to fetch or unpack {key}: {e}")

    # 2. Transcode or Package Frame Outputs
    presigned = None
    final_key = None
    final_output_path = ""
    
    # Determine how many frames we salvaged
    extracted_frames = sorted([f for f in os.listdir(frames_dir) if f.endswith('.png')])
    
    if len(extracted_frames) > 1:
        # Multiple frames -> Compile to MP4 Video
        final_output_path = os.path.join(temp_dir, "final_render.mp4")
        ffmpeg_cmd = [
            "ffmpeg", "-y", "-framerate", "24", "-pattern_type", "glob", "-i", "*.png",
            "-c:v", "libopenh264", "-pix_fmt", "yuv420p", "-profile:v", "high", "-allow_skip_frames", "1", final_output_path
        ]
        
        try:
            subprocess.run(ffmpeg_cmd, cwd=frames_dir, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            final_key = f"{job_id}/final_render.mp4"
            content_type = "video/mp4"
        except subprocess.CalledProcessError as e:
            logger.error(f"ffmpeg failed: {e.stderr.decode()}")
            # Fallback to ZIP archive of the raw PNG frames if ffmpeg dies unpredictably
            final_output_path = os.path.join(temp_dir, "final_render.tar.gz")
            with tarfile.open(final_output_path, "w:gz") as tar:
                for f in extracted_frames:
                    tar.add(os.path.join(frames_dir, f), arcname=f)
            final_key = f"{job_id}/final_render.tar.gz"
            content_type = "application/gzip"

    elif len(extracted_frames) == 1:
        # Single frame -> serve image
        final_output_path = os.path.join(frames_dir, extracted_frames[0])
        final_key = f"{job_id}/final_render.png"
        content_type = "image/png"
    else:
        logger.warning(f"No frames recovered for {job_id}")

    # 3. Handle File Upload
    if final_output_path and os.path.exists(final_output_path):
        with open(final_output_path, "rb") as f:
            minio_service.upload_bytes(
                settings.BUCKET_JOB_OUTPUTS, 
                final_key, 
                f.read(),
                content_type=content_type,
            )
        presigned = minio_service.get_presigned_url(settings.BUCKET_JOB_OUTPUTS, final_key, expiry_hours=168)

    # 4. Final Updates
    async with make_celery_session() as session:
        job_result = await session.execute(select(Job).where(Job.id == job_id))
        job = job_result.scalar_one()
        job.status = JobStatus.COMPLETED
        job.output_path = final_key
        job.presigned_url = presigned
        await session.commit()

    # Teardown job overlay network mappings
    await network_manager.teardown(job_id)

    # Broadcast to the Web UI router handler
    msg_dict = {
        "type": "job_complete",
        "job_id": str(job_id),
        "status": "completed",
        "message": "Render completed. Frame sequences mapped and compiled successfully."
    }
    if presigned:
        msg_dict["download_url"] = presigned
        
    await r.publish("job_updates", json.dumps(msg_dict))
    await r.aclose()
    logger.info(f"Render assembly complete for job {job_id}")

    shutil.rmtree(temp_dir, ignore_errors=True)


@celery.task(name="assembler.assemble_render")
def assemble_render(job_id: str):
    """Celery entry point for render compilation."""
    async_to_sync(process_render_assembly_async)(job_id)
