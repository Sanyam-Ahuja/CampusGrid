"""Assembles data chunks into a single final output."""

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
from app.services.minio_service import minio_service

logger = logging.getLogger(__name__)
settings = get_settings()


async def process_data_assembly_async(job_id: str):
    """Downloads all output chunks, concatenates them locally, and uploads the merged result."""
    # Ensure job actually completed all chunks
    async with make_celery_session() as session:
        job_result = await session.execute(select(Job).where(Job.id == job_id))
        job = job_result.scalar_one_or_none()
        if not job:
            return

        chunks_res = await session.execute(select(Chunk).where(Chunk.job_id == job_id))
        chunks = chunks_res.scalars().all()

        if any(c.status != ChunkStatus.COMPLETED for c in chunks):
            logger.warning(f"Job {job_id} assembler called but chunks not all complete.")
            return

        job.status = JobStatus.ASSEMBLING
        await session.commit()

        r = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
        await r.publish("job_updates", json.dumps({
            "type": "detection_step",
            "job_id": str(job_id),
            "step": "assembling",
            "detail": "Merging map-reduce arrays locally..."
        }))
        await r.aclose()

    # We download all output shards locally into a temp folder
    temp_dir = f"/tmp/campugrid_assemble_{job_id}"
    os.makedirs(temp_dir, exist_ok=True)

    # Identify objects in Minio mapping to this job outputs
    prefix = f"{job_id}/"
    output_keys = minio_service.list_objects(settings.BUCKET_JOB_OUTPUTS, prefix=prefix)

    # Each chunk output is a .tar.gz of the container's /output dir. Download and
    # extract them, collecting the CSV files inside.
    import tarfile
    extract_dir = os.path.join(temp_dir, "extracted")
    os.makedirs(extract_dir, exist_ok=True)
    csv_files: list[str] = []
    raw_files: list[str] = []

    for key in sorted(output_keys):
        base = os.path.basename(key)
        # Skip any previously-assembled final artifact.
        if base.startswith("final_"):
            continue
        local_path = os.path.join(temp_dir, key.replace("/", "_"))
        try:
            bts = minio_service.download_bytes(settings.BUCKET_JOB_OUTPUTS, key)
            with open(local_path, "wb") as f:
                f.write(bts)
        except Exception as e:
            logger.error(f"Failed to fetch {key}: {e}")
            continue

        if key.endswith(".tar.gz") or key.endswith(".tgz"):
            try:
                with tarfile.open(local_path, "r:gz") as tar:
                    for member in tar.getmembers():
                        if member.isfile():
                            member.name = os.path.basename(member.name)  # flatten, prevent traversal
                            tar.extract(member, path=extract_dir)
                            out_path = os.path.join(extract_dir, member.name)
                            if member.name.lower().endswith((".csv", ".tsv")):
                                csv_files.append(out_path)
                            else:
                                raw_files.append(out_path)
            except Exception as e:
                logger.error(f"Failed to extract {key}: {e}")
        else:
            # Bare file output (no archive)
            if local_path.lower().endswith((".csv", ".tsv")):
                csv_files.append(local_path)
            else:
                raw_files.append(local_path)

    presigned = None
    final_key = None
    if csv_files:
        # CSV merge: keep the header from the first shard only.
        merged_path = os.path.join(temp_dir, "merged.csv")
        with open(merged_path, "w") as out:
            for i, shard in enumerate(sorted(csv_files)):
                with open(shard, errors="replace") as f:
                    if i > 0:
                        f.readline()  # drop repeated header
                    out.write(f.read())
        final_key = f"{job_id}/final_merged.csv"
        with open(merged_path, "rb") as final_f:
            minio_service.upload_bytes(
                settings.BUCKET_JOB_OUTPUTS, final_key, final_f.read(),
                content_type="text/csv",
            )
        presigned = minio_service.get_presigned_url(settings.BUCKET_JOB_OUTPUTS, final_key, expiry_hours=168)
    elif raw_files:
        # No CSVs — just bundle whatever the job produced into one archive.
        bundle_path = os.path.join(temp_dir, "results.tar.gz")
        with tarfile.open(bundle_path, "w:gz") as tar:
            for fpath in sorted(raw_files):
                tar.add(fpath, arcname=os.path.basename(fpath))
        final_key = f"{job_id}/final_results.tar.gz"
        with open(bundle_path, "rb") as final_f:
            minio_service.upload_bytes(
                settings.BUCKET_JOB_OUTPUTS, final_key, final_f.read(),
                content_type="application/gzip",
            )
        presigned = minio_service.get_presigned_url(settings.BUCKET_JOB_OUTPUTS, final_key, expiry_hours=168)

    local_files = csv_files or raw_files

    # Final DB Update
    async with make_celery_session() as session:
        job_result = await session.execute(select(Job).where(Job.id == job_id))
        job = job_result.scalar_one()
        job.status = JobStatus.COMPLETED
        job.output_path = final_key if local_files else None
        job.presigned_url = presigned
        await session.commit()

        r2 = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
        await r2.publish("job_updates", json.dumps({
            "type": "job_complete",
            "job_id": str(job_id),
            "status": "completed",
            "download_url": presigned
        }))
        await r2.aclose()


@celery.task(name="assembler.assemble_data")
def assemble_data(job_id: str):
    async_to_sync(process_data_assembly_async)(job_id)
