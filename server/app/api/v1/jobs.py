"""Job submission and management endpoints."""

from uuid import uuid4

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.database import get_db
from app.core.security import TokenPayload, get_current_user
from app.models.job import Job, JobStatus
from app.pipeline.orchestrator import analyze_and_dispatch
from app.schemas.job import (
    JobListItem,
    JobStatusResponse,
    JobSubmitResponse,
    PaginatedJobs,
    PriceEstimate,
)
from app.services.minio_service import minio_service
from app.utils.gpu_benchmarks import customer_price_per_hour, get_price_comparison

settings = get_settings()
router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("/", response_model=JobSubmitResponse, status_code=status.HTTP_202_ACCEPTED)
async def submit_job(
    files: list[UploadFile] = File(...),
    ml_sync_mode: str | None = None,
    requires_public_network: bool = False,
    current_user: TokenPayload = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Submit a job — upload files, create job record, kick AI pipeline."""
    if not files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one file is required",
        )

    job_id = uuid4()
    input_filename = files[0].filename
    for file in files:
        if file.filename.endswith(".zip"):
            input_filename = file.filename
            break
    input_path = f"{job_id}/{input_filename}"

    # Upload files to MinIO
    for file in files:
        content = await file.read()
        key = f"{job_id}/{file.filename}"
        minio_service.upload_bytes(
            bucket=settings.BUCKET_JOB_INPUTS,
            key=key,
            data=content,
            content_type=file.content_type or "application/octet-stream",
        )

    # Create job record
    job = Job(
        id=job_id,
        user_id=current_user.user_id,
        status=JobStatus.ANALYZING,
        input_path=input_path,
        ml_sync_mode=ml_sync_mode,
        requires_public_network=requires_public_network,
    )
    db.add(job)
    await db.flush()
    await db.commit()

    # Kick off AI pipeline as Celery task
    analyze_and_dispatch.delay(str(job_id), str(current_user.user_id))

    return JobSubmitResponse(
        job_id=job_id,
        status="analyzing",
        message="Job submitted. AI pipeline is analyzing your files.",
    )


@router.get("/", response_model=PaginatedJobs)
async def list_jobs(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=20, ge=1, le=100),
    current_user: TokenPayload = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List current user's jobs (paginated)."""
    # Count
    count_result = await db.execute(
        select(func.count(Job.id)).where(Job.user_id == current_user.user_id)
    )
    total = count_result.scalar() or 0

    # Fetch page
    offset = (page - 1) * limit
    result = await db.execute(
        select(Job)
        .where(Job.user_id == current_user.user_id)
        .order_by(desc(Job.created_at))
        .offset(offset)
        .limit(limit)
    )
    jobs = result.scalars().all()

    return PaginatedJobs(
        jobs=[JobListItem.model_validate(j) for j in jobs],
        total=total,
        page=page,
        limit=limit,
    )


@router.get("/{job_id}", response_model=JobStatusResponse)
async def get_job(
    job_id: str,
    current_user: TokenPayload = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get detailed job status with chunks."""
    result = await db.execute(
        select(Job).where(Job.id == job_id, Job.user_id == current_user.user_id)
    )
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    return JobStatusResponse.model_validate(job)


@router.post("/{job_id}/cancel", status_code=status.HTTP_200_OK)
async def cancel_job(
    job_id: str,
    current_user: TokenPayload = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Cancel a running or queued job."""
    result = await db.execute(
        select(Job).where(Job.id == job_id, Job.user_id == current_user.user_id)
    )
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    if job.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot cancel job with status: {job.status.value}",
        )

    job.status = JobStatus.CANCELLED

    # Tell any nodes currently running this job's chunks to stop, and remove
    # not-yet-finished chunks from the Redis queue's reach (the matcher/watchdog
    # both skip chunks of a CANCELLED job).
    from app.models.chunk import Chunk, ChunkStatus
    chunks_res = await db.execute(
        select(Chunk).where(
            Chunk.job_id == job_id,
            Chunk.status.in_([ChunkStatus.ASSIGNED, ChunkStatus.RUNNING]),
        )
    )
    running_chunks = chunks_res.scalars().all()
    if running_chunks:
        import json as _json
        import redis.asyncio as aioredis
        r = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
        try:
            for ch in running_chunks:
                if ch.node_id:
                    await r.publish("node_dispatches", _json.dumps({
                        "type": "job_cancel",
                        "target_node_id": str(ch.node_id),
                        "job_id": str(job_id),
                        "chunk_id": str(ch.id),
                    }))
        finally:
            await r.aclose()

    await db.flush()

@router.post("/{job_id}/resolve_dockerfile", status_code=status.HTTP_200_OK)
async def resolve_dockerfile(
    job_id: str,
    dockerfile: UploadFile | None = File(None),
    use_ai: bool = False,
    current_user: TokenPayload = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Resolve a paused job that needs a Dockerfile.
    
    The user can either:
    1. Upload their own Dockerfile (dockerfile param)
    2. Explicitly authorize AI generation (use_ai=True)
    """
    result = await db.execute(
        select(Job).where(Job.id == job_id, Job.user_id == current_user.user_id)
    )
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    if job.status != JobStatus.NEEDS_DOCKERFILE:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Job is not awaiting Dockerfile resolution. Current status: {job.status.value}",
        )

    if dockerfile:
        # User provided their own Dockerfile — upload it to MinIO alongside the job files
        content = await dockerfile.read()
        key = f"{job_id}/Dockerfile"
        minio_service.upload_bytes(
            bucket=settings.BUCKET_JOB_INPUTS,
            key=key,
            data=content,
            content_type="text/plain",
        )
        # Set status back to ANALYZING so the orchestrator picks up the Dockerfile
        job.status = JobStatus.ANALYZING
        await db.flush()
        await db.commit()

        # Re-kick the pipeline — it will now find the Dockerfile and use the custom path
        analyze_and_dispatch.delay(str(job_id), str(current_user.user_id))

        return {"job_id": str(job_id), "status": "analyzing", "resolution": "custom_dockerfile"}

    elif use_ai:
        # User explicitly authorized AI generation — set status to something
        # the orchestrator recognizes as "user approved AI"
        job.status = JobStatus.QUEUED  # Orchestrator checks: if status != ANALYZING, allow AI
        await db.flush()
        await db.commit()

        # Re-kick the pipeline — it will skip the pause and proceed to Gemini
        analyze_and_dispatch.delay(str(job_id), str(current_user.user_id))

        return {"job_id": str(job_id), "status": "analyzing", "resolution": "ai_generation"}

    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provide either a Dockerfile upload or set use_ai=true",
        )


@router.get("/estimate/price", response_model=PriceEstimate)
async def get_price_estimate(
    gpu_tier: str = Query(..., examples=["RTX 4060"]),
    estimated_hours: float = Query(..., ge=0.1, examples=[4.0]),
    sync_mode: str = Query(default="local_sgd", pattern="^(local_sgd|ddp)$"),
):
    """Public endpoint: price estimate with competitor comparison."""
    base_price = customer_price_per_hour(gpu_tier)

    # For now, use static multiplier (no live supply/demand data yet)
    dyn_mult = 1.0
    sync_mult = 1.5 if sync_mode == "ddp" else 1.0

    total = round(base_price * estimated_hours * dyn_mult * sync_mult, 2)
    comparison = get_price_comparison(gpu_tier, estimated_hours)

    return PriceEstimate(
        gpu_tier=gpu_tier,
        estimated_hours=estimated_hours,
        base_price_per_hour=base_price,
        dynamic_multiplier=dyn_mult * sync_mult,
        total_estimate=total,
        comparison=comparison,
    )
