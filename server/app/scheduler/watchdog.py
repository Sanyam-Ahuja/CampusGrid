"""Celery Beat Watchdog for Fault Tolerance."""

import logging

import redis.asyncio as aioredis
from asgiref.sync import async_to_sync
from sqlalchemy import select

from app.celery_worker import celery_app as celery
from app.core.config import get_settings
from app.core.database import make_celery_session
from app.core.redis import RedisService
from app.models.chunk import Chunk, ChunkStatus
from app.models.job import Job, JobStatus
from app.models.node import Node

logger = logging.getLogger(__name__)
settings = get_settings()


class JobWatchdog:
    HEARTBEAT_TIMEOUT = 90  # seconds (3 missed 30s heartbeats)
    MAX_RETRIES = 3


async def check_and_rescue_async():
    """
    Finds two categories of stuck chunks and rescues them:
    1. ASSIGNED/RUNNING chunks whose node went offline (original watchdog logic).
    2. PENDING chunks that are NOT in the Redis queue (orphaned by Celery crash).
    """
    r = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    redis_svc = RedisService(r)

    async with make_celery_session() as session:
        # Jobs that are cancelled/failed — never rescue their chunks.
        dead_jobs_res = await session.execute(
            select(Job.id).where(Job.status.in_([JobStatus.CANCELLED, JobStatus.FAILED]))
        )
        dead_job_ids = {str(jid) for (jid,) in dead_jobs_res.all()}

        # ── Case 1: chunks assigned to offline nodes ──────────────────────
        active_nodes = await redis_svc.get_active_nodes(JobWatchdog.HEARTBEAT_TIMEOUT)
        active_node_ids = set(n["node_id"] for n in active_nodes)

        result = await session.execute(
            select(Chunk, Node).join(Node, Chunk.node_id == Node.id).where(
                Chunk.status.in_([ChunkStatus.ASSIGNED, ChunkStatus.RUNNING])
            )
        )
        for chunk, node in result.all():
            if str(chunk.job_id) in dead_job_ids:
                continue
            if str(node.id) not in active_node_ids:
                logger.warning(
                    f"Watchdog: Node {node.id} went offline during Chunk {chunk.id}. Rescuing..."
                )
                await _rescue_chunk(session, redis_svc, chunk, node)
                node.reliability_score = max(0.0, node.reliability_score - 0.1)

        # ── Case 2: PENDING chunks NOT in any Redis queue (orphaned) ──────
        # This happens when a Celery worker crashes after popping a chunk from
        # the queue but before committing the ASSIGNED status to the DB.
        # The chunk stays PENDING in DB forever since no worker will re-queue it.
        pending_result = await session.execute(
            select(Chunk).where(Chunk.status == ChunkStatus.PENDING)
        )
        pending_chunks = pending_result.scalars().all()

        if pending_chunks:
            # Snapshot current queue contents (both queues)
            queue_priority = await r.lrange("queue:chunks:priority", 0, -1)
            queue_normal   = await r.lrange("queue:chunks:normal",   0, -1)
            queued_ids = set(queue_priority) | set(queue_normal)

            for chunk in pending_chunks:
                if str(chunk.job_id) in dead_job_ids:
                    continue
                chunk_id_str = str(chunk.id)
                if chunk_id_str not in queued_ids:
                    logger.warning(
                        f"Watchdog: Orphaned PENDING chunk {chunk.id} — not in queue. Re-queuing."
                    )
                    await redis_svc.push_chunk(chunk_id_str, priority="high")

        # ── Case 3: FAILED chunks that still have retries left ────────────
        failed_result = await session.execute(
            select(Chunk).where(
                Chunk.status == ChunkStatus.FAILED,
                Chunk.retry_count < JobWatchdog.MAX_RETRIES
            )
        )
        for chunk in failed_result.scalars().all():
            if str(chunk.job_id) in dead_job_ids:
                continue
            logger.warning(f"Watchdog: Rescuing FAILED chunk {chunk.id} (retry {chunk.retry_count + 1})")
            chunk.status = ChunkStatus.PENDING
            chunk.node_id = None
            chunk.retry_count += 1
            await redis_svc.push_chunk(str(chunk.id), priority="high")

        await session.commit()

    # Trigger dispatcher so rescued chunks get picked up immediately
    from app.scheduler.matcher import dispatch_next_chunk
    dispatch_next_chunk.delay()

    await r.aclose()


async def _rescue_chunk(session, redis_svc: RedisService, chunk: Chunk, node: Node):
    if chunk.retry_count >= JobWatchdog.MAX_RETRIES:
        chunk.status = ChunkStatus.FAILED
        logger.error(f"Chunk {chunk.id} exhausted retries. Failing job.")
        job_result = await session.execute(select(Job).where(Job.id == chunk.job_id))
        job = job_result.scalar_one()
        job.status = JobStatus.FAILED
        return

    chunk.status = ChunkStatus.PENDING
    chunk.node_id = None
    chunk.retry_count += 1
    logger.info(f"Chunk {chunk.id} rescued and queued. (Retry {chunk.retry_count})")

    await redis_svc.push_chunk(str(chunk.id), priority="high")  # FIX: was priority=100 (int)


@celery.task(name="scheduler.watchdog")
def run_watchdog():
    """Celery Beat task entry — also callable manually."""
    async_to_sync(check_and_rescue_async)()
