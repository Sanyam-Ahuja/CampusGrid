"""Event-Driven Node Matching engine with LAN detection."""

import json
import logging
from collections import defaultdict
from datetime import UTC, datetime

import redis.asyncio as aioredis
from asgiref.sync import async_to_sync
from sqlalchemy import func, select

from app.celery_worker import celery_app as celery
from app.core.config import get_settings
from app.core.database import make_celery_session
from app.core.redis import RedisService
from app.models.billing import BillingRecord
from app.models.chunk import Chunk, ChunkStatus
from app.models.job import Job, JobStatus
from app.models.node import Node

logger = logging.getLogger(__name__)
settings = get_settings()

# Max times a chunk is retried on a different node before the job is failed.
# Kept in sync with JobWatchdog.MAX_RETRIES.
MAX_CHUNK_RETRIES = 3


def score_node(resources: dict, chunk_spec: dict) -> float:
    """Higher score = better match."""
    # VRAM match ensures they don't crash
    if resources.get("gpu_vram_gb", 0) < chunk_spec.get("vram_gb", 0):
        return -1.0

    if resources.get("ram_gb", 0) < chunk_spec.get("ram_gb", 0):
        return -1.0

    score = 100.0

    # Priority given to massive bandwidth
    score += min(resources.get("bandwidth_mbps", 10) / 100, 50.0)

    # Priority for highly reliable nodes
    score += (resources.get("reliability_score", 0.8) * 40.0)

    return score


def is_lan_peer(ip_a: str, ip_b: str) -> bool:
    """Check if two nodes are on the same LAN (same /24 subnet)."""
    if not ip_a or not ip_b:
        return False
    subnet_a = ".".join(ip_a.split(".")[:3])
    subnet_b = ".".join(ip_b.split(".")[:3])
    return subnet_a == subnet_b


def find_lan_cluster(
    nodes: list[tuple[str, str, str]],  # [(node_id, ip_address, resources_json), ...]
    min_size: int,
) -> list[str] | None:
    """Find a group of min_size nodes all on the same LAN.
    Returns list of node_ids or None if no cluster found.
    """
    subnets: dict[str, list[str]] = defaultdict(list)
    for node_id, ip, _ in nodes:
        if ip:
            subnet = ".".join(ip.split(".")[:3])
            subnets[subnet].append(node_id)

    for subnet in sorted(subnets, key=lambda s: len(subnets[s]), reverse=True):
        if len(subnets[subnet]) >= min_size:
            return subnets[subnet][:min_size]
    return None


async def find_best_matches(chunk: Chunk, available_nodes: list[tuple[str, str]]) -> list[str]:
    """Returns a list of node_ids sorted by suitability score (descending)."""
    scored_nodes = []

    for node_id, resources_json in available_nodes:
        if not resources_json:
            continue
        try:
            res = json.loads(resources_json)
        except json.JSONDecodeError:
            continue

        score = score_node(res, chunk.spec)
        if score > 0:  # Only nodes that meet minimum requirements
            scored_nodes.append((score, node_id))

    # Sort by score descending
    scored_nodes.sort(key=lambda x: x[0], reverse=True)
    return [node_id for _, node_id in scored_nodes]


async def process_chunk_success_async(chunk_id: str, node_id: str):
    r = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    redis_svc = RedisService(r)

    # Free the node so the dispatcher can hand it the next chunk.
    await redis_svc.mark_node_available(node_id)

    async with make_celery_session() as session:
        # Update node reliability
        node_result = await session.execute(select(Node).where(Node.id == node_id))
        node = node_result.scalar_one_or_none()
        if node:
            # Slow recovery
            node.reliability_score = min(1.0, node.reliability_score + 0.02)

        # Update chunk status
        chunk_result = await session.execute(select(Chunk).where(Chunk.id == chunk_id))
        chunk = chunk_result.scalar_one_or_none()
        if not chunk:
            logger.warning(f"chunk_success for unknown chunk {chunk_id}; ignoring")
            await session.commit()
            await r.aclose()
            return

        # Only the assigned node may report a chunk's result.
        if chunk.node_id and str(chunk.node_id) != str(node_id):
            logger.warning(f"Ignoring chunk_success for {chunk_id} from non-assignee node {node_id}")
            await session.commit()
            await r.aclose()
            return

        if chunk.status != ChunkStatus.COMPLETED:
            chunk.status = ChunkStatus.COMPLETED
            chunk.completed_at = datetime.now(UTC)
            if chunk.started_at:
                elapsed_h = (chunk.completed_at - chunk.started_at).total_seconds() / 3600.0
                chunk.gpu_hours = round(max(elapsed_h, 0.0), 4)

        # Check if job is complete
        job_id = chunk.job_id

        # Bump the job's completed_chunks counter from ground truth.
        completed_count = await session.execute(
            select(func.count(Chunk.id)).where(
                Chunk.job_id == job_id, Chunk.status == ChunkStatus.COMPLETED
            )
        )
        job_for_count = (await session.execute(select(Job).where(Job.id == job_id))).scalar_one_or_none()
        if job_for_count:
            job_for_count.completed_chunks = completed_count.scalar() or 0

        # If the job was cancelled/failed, record the chunk as done but do not
        # bill, assemble, or flip the job back to a live state.
        if job_for_count and job_for_count.status in (JobStatus.CANCELLED, JobStatus.FAILED):
            await session.commit()
            await r.aclose()
            return

        # ── Billing + gamification (idempotent: once per chunk) ──────────────
        if node and job_for_count and chunk.status == ChunkStatus.COMPLETED:
            already_billed = await session.execute(
                select(func.count(BillingRecord.id)).where(BillingRecord.chunk_id == chunk.id)
            )
            if (already_billed.scalar() or 0) == 0:
                from app.services.billing_service import BillingService
                from app.utils.gamification import update_streak

                sync_mode_raw = job_for_count.ml_sync_mode
                sync_mode = (sync_mode_raw if isinstance(sync_mode_raw, str)
                             else sync_mode_raw.value) if sync_mode_raw else "local_sgd"

                active_nodes = await redis_svc.get_active_nodes(30)
                queue_depth = await redis_svc.get_queue_depth()

                billing_svc = BillingService(session)
                breakdown = billing_svc.calculate_chunk_cost(
                    gpu_model=node.gpu_model,
                    gpu_hours=chunk.gpu_hours or 0.0,
                    sync_mode=sync_mode,
                    available_count=len(active_nodes),
                    total_count=max(len(active_nodes), 1),
                    queue_depth=queue_depth,
                )
                await billing_svc.record_chunk_billing(chunk, node, job_for_count, breakdown)

                # Contribution streak
                ns, nl, _today = update_streak(
                    node.current_streak, node.longest_streak, node.last_contribution_date
                )
                node.current_streak = ns
                node.longest_streak = nl
                node.last_contribution_date = datetime.now(UTC)

        pending_chunks = await session.execute(
            select(func.count(Chunk.id)).where(Chunk.job_id == job_id, Chunk.status != ChunkStatus.COMPLETED)
        )
        remaining = pending_chunks.scalar() or 0

        if remaining == 0:
            job_result = await session.execute(select(Job).where(Job.id == job_id))
            job = job_result.scalar_one_or_none()
            if job:
                job_type = job.type if isinstance(job.type, str) else job.type.value
                if job_type == "data":
                    from app.assembler.data_assembler import assemble_data
                    assemble_data.delay(str(job_id))
                elif job_type == "ml_training":
                    from app.assembler.ml_assembler import assemble_ml
                    assemble_ml.delay(str(job_id))
                elif job_type == "simulation":
                    from app.assembler.sim_assembler import assemble_simulation
                    assemble_simulation.delay(str(job_id))
                elif job_type == "render":
                    from app.assembler.render_assembler import assemble_render
                    assemble_render.delay(str(job_id))
                else:
                    from app.services.minio_service import minio_service
                    # We can directly use the global `settings` defined at the top of the file!
                    # For demo purposes, we will return the URL of the first output payload!
                    first_chunk_key = f"{job_id}/chunk_0.tar.gz"
                    job.presigned_url = minio_service.get_presigned_url(
                        settings.BUCKET_JOB_OUTPUTS,
                        first_chunk_key,
                        expiry_hours=168
                    )
                    job.status = JobStatus.COMPLETED

        await session.commit()

    # A node just freed up — pull the next queued chunk onto it.
    dispatch_next_chunk.delay()
    await r.aclose()


@celery.task(name="scheduler.chunk_success")
def chunk_success(chunk_id: str, node_id: str):
    async_to_sync(process_chunk_success_async)(chunk_id, node_id)


async def process_chunk_failed_async(chunk_id: str, node_id: str):
    r = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    redis_svc = RedisService(r)
    requeued = False
    async with make_celery_session() as session:
        # Penalize node
        node_result = await session.execute(select(Node).where(Node.id == node_id))
        node = node_result.scalar_one_or_none()
        if node:
            node.reliability_score = max(0.0, node.reliability_score - 0.1)

        # Free the node so it can pick up other work.
        await redis_svc.mark_node_available(node_id)

        chunk_result = await session.execute(select(Chunk).where(Chunk.id == chunk_id))
        chunk = chunk_result.scalar_one_or_none()
        # Only the assigned node may report a chunk's failure.
        if chunk and chunk.node_id and str(chunk.node_id) != str(node_id):
            logger.warning(f"Ignoring chunk_failed for {chunk_id} from non-assignee node {node_id}")
            chunk = None
        if chunk and chunk.status not in (ChunkStatus.COMPLETED,):
            if chunk.retry_count >= MAX_CHUNK_RETRIES:
                # Exhausted retries — now (and only now) the job is a failure.
                chunk.status = ChunkStatus.FAILED
                job_result = await session.execute(select(Job).where(Job.id == chunk.job_id))
                job = job_result.scalar_one_or_none()
                if job and job.status not in (JobStatus.FAILED, JobStatus.CANCELLED):
                    job.status = JobStatus.FAILED
                logger.error(f"Chunk {chunk_id} exhausted {MAX_CHUNK_RETRIES} retries; failing job {chunk.job_id}")
            else:
                # Re-queue on another node instead of killing the whole job.
                chunk.retry_count += 1
                chunk.status = ChunkStatus.PENDING
                chunk.node_id = None
                await redis_svc.push_chunk(str(chunk.id), priority="high")
                requeued = True
                logger.warning(f"Chunk {chunk_id} failed on node {node_id}; requeued (retry {chunk.retry_count}/{MAX_CHUNK_RETRIES})")

        await session.commit()

    # Trigger an immediate re-dispatch of the requeued chunk.
    if requeued:
        dispatch_chunk.delay(chunk_id)
    await r.aclose()


@celery.task(name="scheduler.chunk_failed")
def chunk_failed(chunk_id: str, node_id: str):
    async_to_sync(process_chunk_failed_async)(chunk_id, node_id)


async def process_requeue_chunk_async(chunk_id: str):
    """Reset an ASSIGNED/RUNNING chunk back to PENDING and re-dispatch it.

    Used when a dispatch couldn't be delivered (target node not connected) — the
    chunk would otherwise stay ASSIGNED forever and be ignored by dispatch_chunk.
    """
    r = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    redis_svc = RedisService(r)
    did_reset = False
    async with make_celery_session() as session:
        chunk = await session.get(Chunk, chunk_id)
        if chunk and chunk.status in (ChunkStatus.ASSIGNED, ChunkStatus.RUNNING):
            # Free the previously-claimed node so it isn't stuck busy.
            if chunk.node_id:
                await redis_svc.mark_node_available(str(chunk.node_id))
            chunk.status = ChunkStatus.PENDING
            chunk.node_id = None
            await session.commit()
            await redis_svc.push_chunk(chunk_id, priority="high")
            did_reset = True
    await r.aclose()
    if did_reset:
        await process_dispatch_chunk_async(chunk_id)


@celery.task(name="scheduler.requeue_chunk")
def requeue_chunk(chunk_id: str):
    async_to_sync(process_requeue_chunk_async)(chunk_id)


async def process_dispatch_chunk_async(chunk_id: str):
    r = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    redis_svc = RedisService(r)

    async with make_celery_session() as session:
        # Load Chunk
        chunk_info = await session.get(Chunk, chunk_id)
        if not chunk_info or chunk_info.status != ChunkStatus.PENDING:
            await r.aclose()
            return

        # Don't dispatch chunks belonging to a cancelled/failed job.
        parent_job = await session.get(Job, chunk_info.job_id)
        if parent_job and parent_job.status in (JobStatus.CANCELLED, JobStatus.FAILED):
            logger.info(f"Skipping dispatch of chunk {chunk_id}: job {chunk_info.job_id} is {parent_job.status.value}")
            await r.aclose()
            return

        # get_active_nodes() returns list[dict] with a "node_id" key injected.
        # Build the (node_id, resources_json_str) tuples that find_best_match needs.
        active_node_dicts = await redis_svc.get_active_nodes(30)

        nodes_with_res: list[tuple[str, str]] = []
        for node_dict in active_node_dicts:
            nid = node_dict.get("node_id")
            if not nid:
                continue
            # Check the node isn't currently busy
            status = await r.get(f"node:status:{nid}")
            if status == "busy":
                continue
            nodes_with_res.append((nid, json.dumps(node_dict)))

        logger.info(f"Dispatching chunk {chunk_id}: {len(nodes_with_res)} available nodes")

        # Find best node candidates
        candidates = await find_best_matches(chunk_info, nodes_with_res)

        best_match_id = None
        for candidate_id in candidates:
            # ATOMIC CLAIM: Ensure no other worker has taken this node in the last millisecond
            if await redis_svc.claim_node(candidate_id):
                best_match_id = candidate_id
                break

        if not best_match_id:
            # No node available right now (or all candidates were just claimed by others)
            # Requeue with a small delay or lower priority to prevent tight-looping
            await redis_svc.push_chunk(chunk_id, priority="normal")
            logger.info(f"No node (or claim failed) for chunk {chunk_id}, requeued")
            await r.aclose()
            return

        # Update Postgres
        now = datetime.now(UTC)
        chunk_info.node_id = best_match_id
        chunk_info.status = ChunkStatus.ASSIGNED
        chunk_info.assigned_at = now
        # We don't get a separate "running" signal from the node yet, so treat
        # dispatch time as the start for duration/gpu-hour accounting.
        if not chunk_info.started_at:
            chunk_info.started_at = now
        await session.commit()

        # CRITICAL FIX: dispatch_to_node() calls ws_manager.send_to_node() which
        # lives in the FastAPI (uvicorn) process.  Celery workers run in a DIFFERENT
        # process and have an empty ws_manager with no connections.
        # Fix: publish a Redis message so the FastAPI lifespan listener can forward it.
        import json as _json
        dispatch_payload = _json.dumps({
            "type": "chunk_dispatch",
            "target_node_id": best_match_id,
            "job_id": str(chunk_info.job_id),
            "chunk_id": str(chunk_info.id),
            "spec": chunk_info.spec,
        })
        await r.publish("node_dispatches", dispatch_payload)
        logger.info(f"Published chunk {chunk_id} dispatch to node {best_match_id} via Redis")

    await r.aclose()


@celery.task(name="scheduler.dispatch_chunk")
def dispatch_chunk(chunk_id: str):
    """Triggered by Queue arrival or when node becomes free."""
    async_to_sync(process_dispatch_chunk_async)(chunk_id)


async def process_dispatch_next_chunk_async():
    """Pops the next chunk from the queue and tries to match it."""
    r = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    redis_svc = RedisService(r)
    
    chunk_id = await redis_svc.pop_chunk()
    if chunk_id:
        logger.info(f"Dispatching next chunk from queue: {chunk_id}")
        await process_dispatch_chunk_async(chunk_id)
    
    await r.aclose()


@celery.task(name="scheduler.dispatch_next_chunk")
def dispatch_next_chunk():
    """Triggered when a node becomes available to handle the next queued item."""
    async_to_sync(process_dispatch_next_chunk_async)()
