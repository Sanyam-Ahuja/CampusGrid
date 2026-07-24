"""CampuGrid Server — FastAPI entry point."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.router import api_router
from app.core.config import get_settings
from app.core.database import init_db
from app.core.redis import redis_pool
from app.services.minio_service import minio_service
from app.api.v1.websocket import ws_manager
import asyncio
import json
import redis.asyncio as aioredis

settings = get_settings()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    # ── Startup ──
    logger.info("🚀 CampuGrid server starting...")

    # Create database tables (dev only — use Alembic in prod)
    logger.info("📦 Initializing database tables...")
    await init_db()

    # Ensure MinIO buckets exist
    logger.info("🪣 Ensuring MinIO buckets...")
    try:
        minio_service.ensure_buckets()
        logger.info("✅ MinIO buckets ready")
    except Exception as e:
        logger.warning(f"⚠️ MinIO not available: {e}. Start infra with 'docker compose up'")

    # Sentry init
    # ── Redis PubSub Listener (customer job updates) ──
    async def listen_to_job_updates():
        try:
            client = aioredis.Redis(connection_pool=redis_pool)
            pubsub = client.pubsub()
            await pubsub.subscribe("job_updates")
            logger.info("📡 Listening to Redis channel: job_updates")
            async for message in pubsub.listen():
                if message["type"] == "message":
                    data = json.loads(message["data"])
                    job_id = data.get("job_id")
                    if job_id:
                        await ws_manager.broadcast_to_job(job_id, data)
        except Exception as e:
            logger.warning(f"Redis job_updates PubSub Error: {e}")

    # ── Redis PubSub Listener (node chunk dispatches) ──
    # CRITICAL: Celery workers run in a separate process and cannot call
    # ws_manager.send_to_node() — their ws_manager has zero connections.
    # This listener bridges that gap: Celery publishes to "node_dispatches"
    # and we (the FastAPI process, owning all live WS connections) forward it.
    async def listen_to_node_dispatches():
        try:
            client2 = aioredis.Redis(connection_pool=redis_pool)
            pubsub2 = client2.pubsub()
            await pubsub2.subscribe("node_dispatches")
            logger.info("📡 Listening to Redis channel: node_dispatches")
            async for message in pubsub2.listen():
                if message["type"] == "message":
                    data = json.loads(message["data"])
                    target_node_id = data.get("target_node_id")

                    # Cancellation: tell the node to stop the running container.
                    if data.get("type") == "job_cancel":
                        if target_node_id:
                            await ws_manager.send_to_node(target_node_id, {
                                "type": "job_cancel",
                                "job_id": data.get("job_id"),
                                "chunk_id": data.get("chunk_id"),
                            })
                        continue

                    if target_node_id:
                        # Build the job_dispatch message the Tauri node expects
                        dispatch_msg = {
                            "type": "job_dispatch",
                            "job_id": data.get("job_id"),
                            "chunk_id": data.get("chunk_id"),
                            "spec": data.get("spec", {}),
                        }
                        sent = await ws_manager.send_to_node(target_node_id, dispatch_msg)
                        if sent:
                            logger.info(f"✅ Dispatched chunk {data.get('chunk_id')} to node {target_node_id}")
                        else:
                            logger.warning(f"⚠️  Node {target_node_id} not connected — chunk {data.get('chunk_id')} undelivered")
                            # Reset the chunk to PENDING and re-dispatch. (dispatch_chunk
                            # alone ignores ASSIGNED chunks, so the old code never retried.)
                            from app.scheduler.matcher import requeue_chunk
                            if data.get("chunk_id"):
                                requeue_chunk.apply_async(
                                    args=[data["chunk_id"]],
                                    countdown=15  # retry in 15s
                                )
        except Exception as e:
            logger.warning(f"Redis node_dispatches PubSub Error: {e}")

    redis_task = asyncio.create_task(listen_to_job_updates())
    node_dispatch_task = asyncio.create_task(listen_to_node_dispatches())

    logger.info("✅ CampuGrid server ready!")
    yield

    # ── Shutdown ──
    redis_task.cancel()
    node_dispatch_task.cancel()
    logger.info("👋 CampuGrid server shutting down...")


# ── Create App ──────────────────────────────────────────────────

app = FastAPI(
    title="CampuGrid API",
    description="Distributed P2P GPU Compute Platform — Server API",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Prometheus metrics
try:
    from prometheus_fastapi_instrumentator import Instrumentator
    Instrumentator().instrument(app).expose(app, endpoint="/metrics")
except ImportError:
    logger.info("prometheus-fastapi-instrumentator not installed, skipping metrics")

# Include API routes
app.include_router(api_router)


# ── Health Endpoint ─────────────────────────────────────────────

@app.get("/health", tags=["health"])
async def health_check():
    """Health check — verifies DB, Redis, and MinIO connectivity."""
    health = {"status": "ok", "services": {}}

    # Database
    try:
        from app.core.database import engine
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        health["services"]["database"] = "connected"
    except Exception as e:
        health["services"]["database"] = f"error: {e}"
        health["status"] = "degraded"

    # Redis
    try:
        import redis.asyncio as aioredis
        r = aioredis.from_url(settings.REDIS_URL)
        await r.ping()
        await r.aclose()
        health["services"]["redis"] = "connected"
    except Exception as e:
        health["services"]["redis"] = f"error: {e}"
        health["status"] = "degraded"

    # MinIO
    try:
        minio_service.client.list_buckets()
        health["services"]["minio"] = "connected"
    except Exception as e:
        health["services"]["minio"] = f"error: {e}"
        health["status"] = "degraded"

    return health


# Need this import for the health check SQL
from sqlalchemy import text


# ── Admin Rescue Endpoint ───────────────────────────────────────────
# Finds orphaned PENDING chunks (not in Redis queue) and re-queues them.
# Call with: curl -X POST http://localhost:8000/admin/rescue

@app.post("/admin/rescue", tags=["admin"])
async def admin_rescue_orphaned_chunks():
    """
    Scan DB for PENDING chunks that are not in any Redis queue,
    push them back with high priority, and trigger dispatch.
    Use this to recover from Celery crash scenarios.
    """
    import redis.asyncio as aioredis
    from sqlalchemy import select
    from app.core.database import async_session
    from app.models.chunk import Chunk, ChunkStatus
    from app.core.redis import RedisService

    r = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    redis_svc = RedisService(r)

    rescued = []
    already_queued = []

    try:
        async with async_session() as session:
            result = await session.execute(
                select(Chunk).where(Chunk.status == ChunkStatus.PENDING)
            )
            pending = result.scalars().all()

            queue_priority = await r.lrange("queue:chunks:priority", 0, -1)
            queue_normal   = await r.lrange("queue:chunks:normal",   0, -1)
            queued_ids = set(queue_priority) | set(queue_normal)

            for chunk in pending:
                cid = str(chunk.id)
                if cid not in queued_ids:
                    await redis_svc.push_chunk(cid, priority="high")
                    rescued.append(cid)
                    logger.info(f"Rescue: re-queued orphaned chunk {cid}")
                else:
                    already_queued.append(cid)

        # Trigger the dispatcher immediately
        if rescued:
            from app.scheduler.matcher import dispatch_next_chunk
            dispatch_next_chunk.delay()

    finally:
        await r.aclose()

    return {
        "rescued": rescued,
        "already_in_queue": already_queued,
        "total_rescued": len(rescued),
        "message": f"Re-queued {len(rescued)} orphaned chunks. Dispatch triggered." if rescued
                   else "No orphaned chunks found."
    }
