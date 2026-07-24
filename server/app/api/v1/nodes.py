"""Node registration and management endpoints."""

from datetime import UTC, datetime

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.redis import RedisService, get_redis
from app.core.security import TokenPayload, create_access_token, get_current_user
from app.models.billing import BillingRecord
from app.models.chunk import Chunk, ChunkStatus
from app.models.job import Job, JobStatus
from app.models.node import Node, NodeStatus
from app.schemas.node import ClusterStats, NodeRegister, NodeResponse, NodeSettingsUpdate
from app.services.billing_service import BillingService
from app.utils.gamification import get_tier

router = APIRouter(prefix="/nodes", tags=["nodes"])


@router.post("/register", status_code=status.HTTP_201_CREATED)
async def register_node(
    data: NodeRegister,
    current_user: TokenPayload = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Register a contributor machine. Returns node info + a dedicated node JWT."""
    node = Node(
        user_id=current_user.user_id,
        hostname=data.hostname,
        cpu_cores=data.cpu_cores,
        ram_gb=data.ram_gb,
        gpu_model=data.gpu_model,
        gpu_vram_gb=data.gpu_vram_gb,
        cuda_version=data.cuda_version,
        os=data.os,
        bandwidth_mbps=data.bandwidth_mbps,
        ip_address=data.ip_address,
        # Starts offline; the node's WebSocket connection flips it online.
        status=NodeStatus.OFFLINE,
    )
    db.add(node)
    await db.flush()
    await db.refresh(node)

    # Generate a long-lived JWT for the Tauri daemon to use
    from datetime import timedelta

    node_token = create_access_token(
        current_user.user_id,
        current_user.role,
        expires_delta=timedelta(days=365),
    )

    return {
        "node": NodeResponse.model_validate(node),
        "node_id": str(node.id),
        "node_token": node_token,
        "message": "Node registered. Use the node_token in your Tauri daemon to authenticate.",
    }


@router.get("/me", response_model=list[NodeResponse])
async def get_my_nodes(
    current_user: TokenPayload = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get all nodes belonging to the current user."""
    result = await db.execute(
        select(Node).where(Node.user_id == current_user.user_id)
    )
    nodes = result.scalars().all()
    return [NodeResponse.model_validate(n) for n in nodes]


@router.post("/me/{node_id}/token")
async def regenerate_node_token(
    node_id: str,
    current_user: TokenPayload = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Regenerate a JWT token for a specific node."""
    result = await db.execute(
        select(Node).where(Node.id == node_id, Node.user_id == current_user.user_id)
    )
    node = result.scalar_one_or_none()
    if not node:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Node not found")

    from datetime import timedelta

    node_token = create_access_token(
        current_user.user_id,
        current_user.role,
        expires_delta=timedelta(days=365),
    )

    return {
        "node_id": str(node.id),
        "node_token": node_token,
        "message": "New token generated. Update your Tauri daemon configuration.",
    }


@router.patch("/me/{node_id}/settings", response_model=NodeResponse)
async def update_node_settings(
    node_id: str,
    data: NodeSettingsUpdate,
    current_user: TokenPayload = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Update contributor node settings (resource limits, schedule)."""
    result = await db.execute(
        select(Node).where(Node.id == node_id, Node.user_id == current_user.user_id)
    )
    node = result.scalar_one_or_none()
    if not node:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Node not found")

    if data.resource_limits is not None:
        node.resource_limits = data.resource_limits

    await db.flush()
    await db.refresh(node)
    return NodeResponse.model_validate(node)


@router.get("/me/{node_id}/history")
async def get_node_history(
    node_id: str,
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=15, ge=1, le=100),
    current_user: TokenPayload = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get job history for a specific contributor node."""
    # Verify ownership
    result = await db.execute(
        select(Node).where(Node.id == node_id, Node.user_id == current_user.user_id)
    )
    node = result.scalar_one_or_none()
    if not node:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Node not found")

    offset = (page - 1) * limit

    # Get completed chunks for this node with job info
    chunks_result = await db.execute(
        select(Chunk)
        .where(Chunk.node_id == node_id, Chunk.status == ChunkStatus.COMPLETED)
        .order_by(Chunk.completed_at.desc())
        .offset(offset)
        .limit(limit)
    )
    chunks = chunks_result.scalars().all()

    # Get job info for each chunk
    history = []
    for chunk in chunks:
        job_result = await db.execute(select(Job).where(Job.id == chunk.job_id))
        job = job_result.scalar_one_or_none()

        # Get billing record
        billing_result = await db.execute(
            select(BillingRecord).where(BillingRecord.chunk_id == chunk.id)
        )
        billing = billing_result.scalar_one_or_none()

        history.append({
            "chunk_id": str(chunk.id),
            "job_id": str(chunk.job_id),
            "job_type": job.type.value if job and job.type else "unknown",
            "duration_seconds": (
                (chunk.completed_at - chunk.started_at).total_seconds()
                if chunk.completed_at and chunk.started_at
                else 0
            ),
            "gpu_hours": chunk.gpu_hours,
            "earned": billing.contributor_credit if billing else 0.0,
            "completed_at": chunk.completed_at.isoformat() if chunk.completed_at else None,
        })

    # Total count
    count_result = await db.execute(
        select(func.count(Chunk.id)).where(
            Chunk.node_id == node_id, Chunk.status == ChunkStatus.COMPLETED
        )
    )
    total = count_result.scalar() or 0

    return {"history": history, "total": total, "page": page, "limit": limit}


@router.get("/stats", response_model=ClusterStats)
async def get_cluster_stats(
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    """Public endpoint: cluster-wide statistics."""
    redis_svc = RedisService(redis)

    # Total nodes
    total_result = await db.execute(select(func.count(Node.id)))
    total_nodes = total_result.scalar() or 0

    # Active nodes (heartbeat within 30s)
    active_nodes_list = await redis_svc.get_active_nodes(timeout_seconds=30)
    active_nodes = len(active_nodes_list)

    # Jobs completed today
    today_start = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    jobs_today_result = await db.execute(
        select(func.count(Job.id)).where(
            Job.status == JobStatus.COMPLETED,
            Job.completed_at >= today_start,
        )
    )
    jobs_completed_today = jobs_today_result.scalar() or 0

    # GPU hours today — real data from billing
    billing_svc = BillingService(db)
    total_gpu_hours_today = await billing_svc.get_total_gpu_hours_today()

    # GPU models available
    gpu_result = await db.execute(
        select(Node.gpu_model).distinct().where(Node.status != NodeStatus.SUSPENDED)
    )
    gpu_models = [row[0] for row in gpu_result.all()]

    return ClusterStats(
        total_nodes=total_nodes,
        active_nodes=active_nodes,
        total_gpu_hours_today=total_gpu_hours_today,
        jobs_completed_today=jobs_completed_today,
        available_gpu_models=gpu_models,
    )


@router.get("/leaderboard")
async def get_leaderboard(
    period: str = Query(default="month", pattern="^(week|month|all)$"),
    limit: int = Query(default=20, ge=1, le=100),
    current_user: TokenPayload = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get campus contributor leaderboard."""
    # Rank all nodes by total_gpu_hours
    result = await db.execute(
        select(Node)
        .where(Node.status != NodeStatus.SUSPENDED)
        .order_by(Node.total_gpu_hours.desc())
        .limit(limit)
    )
    nodes = result.scalars().all()

    leaderboard = []
    for rank, node in enumerate(nodes, start=1):
        tier = get_tier(node.total_gpu_hours)
        leaderboard.append({
            "rank": rank,
            "node_id": str(node.id),
            "hostname": node.hostname,
            "gpu_model": node.gpu_model,
            "total_gpu_hours": node.total_gpu_hours,
            "total_earned": node.total_earned,
            "reliability_score": node.reliability_score,
            "current_streak": node.current_streak,
            "tier_name": tier["name"],
            "tier_icon": tier["icon"],
            "tier_level": tier["level"],
            "is_you": str(node.user_id) == str(current_user.user_id),
        })

    # Find current user's rank if not in top N
    user_nodes = [e for e in leaderboard if e["is_you"]]
    user_rank = user_nodes[0]["rank"] if user_nodes else None

    if not user_rank:
        # Get user's best node rank
        user_result = await db.execute(
            select(Node)
            .where(Node.user_id == current_user.user_id)
            .order_by(Node.total_gpu_hours.desc())
            .limit(1)
        )
        user_node = user_result.scalar_one_or_none()
        if user_node:
            # Count how many nodes have more GPU hours
            rank_result = await db.execute(
                select(func.count(Node.id)).where(
                    Node.total_gpu_hours > user_node.total_gpu_hours,
                    Node.status != NodeStatus.SUSPENDED,
                )
            )
            user_rank = (rank_result.scalar() or 0) + 1

    return {
        "leaderboard": leaderboard,
        "your_rank": user_rank,
        "period": period,
    }
