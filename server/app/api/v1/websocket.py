"""WebSocket connection manager and route handlers."""

import logging

import redis.asyncio as aioredis
from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from app.core.redis import RedisService

logger = logging.getLogger(__name__)
router = APIRouter(tags=["websocket"])


class ConnectionManager:
    """Manages WebSocket connections for nodes and customers."""

    def __init__(self):
        # node_id → WebSocket
        self.node_connections: dict[str, WebSocket] = {}
        # job_id → [WebSocket clients]
        self.customer_connections: dict[str, list[WebSocket]] = {}

    # ── Node Connections ────────────────────────────────────────

    async def connect_node(self, node_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        self.node_connections[node_id] = websocket
        logger.info(f"Node {node_id} connected. Total nodes: {len(self.node_connections)}")

    def disconnect_node(self, node_id: str, websocket: WebSocket) -> None:
        if self.node_connections.get(node_id) == websocket:
            self.node_connections.pop(node_id, None)
            logger.info(f"Node {node_id} disconnected. Total nodes: {len(self.node_connections)}")

    async def send_to_node(self, node_id: str, data: dict) -> bool:
        """Send a message to a specific node. Returns success."""
        ws = self.node_connections.get(node_id)
        if ws and ws.client_state == WebSocketState.CONNECTED:
            try:
                await ws.send_json(data)
                return True
            except Exception as e:
                logger.error(f"Failed to send to node {node_id}: {e}")
                self.disconnect_node(node_id, ws)
        return False

    # ── Customer Connections ────────────────────────────────────

    async def connect_customer(self, job_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        if job_id not in self.customer_connections:
            self.customer_connections[job_id] = []
        self.customer_connections[job_id].append(websocket)
        logger.info(f"Customer connected to job {job_id}. Watchers: {len(self.customer_connections[job_id])}")

    def disconnect_customer(self, job_id: str, websocket: WebSocket) -> None:
        if job_id in self.customer_connections:
            self.customer_connections[job_id] = [
                ws for ws in self.customer_connections[job_id] if ws != websocket
            ]
            if not self.customer_connections[job_id]:
                del self.customer_connections[job_id]

    async def broadcast_to_job(self, job_id: str, data: dict) -> None:
        """Broadcast a message to all customers watching a job."""
        watchers = self.customer_connections.get(job_id, [])
        disconnected = []
        for ws in watchers:
            if ws.client_state == WebSocketState.CONNECTED:
                try:
                    await ws.send_json(data)
                except Exception:
                    disconnected.append(ws)
            else:
                disconnected.append(ws)
        # Clean up disconnected
        for ws in disconnected:
            self.disconnect_customer(job_id, ws)

    async def broadcast_to_user(self, user_id: str, data: dict) -> None:
        """Broadcast to all job watchers for a given user (future: user-level channel)."""
        # For now, this is a no-op — we'd need a user→job mapping
        pass


# Global singleton
ws_manager = ConnectionManager()


# ── WebSocket Routes ────────────────────────────────────────────

@router.websocket("/ws/node/{node_id}")
async def ws_node_connection(
    websocket: WebSocket,
    node_id: str,
    token: str = Query(...),
):
    """Persistent WebSocket for contributor node agents."""
    # Validate the JWT token before accepting
    try:
        from app.core.security import decode_token
        payload = decode_token(token)
        user_id = payload.get("sub")
        if not user_id:
            await websocket.close(code=4001, reason="Invalid token")
            return
        logger.info(f"Node {node_id} authenticated as user {user_id}")
    except Exception as e:
        logger.warning(f"Node {node_id} failed auth: {e}")
        await websocket.close(code=4001, reason="Authentication failed")
        return

    # Authorization: the node must belong to the authenticated user. Otherwise a
    # valid token could connect as ANY node_id and flip its state / report status.
    try:
        from sqlalchemy import select as _sa_select
        from app.core.database import async_session as _async_session
        from app.models.node import Node as _NodeModel
        async with _async_session() as _db:
            _node = (await _db.execute(_sa_select(_NodeModel).where(_NodeModel.id == node_id))).scalar_one_or_none()
        if _node is None or str(_node.user_id) != str(user_id):
            logger.warning(f"Node {node_id} ownership check failed for user {user_id}")
            await websocket.close(code=4003, reason="Node does not belong to this user")
            return
    except Exception as e:
        logger.warning(f"Node {node_id} ownership check error: {e}")
        await websocket.close(code=4003, reason="Authorization failed")
        return

    await ws_manager.connect_node(node_id, websocket)

    # Mark node online in pg database immediately
    try:
        from sqlalchemy import update
        from app.core.database import async_session
        from app.models.node import Node as NodeModel
        async with async_session() as db:
            await db.execute(update(NodeModel).where(NodeModel.id == node_id).values(status="online"))
            await db.commit()
    except Exception as db_err:
        logger.debug(f"Could not mark node {node_id} online: {db_err}")

    from app.core.config import get_settings
    settings = get_settings()

    redis_client = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    redis_svc = RedisService(redis_client)

    try:
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")

            if msg_type == "heartbeat":
                resources = data.get("resources", {})

                # Enrich heartbeat with the Node's registered DB specs so the
                # matcher always has gpu_vram_gb / ram_gb even from bare clients.
                try:
                    from sqlalchemy import select as sa_select
                    from app.core.database import async_session as db_session
                    from app.models.node import Node as NodeModel
                    async with db_session() as db:
                        res = await db.execute(sa_select(NodeModel).where(NodeModel.id == node_id))
                        node_row = res.scalar_one_or_none()
                        if node_row:
                            resources.setdefault("gpu_vram_gb", node_row.gpu_vram_gb or 0)
                            resources.setdefault("ram_gb", node_row.ram_gb or 0)
                            resources.setdefault("bandwidth_mbps", node_row.bandwidth_mbps or 10)
                            resources.setdefault("reliability_score", node_row.reliability_score or 0.8)
                            resources.setdefault("gpu_model", node_row.gpu_model or "")
                except Exception as enrich_err:
                    logger.debug(f"Heartbeat enrich skipped: {enrich_err}")

                # Store enriched resources in Redis (used by matcher)
                await redis_svc.update_heartbeat(
                    node_id=node_id,
                    resources=resources,
                )

                # Mark node available and trigger dispatcher
                if data.get("available", False):
                    await redis_svc.mark_node_available(node_id)
                    try:
                        from app.scheduler.matcher import dispatch_next_chunk
                        dispatch_next_chunk.delay()
                    except Exception as e:
                        logger.warning(f"Failed to trigger dispatcher: {e}")
                else:
                    await redis_svc.mark_node_busy(node_id)

            elif msg_type == "chunk_ack":
                logger.info(f"Node {node_id} ACKed chunk {data.get('chunk_id')}")
                # TODO Phase 2: update chunk status to 'running'

            elif msg_type == "chunk_status":
                chunk_id = data.get("chunk_id")
                status = data.get("status")

                if status == "completed":
                    logger.info(f"Node {node_id} completed chunk {chunk_id}")
                    # Trigger the background celery success handler
                    from app.scheduler.matcher import chunk_success
                    chunk_success.delay(chunk_id, node_id)

                    if data.get("job_id"):
                        await ws_manager.broadcast_to_job(data["job_id"], data)

                elif status == "failed":
                    logger.warning(f"Node {node_id} failed chunk {chunk_id}: {data.get('error')}")
                    from app.scheduler.matcher import chunk_failed
                    chunk_failed.delay(chunk_id, node_id)
                    
                    if data.get("job_id"):
                        await ws_manager.broadcast_to_job(data["job_id"], data)

                elif status == "running":
                    if data.get("job_id"):
                        await ws_manager.broadcast_to_job(data["job_id"], data)

            elif msg_type == "log":
                if data.get("job_id"):
                    await ws_manager.broadcast_to_job(data["job_id"], data)

    except WebSocketDisconnect:
        logger.info(f"Node {node_id} disconnected.")
    except Exception as e:
        logger.exception(f"CRASH in node WebSocket {node_id}: {e}")
    finally:
        is_active = ws_manager.node_connections.get(node_id) == websocket
        ws_manager.disconnect_node(node_id, websocket)
        if is_active:
            if 'redis_svc' in locals():
                try:
                    await redis_svc.remove_node(node_id)
                except:
                    pass
            
            try:
                from sqlalchemy import update
                from app.core.database import async_session
                from app.models.node import Node as NodeModel
                async with async_session() as db:
                    await db.execute(update(NodeModel).where(NodeModel.id == node_id).values(status="offline"))
                    await db.commit()
            except Exception as db_err:
                logger.debug(f"Could not mark node {node_id} offline in pg: {db_err}")
            
        if 'redis_client' in locals():
            await redis_client.aclose()


@router.websocket("/ws/job/{job_id}")
async def ws_job_monitor(
    websocket: WebSocket,
    job_id: str,
    token: str = Query(default=""),
):
    """WebSocket for customer to monitor a job in real-time."""
    # Validate token
    try:
        from app.core.security import decode_token
        payload = decode_token(token)
        user_id = payload.get("sub")
        role = payload.get("role")
        if not user_id:
            await websocket.close(code=4001, reason="Invalid token")
            return
    except Exception as e:
        logger.warning(f"Job WS auth failed for {job_id}: {e}")
        await websocket.close(code=4001, reason="Authentication failed")
        return

    # Authorization: only the job's owner (or an admin) may watch it.
    try:
        from sqlalchemy import select as _sa_select
        from app.core.database import async_session as _async_session
        from app.models.job import Job as _JobModel
        async with _async_session() as _db:
            _job = (await _db.execute(_sa_select(_JobModel).where(_JobModel.id == job_id))).scalar_one_or_none()
        if _job is None or (str(_job.user_id) != str(user_id) and role != "admin"):
            logger.warning(f"Job WS ownership check failed: job {job_id} user {user_id}")
            await websocket.close(code=4003, reason="Not authorized for this job")
            return
    except Exception as e:
        logger.warning(f"Job WS ownership check error for {job_id}: {e}")
        await websocket.close(code=4003, reason="Authorization failed")
        return

    await ws_manager.connect_customer(job_id, websocket)
    
    # Pre-send current state if pipeline analysis already completed (fixes WebSocket connection race condition)
    try:
        if _job.status == "needs_dockerfile":
            await websocket.send_json({
                "type": "detection_step",
                "job_id": job_id,
                "step": "needs_dockerfile",
                "detail": (_job.profile or {}).get("error_details", "Dockerfile required for this custom workload.")
            })
        elif _job.status == "failed":
            await websocket.send_json({
                "type": "detection_step",
                "job_id": job_id,
                "step": "failed",
                "detail": (_job.profile or {}).get("error_details", "AI Pipeline analysis failed.")
            })
        elif _job.status not in ("analyzing",):
            await websocket.send_json({
                "type": "pipeline_complete",
                "job_id": job_id,
                "profile": _job.profile
            })
    except Exception as pre_send_err:
        logger.warning(f"Failed to send initial job state to WS client: {pre_send_err}")

    try:
        while True:
            # Keep connection alive — customer doesn't send much
            data = await websocket.receive_text()
            # Could handle ping/pong or client-side events here
    except WebSocketDisconnect:
        ws_manager.disconnect_customer(job_id, websocket)
    except Exception as e:
        logger.error(f"Job WS error for {job_id}: {e}")
        ws_manager.disconnect_customer(job_id, websocket)
