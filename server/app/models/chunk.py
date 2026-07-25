"""Chunk model — individual work unit assigned to a node."""

import enum
from datetime import datetime
from uuid import UUID

from sqlalchemy import JSON, DateTime, Enum, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class ChunkStatus(str, enum.Enum):
    PENDING = "pending"
    ASSIGNED = "assigned"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    REQUEUED = "requeued"


class Chunk(Base):
    __tablename__ = "chunks"

    job_id: Mapped[UUID] = mapped_column(ForeignKey("jobs.id"), nullable=False)
    node_id: Mapped[UUID | None] = mapped_column(ForeignKey("nodes.id"), nullable=True)
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)

    # State
    status: Mapped[ChunkStatus] = mapped_column(
        Enum(ChunkStatus, name="chunk_status"),
        default=ChunkStatus.PENDING,
        nullable=False,
    )

    # Execution details
    spec: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    checkpoint_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    # True for the final "assembly" task that compiles chunks into the final output.
    # Assembly chunks run on a node just like render chunks but are scheduled only
    # after all render chunks complete.
    is_assembly: Mapped[bool] = mapped_column(Integer, default=0, nullable=False)


    # Metrics
    gpu_hours: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    cost: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    estimated_seconds: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Timestamps
    assigned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Relationships
    job = relationship("Job", back_populates="chunks")
    node = relationship("Node", back_populates="chunks")
    billing_record = relationship("BillingRecord", back_populates="chunk", uselist=False)

    def __repr__(self) -> str:
        return f"<Chunk {self.chunk_index} of job={self.job_id} status={self.status.value}>"
