"""Pydantic schemas for Job endpoints."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field

# ── Response Schemas ────────────────────────────────────────────

class JobSubmitResponse(BaseModel):
    job_id: UUID
    status: str = "analyzing"
    message: str = "Job submitted. AI pipeline is analyzing your files."


class ChunkResponse(BaseModel):
    id: UUID
    chunk_index: int
    status: str
    node_id: UUID | None
    gpu_hours: float
    cost: float
    retry_count: int
    assigned_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None

    model_config = {"from_attributes": True}


class JobStatusResponse(BaseModel):
    id: UUID
    type: str | None
    status: str
    ml_sync_mode: str | None
    container_image: str | None
    profile: dict | None
    cost_estimate: dict | None
    actual_cost: float
    total_chunks: int
    completed_chunks: int
    input_path: str | None
    output_path: str | None
    presigned_url: str | None
    created_at: datetime
    updated_at: datetime | None
    completed_at: datetime | None
    chunks: list[ChunkResponse] = []

    model_config = {"from_attributes": True}


class JobListItem(BaseModel):
    id: UUID
    type: str | None
    status: str
    total_chunks: int
    completed_chunks: int
    actual_cost: float
    created_at: datetime
    completed_at: datetime | None

    model_config = {"from_attributes": True}


class PaginatedJobs(BaseModel):
    jobs: list[JobListItem]
    total: int
    page: int
    limit: int


class PriceEstimate(BaseModel):
    gpu_tier: str
    estimated_hours: float
    base_price_per_hour: float
    dynamic_multiplier: float
    total_estimate: float
    comparison: dict = Field(
        ...,
        examples=[{
            "colab_pro": 2.44,
            "aws_p3": 12.24,
            "runpod": 1.76,
            "campugrid": 1.92,
            "savings_vs_colab": "21%",
            "savings_vs_aws": "84%",
        }],
    )
