"""CampuGrid Server Configuration — loads from environment variables."""

import json
from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from .env file or environment."""

    # === Database ===
    DATABASE_URL: str = "postgresql+asyncpg://campugrid:campugrid_dev_password@localhost:5432/campugrid"

    # === Redis ===
    REDIS_URL: str = "redis://localhost:6379/0"

    # === MinIO ===
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "campugrid"
    MINIO_SECRET_KEY: str = "campugrid_dev_secret"
    MINIO_SECURE: bool = False

    # Public-facing S3 endpoint used when SIGNING presigned URLs that are handed
    # to remote contributor nodes and customer browsers. Internal server↔MinIO
    # traffic still uses MINIO_ENDPOINT; only the host embedded in presigned URLs
    # changes. Leave blank to fall back to MINIO_ENDPOINT (single-machine dev).
    PUBLIC_MINIO_ENDPOINT: str = ""
    PUBLIC_MINIO_SECURE: bool = True

    # === JWT Auth ===
    JWT_SECRET_KEY: str = "dev-secret-key-change-in-production-please"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_MINUTES: int = 1440  # 24 hours

    # === Google OAuth ===
    GOOGLE_CLIENT_ID: str = ""
    GOOGLE_CLIENT_SECRET: str = ""

    # === Gemini API ===
    GEMINI_API_KEY: str = ""
    GEMINI_MODEL: str = "gemini-2.5-flash"

    # === Sentry ===
    SENTRY_DSN: str = ""

    # === Server ===
    SERVER_HOST: str = "0.0.0.0"
    SERVER_PORT: int = 8000
    CORS_ORIGINS: str = '["http://localhost:3000", "http://localhost:1420", "http://127.0.0.1:1420", "http://localhost:3001", "http://localhost:3002"]'

    # === MinIO Buckets ===
    BUCKET_JOB_INPUTS: str = "job-inputs"
    BUCKET_JOB_OUTPUTS: str = "job-outputs"
    BUCKET_CHECKPOINTS: str = "checkpoints"
    BUCKET_BUILD_CONTEXTS: str = "build-contexts"

    @property
    def cors_origins_list(self) -> list[str]:
        return json.loads(self.CORS_ORIGINS)

    model_config = {
        "env_file": "../infra/.env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }


@lru_cache
def get_settings() -> Settings:
    """Cached settings singleton."""
    return Settings()
