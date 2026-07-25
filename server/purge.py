import asyncio
import os

import redis.asyncio as aioredis
from sqlalchemy import text

from app.core.database import async_session


async def purge_all():
    print("Purging system...\n")

    # -- 1. PostgreSQL ---------------------------------------------------------
    print("[1/3] Purging PostgreSQL records...")
    async with async_session() as session:
        # Apply pending schema migrations
        try:
            await session.execute(
                text(
                    "ALTER TABLE chunks ADD COLUMN IF NOT EXISTS "
                    "is_assembly INTEGER NOT NULL DEFAULT 0"
                )
            )
            print("  OK  Schema: is_assembly column ensured.")
        except Exception as mig_err:
            print(f"  --  Schema migration skipped: {mig_err}")

        # Truncate in dependency order
        await session.execute(text("TRUNCATE billing_records CASCADE"))
        await session.execute(text("TRUNCATE chunks CASCADE"))
        await session.execute(text("TRUNCATE jobs CASCADE"))

        # Reset node stats — cast string to the postgres enum type explicitly
        await session.execute(
            text(
                "UPDATE nodes SET "
                "  status = 'offline'::node_status, "
                "  reliability_score = 0.8, "
                "  current_streak = 0, "
                "  last_contribution_date = NULL"
            )
        )

        await session.commit()
        print("  OK  billing_records, chunks, jobs TRUNCATED.")
        print("  OK  Nodes reset to offline / default reliability.\n")

    # -- 2. Redis --------------------------------------------------------------
    print("[2/3] Flushing Redis store...")
    try:
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        r = aioredis.Redis.from_url(redis_url, decode_responses=True)
        await r.flushdb()
        await r.aclose()
        print("  OK  Redis flushed (all keys removed).\n")
    except Exception as e:
        print(f"  WARN  Error flushing Redis: {e}\n")

    # -- 3. MinIO / S3 ---------------------------------------------------------
    print("[3/3] Purging MinIO buckets...")
    try:
        from app.services.minio_service import minio_client, REQUIRED_BUCKETS

        total_deleted = 0
        for bucket in REQUIRED_BUCKETS:
            if minio_client.bucket_exists(bucket):
                objects = list(minio_client.list_objects(bucket, recursive=True))
                if objects:
                    for obj in objects:
                        minio_client.remove_object(bucket, obj.object_name)
                    total_deleted += len(objects)
                    print(f"  OK  Cleared {len(objects)} object(s) from '{bucket}'.")
                else:
                    print(f"  OK  Bucket '{bucket}' already empty.")
            else:
                print(f"  --  Bucket '{bucket}' does not exist, skipping.")

        print(f"\n  Total MinIO objects deleted: {total_deleted}\n")
    except Exception as e:
        print(f"  WARN  Error clearing MinIO: {e}\n")

    print("-" * 50)
    print("System completely purged and ready for a fresh run!")
    print("Next step: restart the daemon on your laptop to reconnect.\n")


if __name__ == "__main__":
    asyncio.run(purge_all())
