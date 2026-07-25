import asyncio
import redis.asyncio as aioredis
from app.core.database import async_session
from sqlalchemy import text

async def purge_all():
    print("🧹 [1/2] Purging PostgreSQL records...")
    async with async_session() as session:
        # Just truncate the heavy tables via direct sql
        await session.execute(text("TRUNCATE chunks CASCADE"))
        await session.execute(text("TRUNCATE jobs CASCADE"))
        print("  ✅ PostgreSQL Tables TRUNCATED successfully.")
        await session.commit()
    
    print("🧹 [2/2] Flushing Redis Store...")
    try:
        import os
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        r = aioredis.Redis.from_url(redis_url, decode_responses=True)
        await r.flushdb()
        await r.aclose()
        print("  ✅ Redis Flushed.")
    except Exception as e:
        print(f"  ⚠️ Error flushing redis: {e}")

    print("🧹 [3/3] Purging MinIO files...")
    try:
        from app.services.minio_service import minio_client, REQUIRED_BUCKETS
        for bucket in REQUIRED_BUCKETS:
            if minio_client.bucket_exists(bucket):
                objects = list(minio_client.list_objects(bucket, recursive=True))
                if objects:
                    for obj in objects:
                        minio_client.remove_object(bucket, obj.object_name)
                    print(f"  ✅ Cleared {len(objects)} objects from bucket '{bucket}'.")
                else:
                    print(f"  ✅ Bucket '{bucket}' is already empty.")
    except Exception as e:
        print(f"  ⚠️ Error clearing MinIO: {e}")

    print("🎉 System Data Completely Purged!")

if __name__ == "__main__":
    asyncio.run(purge_all())
