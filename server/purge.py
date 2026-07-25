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

    print("🎉 System Data Completely Purged!")

if __name__ == "__main__":
    asyncio.run(purge_all())
