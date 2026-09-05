from fastapi import APIRouter, Depends, status, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.api.dependencies import get_db

from app.core.redis import redis_manager

router = APIRouter()

@router.get("/live", status_code=status.HTTP_200_OK)
async def check_liveness():
    return {"status": "ok"}

@router.get("/ready", status_code=status.HTTP_200_OK)
async def check_readiness(db: AsyncSession = Depends(get_db)):
    db_ok = False
    try:
        await db.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        db_ok = False

    redis_ok = await redis_manager.ping()

    if not db_ok:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"database": "down", "redis": "up" if redis_ok else "down"}
        )

    return {
        "status": "ready",
        "database": "up",
        "redis": "up" if redis_ok else "down"
    }


