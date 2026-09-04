from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import AsyncSessionLocal

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session

async def get_current_user():
    # Placeholder for Phase 2/3: authentication dependency
    # Will extract and verify JWT/OAuth2 token
    # Currently returns a dummy user ID or just None
    pass

