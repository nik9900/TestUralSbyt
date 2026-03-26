from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.core.config import get_settings

settings = get_settings()

engine = create_async_engine(
    settings.database_url,
    echo=False,
    pool_pre_ping=True,
)

AsyncSessionFactory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


class Base(DeclarativeBase):
    """Базовый класс для всех ORM-моделей приложения."""

    pass


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Зависимость FastAPI: возвращает асинхронную сессию базы данных.

    Yields:
        AsyncSession: Активная сессия SQLAlchemy с автоматическим
            коммитом при успехе и откатом при возникновении ошибки.

    Raises:
        Exception: Любое исключение, возникшее во время запроса,
            после выполнения отката транзакции.
    """
    async with AsyncSessionFactory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
