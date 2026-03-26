import asyncio
from logging.config import fileConfig

from sqlalchemy import pool
from sqlalchemy.ext.asyncio import create_async_engine

import app.models
from alembic import context
from app.core.config import get_settings
from app.core.database import Base

config = context.config
settings = get_settings()

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Запуск миграций в оффлайн-режиме без активного соединения с базой данных.

    Настраивает контекст Alembic с использованием URL базы данных
    напрямую, без создания движка SQLAlchemy.
    """
    context.configure(
        url=settings.database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection) -> None:
    """Выполнение миграций через существующее соединение с базой данных.

    Args:
        connection: Активное соединение с базой данных, переданное Alembic.
    """
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Создание асинхронного движка и запуск миграций базы данных.

    Создаёт временный асинхронный движок SQLAlchemy, устанавливает
    соединение и делегирует выполнение миграций синхронной функции.
    """
    async_engine = create_async_engine(settings.database_url, poolclass=pool.NullPool)
    async with async_engine.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await async_engine.dispose()


def run_migrations_online() -> None:
    """Запуск миграций в онлайн-режиме через асинхронный движок SQLAlchemy.

    Оборачивает асинхронную функцию миграций в синхронный вызов
    через asyncio.run для совместимости с интерфейсом Alembic.
    """
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
