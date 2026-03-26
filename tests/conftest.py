import os
from datetime import datetime, timezone

import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

TEST_ENV_DEFAULTS = {
    "DATABASE_URL": "sqlite+aiosqlite:///:memory:",
    "CELERY_BROKER_URL": "amqp://guest:guest@localhost//",
    "CELERY_RESULT_BACKEND": "rpc://",
    "S3_ENDPOINT_URL": "http://localhost:9000",
    "S3_ACCESS_KEY": "test",
    "S3_SECRET_KEY": "test",
    "S3_BUCKET_NAME": "test-bucket",
    "INGESTION_INTERVAL_SECONDS": "60",
    "TASKS_API_URL": "https://example.com/todos",
    "METRICS_API_URL": "https://example.com/posts",
}


def _bootstrap_test_env() -> None:
    for key, default_value in TEST_ENV_DEFAULTS.items():
        current_value = os.environ.get(key)
        if current_value is None or current_value == "" or current_value.startswith("$"):
            os.environ[key] = default_value


_bootstrap_test_env()

from app.core.database import Base  # noqa: E402
from app.models.metric import Metric  # noqa: E402
from app.models.report import Report  # noqa: E402
from app.models.task import Task  # noqa: E402


@pytest_asyncio.fixture
async def engine():
    eng = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await eng.dispose()


@pytest_asyncio.fixture
async def session(engine):
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        yield s


@pytest_asyncio.fixture
async def client(session):
    from app.core.database import get_db
    from app.main import create_app

    app = create_app()
    app.dependency_overrides[get_db] = lambda: session

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


def make_task(
    external_id: int = 1,
    title: str = "Test task",
    status: str = "pending",
    hours_spent: float = 2.0,
    collected_at: datetime | None = None,
) -> Task:
    return Task(
        external_id=external_id,
        title=title,
        status=status,
        hours_spent=hours_spent,
        external_created_at=datetime.now(tz=timezone.utc),
        collected_at=collected_at or datetime.now(tz=timezone.utc),
    )


def make_metric(
    sensor_id: str = "srv-01",
    cpu_load: float = 50.0,
    memory_usage: float = 0.7,
    collected_at: datetime | None = None,
) -> Metric:
    return Metric(
        sensor_id=sensor_id,
        cpu_load=cpu_load,
        memory_usage=memory_usage,
        external_timestamp=datetime.now(tz=timezone.utc),
        collected_at=collected_at or datetime.now(tz=timezone.utc),
    )


def make_report(
    task_id: str = "test-task-id",
    status: str = "pending",
    period_from: datetime | None = None,
    period_to: datetime | None = None,
) -> Report:
    now = datetime.now(tz=timezone.utc)
    return Report(
        task_id=task_id,
        status=status,
        period_from=period_from or now,
        period_to=period_to or now,
        created_at=now,
    )
