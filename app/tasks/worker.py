import asyncio
import logging
from typing import Any

from celery import Celery
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.core.storage import S3Client
from app.repositories.metric_repository import MetricRepository
from app.repositories.report_repository import ReportRepository
from app.repositories.task_repository import TaskRepository
from app.services.ingestion_service import IngestionService
from app.services.report_service import ReportService

logger = logging.getLogger(__name__)
settings = get_settings()

celery_app = Celery(
    "datacollector",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    beat_schedule={
        "ingest-every-10-minutes": {
            "task": "app.tasks.worker.run_ingestion",
            "schedule": 600,
        },
    },
)

_sync_url = settings.database_url.replace("+asyncpg", "+psycopg2")
_sync_engine = create_engine(_sync_url, pool_pre_ping=True)
SyncSession = sessionmaker(bind=_sync_engine, autoflush=False, autocommit=False)


def _get_sync_session() -> Session:
    return SyncSession()


def _make_async_session_factory() -> async_sessionmaker[AsyncSession]:
    engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _run_ingestion_task(self: Any) -> dict[str, int]:
    logger.info("Запуск загрузки данных (попытка %d)", self.request.retries + 1)
    session = _get_sync_session()
    try:
        counts = asyncio.get_event_loop().run_until_complete(_async_ingest())
        session.commit()
        logger.info("Загрузка данных завершена: %s", counts)
        return counts
    except Exception as exc:
        session.rollback()
        logger.warning("Ошибка загрузки данных: %s — повтор попытки", exc)
        raise
    finally:
        session.close()


run_ingestion: Any = celery_app.task(
    bind=True,
    name="app.tasks.worker.run_ingestion",
    max_retries=5,
    default_retry_delay=30,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
)(_run_ingestion_task)


async def _async_ingest() -> dict[str, int]:
    factory = _make_async_session_factory()
    async with factory() as session:
        async with session.begin():
            svc = IngestionService(TaskRepository(session), MetricRepository(session), settings)
            return await svc.ingest_all()


def _generate_report_task(self: Any, task_id: str) -> str:
    logger.info("Генерация отчёта task_id=%s", task_id)
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_async_generate_report(task_id))
    except Exception as exc:
        logger.exception("Задача генерации отчёта %s завершилась ошибкой: %s", task_id, exc)
        raise self.retry(exc=exc)
    finally:
        loop.close()


generate_report: Any = celery_app.task(
    bind=True,
    name="app.tasks.worker.generate_report",
    max_retries=3,
    default_retry_delay=10,
)(_generate_report_task)


async def _async_generate_report(task_id: str) -> str:
    factory = _make_async_session_factory()
    async with factory() as session:
        async with session.begin():
            svc = ReportService(
                TaskRepository(session),
                MetricRepository(session),
                ReportRepository(session),
                S3Client(),
            )
            return await svc.generate_and_upload(task_id)
