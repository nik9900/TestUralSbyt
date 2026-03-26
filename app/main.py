import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.api.routes import analytics, ingestion, reports, system
from app.core.config import get_settings
from app.core.database import Base, engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
    logger.info("Создание таблиц базы данных при необходимости...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("База данных готова.")
    yield


def create_app() -> FastAPI:
    application = FastAPI(
        title="Тестовое",
        description=(
            "Сервис агрегации данных из внешних источников: "
            "вычисляет аналитику и экспортирует отчёты в S3."
        ),
        version="1.0.0",
        docs_url="/docs",
        redoc_url=None,
        lifespan=lifespan,
    )

    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    application.include_router(system.router)
    application.include_router(analytics.router)
    application.include_router(reports.router)
    application.include_router(ingestion.router)

    application.mount("/static", StaticFiles(directory="app/static"), name="static")

    return application


app = create_app()
