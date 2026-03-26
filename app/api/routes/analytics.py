from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.repositories.metric_repository import MetricRepository
from app.repositories.task_repository import TaskRepository
from app.schemas.analytics import AnalyticsSummarySchema
from app.services.analytics_service import AnalyticsService

router = APIRouter(prefix="/analytics", tags=["Аналитика"])


def _get_analytics_service(db: AsyncSession = Depends(get_db)) -> AnalyticsService:
    """Фабрика зависимостей: создаёт сервис аналитики с внедрёнными репозиториями.

    Args:
        db: Асинхронная сессия базы данных, предоставляемая FastAPI.

    Returns:
        AnalyticsService: Настроенный экземпляр сервиса аналитики.
    """
    return AnalyticsService(
        task_repo=TaskRepository(db),
        metric_repo=MetricRepository(db),
    )


@router.get(
    "/summary",
    response_model=AnalyticsSummarySchema,
    summary="Сводная статистика за последние 24 часа",
)
async def get_summary(
    service: AnalyticsService = Depends(_get_analytics_service),
) -> AnalyticsSummarySchema:
    """Возвращает агрегированную статистику за последние 24 часа.

    Вычисляет и возвращает:
        - Общее количество собранных и завершённых задач.
        - Среднее время на задачу в часах.
        - Среднюю нагрузку CPU и максимальное использование памяти по всем сенсорам.

    Returns:
        AnalyticsSummarySchema: Схема с агрегированными показателями.
    """
    return await service.get_summary()
