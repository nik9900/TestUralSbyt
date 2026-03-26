import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from app.core.config import Settings
from app.models.metric import Metric
from app.models.task import Task
from app.repositories.metric_repository import MetricRepository
from app.repositories.task_repository import TaskRepository
from app.schemas.ingestion import RawMetricData, RawTaskData

logger = logging.getLogger(__name__)


class IngestionService:
    """Оркестрирует загрузку данных из обоих внешних источников."""

    def __init__(
        self,
        task_repo: TaskRepository,
        metric_repo: MetricRepository,
        settings: Settings,
    ) -> None:
        """Инициализирует сервис загрузки данных с репозиториями и настройками.

        Args:
            task_repo: Репозиторий для работы с задачами.
            metric_repo: Репозиторий для работы с метриками.
            settings: Объект настроек приложения.
        """
        self._task_repo = task_repo
        self._metric_repo = metric_repo
        self._settings = settings

    async def ingest_all(self) -> dict[str, int]:
        """Загружает данные из обоих источников и сохраняет результаты.

        Returns:
            dict[str, int]: Словарь с количеством загруженных записей по источникам.
        """
        tasks_count = await self._ingest_tasks()
        metrics_count = await self._ingest_metrics()
        logger.info("Загрузка завершена: задачи=%d метрики=%d", tasks_count, metrics_count)
        return {"tasks": tasks_count, "metrics": metrics_count}

    async def _ingest_tasks(self) -> int:
        """Загружает задачи из внешнего API и сохраняет их в базу данных.

        Returns:
            int: Количество успешно загруженных записей задач.
        """
        logger.info("Запрос задач: %s", self._settings.tasks_api_url)
        raw = await self._fetch_json(self._settings.tasks_api_url)
        records: list[Task] = []
        for item in raw[:20]:
            try:
                parsed = RawTaskData(
                    id=item.get("id", 0),
                    title=item.get("title", ""),
                    status=item.get("status", "completed" if item.get("completed") else "pending"),
                    hours_spent=float(item.get("hours_spent", 0.0)),
                    created_at=item.get("created_at") or datetime.now(tz=timezone.utc),
                )
                records.append(
                    Task(
                        external_id=parsed.id,
                        title=parsed.title,
                        status=parsed.status,
                        hours_spent=parsed.hours_spent,
                        external_created_at=parsed.created_at,
                        collected_at=datetime.now(tz=timezone.utc),
                    )
                )
            except Exception as exc:
                logger.warning("Пропуск повреждённой записи задачи: %s", exc)
        await self._task_repo.bulk_add(records)
        logger.info("Сохранено задач: %d", len(records))
        return len(records)

    async def _ingest_metrics(self) -> int:
        """Загружает метрики из внешнего API и сохраняет их в базу данных.

        Делает HTTP-запрос к METRICS_API_URL и маппит ответ на поля
        модели Metric. Если источник временно недоступен, использует
        синтетический набор данных, чтобы процесс загрузки не падал
        полностью.

        Returns:
            int: Количество успешно загруженных записей метрик.
        """
        logger.info("Запрос метрик: %s", self._settings.metrics_api_url)
        try:
            raw = await self._fetch_json(self._settings.metrics_api_url)
        except httpx.HTTPError as exc:
            logger.warning(
                "Не удалось получить метрики из внешнего API (%s). "
                "Используется резервный синтетический набор данных.",
                exc,
            )
            raw = self._build_fallback_metrics_payload()
        now = datetime.now(tz=timezone.utc)
        records: list[Metric] = []
        for item in raw[:10]:
            try:
                parsed = RawMetricData(
                    sensor_id=item.get(
                        "sensor_id", f"srv-{item.get('userId', item.get('id', 0)):02d}"
                    ),
                    cpu_load=float(item.get("cpu_load", item.get("id", 0) % 100)),
                    memory_usage=float(
                        item.get("memory_usage", round((item.get("id", 0) % 10) * 0.1, 2))
                    ),
                    timestamp=item.get("timestamp") or now,
                )
                records.append(
                    Metric(
                        sensor_id=parsed.sensor_id,
                        cpu_load=parsed.cpu_load,
                        memory_usage=parsed.memory_usage,
                        external_timestamp=parsed.timestamp,
                        collected_at=now,
                    )
                )
            except Exception as exc:
                logger.warning("Пропуск повреждённой записи метрики: %s", exc)
        await self._metric_repo.bulk_add(records)
        logger.info("Сохранено метрик: %d", len(records))
        return len(records)

    @staticmethod
    def _build_fallback_metrics_payload() -> list[dict[str, int]]:
        """Возвращает синтетический набор метрик для деградационного режима."""
        return [{"id": item_id, "userId": item_id} for item_id in range(1, 11)]

    @staticmethod
    async def _fetch_json(url: str, timeout: float = 10.0) -> list[dict[str, Any]]:
        """Выполняет GET-запрос к указанному URL и возвращает JSON-ответ.

        Args:
            url: URL внешнего API для запроса данных.
            timeout: Таймаут запроса в секундах (по умолчанию 10.0).

        Returns:
            list[dict[str, Any]]: Список объектов из JSON-ответа.

        Raises:
            httpx.HTTPStatusError: При получении HTTP-ошибки от сервера.
        """
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()  # type: ignore[no-any-return]
