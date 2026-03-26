import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd

from app.models.metric import Metric
from app.models.task import Task
from app.repositories.metric_repository import MetricRepository
from app.repositories.task_repository import TaskRepository
from app.schemas.analytics import AnalyticsSummarySchema, SystemLoadSchema

logger = logging.getLogger(__name__)


class AnalyticsService:
    """Вычисляет сводную статистику за последние 24 часа."""

    def __init__(self, task_repo: TaskRepository, metric_repo: MetricRepository) -> None:
        """Инициализирует сервис аналитики с репозиториями данных.

        Args:
            task_repo: Репозиторий для работы с задачами.
            metric_repo: Репозиторий для работы с метриками.
        """
        self._task_repo = task_repo
        self._metric_repo = metric_repo

    async def get_summary(self) -> AnalyticsSummarySchema:
        """Формирует сводную аналитику за последние 24 часа.

        Returns:
            AnalyticsSummarySchema: Схема с агрегированными показателями
                по задачам и нагрузке системы.
        """
        since = datetime.now(tz=timezone.utc) - timedelta(hours=24)

        tasks = await self._task_repo.list_since(since)
        metrics = await self._metric_repo.list_since(since)

        task_stats = self._compute_task_stats(list(tasks))
        system_load = self._compute_system_load(list(metrics))

        return AnalyticsSummarySchema(
            period="last_24h",
            total_tasks_collected=task_stats["total"],
            total_tasks_completed=task_stats["completed"],
            average_hours_per_task=task_stats["avg_hours"],
            system_load=system_load,
            generated_at=datetime.now(tz=timezone.utc),
        )

    @staticmethod
    def _compute_task_stats(tasks: list[Task]) -> dict[str, Any]:
        """Вычисляет статистику по задачам с помощью Pandas.

        Args:
            tasks: Список ORM-объектов Task за анализируемый период.

        Returns:
            dict: Словарь с ключами total, completed, avg_hours.
        """
        if not tasks:
            return {"total": 0, "completed": 0, "avg_hours": 0.0}

        task_dataframe = pd.DataFrame(
            [
                {"status": task_record.status, "hours_spent": task_record.hours_spent}
                for task_record in tasks
            ]
        )

        completed = int((task_dataframe["status"] == "completed").sum())
        avg_hours = (
            float(round(task_dataframe["hours_spent"].mean(), 2))
            if len(task_dataframe) > 0
            else 0.0
        )

        return {"total": len(task_dataframe), "completed": completed, "avg_hours": avg_hours}

    @staticmethod
    def _compute_system_load(metrics: list[Metric]) -> SystemLoadSchema:
        """Вычисляет агрегированные показатели нагрузки системы с помощью NumPy.

        Args:
            metrics: Список ORM-объектов Metric за анализируемый период.

        Returns:
            SystemLoadSchema: Схема со средней нагрузкой CPU и максимальным
                использованием памяти.
        """
        if not metrics:
            return SystemLoadSchema(avg_cpu=0.0, max_memory=0.0)

        cpu_values = np.array([metric_record.cpu_load for metric_record in metrics], dtype=float)
        mem_values = np.array(
            [metric_record.memory_usage for metric_record in metrics], dtype=float
        )

        return SystemLoadSchema(
            avg_cpu=round(float(np.mean(cpu_values)), 2),
            max_memory=round(float(np.max(mem_values)), 2),
        )
