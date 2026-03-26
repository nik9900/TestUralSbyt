import io
import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from app.models.report import Report
from app.repositories.metric_repository import MetricRepository
from app.repositories.report_repository import ReportRepository
from app.repositories.task_repository import TaskRepository

if TYPE_CHECKING:
    from app.core.storage import S3Client

logger = logging.getLogger(__name__)


class ReportService:
    """Генерирует CSV-отчёты из данных базы данных и загружает их в S3."""

    def __init__(
        self,
        task_repo: TaskRepository,
        metric_repo: MetricRepository,
        report_repo: ReportRepository,
        s3_client: "S3Client | None" = None,
    ) -> None:
        """Инициализирует сервис отчётов с репозиториями и клиентом S3.

        Args:
            task_repo: Репозиторий для работы с задачами.
            metric_repo: Репозиторий для работы с метриками.
            report_repo: Репозиторий для работы с отчётами.
            s3_client: Клиент для загрузки файлов в S3 хранилище.
                Не требуется для операций, которые только создают
                запись отчёта или возвращают его статус.
        """
        self._task_repo = task_repo
        self._metric_repo = metric_repo
        self._report_repo = report_repo
        self._s3 = s3_client

    async def schedule_report(self, period_from: datetime, period_to: datetime) -> str:
        """Создаёт запись отчёта в статусе pending и возвращает его task_id.

        Args:
            period_from: Начало периода выгрузки данных.
            period_to: Конец периода выгрузки данных.

        Returns:
            str: Уникальный идентификатор задачи генерации отчёта.
        """
        task_id = str(uuid.uuid4())
        report = Report(
            task_id=task_id,
            status="pending",
            period_from=period_from,
            period_to=period_to,
            created_at=datetime.now(tz=timezone.utc),
        )
        await self._report_repo.add(report)
        return task_id

    async def generate_and_upload(self, task_id: str) -> str:
        """Формирует CSV-отчёт, загружает его в S3 и обновляет статус в базе.

        Args:
            task_id: Уникальный идентификатор задачи генерации отчёта.

        Returns:
            str: Путь к файлу отчёта в S3 хранилище.

        Raises:
            ValueError: Если отчёт с указанным task_id не найден в базе данных.
            Exception: При любой ошибке генерации или загрузки отчёта,
                статус записи обновляется до failed.
        """
        report = await self._report_repo.get_by_task_id(task_id)
        if report is None:
            raise ValueError(f"Report with task_id {task_id!r} not found")

        if self._s3 is None:
            raise RuntimeError("S3 client is required to generate and upload reports")

        await self._report_repo.update_status(task_id, "processing")
        try:
            csv_bytes = await self._build_csv(report.period_from, report.period_to)
            s3_key = f"reports/{task_id}.csv"
            s3_path = self._s3.upload_file(s3_key, csv_bytes)
            await self._report_repo.update_status(task_id, "done", s3_path)
            return s3_path
        except Exception as exc:
            await self._report_repo.update_status(task_id, "failed")
            logger.exception("Ошибка генерации отчёта для task_id=%s: %s", task_id, exc)
            raise

    async def get_status(self, task_id: str) -> Report | None:
        """Возвращает текущий статус отчёта по идентификатору задачи.

        Args:
            task_id: Уникальный идентификатор задачи генерации отчёта.

        Returns:
            Report | None: Объект отчёта или None, если не найден.
        """
        return await self._report_repo.get_by_task_id(task_id)

    async def _build_csv(self, from_dt: datetime, to_dt: datetime) -> bytes:
        """Формирует CSV-файл с данными задач и метрик за указанный период.

        Args:
            from_dt: Начало временного диапазона выборки данных.
            to_dt: Конец временного диапазона выборки данных.

        Returns:
            bytes: Содержимое CSV-файла в байтовом представлении (UTF-8).
        """
        tasks = await self._task_repo.list_between(from_dt, to_dt)
        metrics = await self._metric_repo.list_between(from_dt, to_dt)

        output = io.StringIO()
        output.write("=== TASKS ===\n")
        if tasks:
            tasks_df = pd.DataFrame(
                [
                    {
                        "Источник": "задачи",
                        "Внешний ИД": task_record.external_id,
                        "Название": task_record.title,
                        "Статус": task_record.status,
                        "Часов затрачено": task_record.hours_spent,
                        "Дата сбора": task_record.collected_at,
                    }
                    for task_record in tasks
                ]
            )
            tasks_df.to_csv(output, index=False)
        else:
            output.write("No task records for the selected period.\n")

        output.write("\n=== METRICS ===\n")
        if metrics:
            metrics_df = pd.DataFrame(
                [
                    {
                        "Источник": "метрики",
                        "ИД сенсора": metric_record.sensor_id,
                        "Нагрузка ЦП (%)": metric_record.cpu_load,
                        "Использование памяти": metric_record.memory_usage,
                        "Дата сбора": metric_record.collected_at,
                    }
                    for metric_record in metrics
                ]
            )
            metrics_df.to_csv(output, index=False)
        else:
            output.write("No metric records for the selected period.\n")

        return output.getvalue().encode("utf-8-sig")
