from datetime import datetime
from typing import Sequence

from sqlalchemy import select

from app.models.report import Report
from app.repositories.base import BaseRepository


class ReportRepository(BaseRepository[Report]):
    """Репозиторий для работы с записями отчётов в базе данных."""

    _model = Report

    async def list_since(self, since: datetime) -> Sequence[Report]:
        result = await self._session.execute(
            select(Report).where(Report.created_at >= since).order_by(Report.created_at.desc())
        )
        return result.scalars().all()

    async def get_by_task_id(self, task_id: str) -> Report | None:
        result = await self._session.execute(select(Report).where(Report.task_id == task_id))
        return result.scalar_one_or_none()

    async def update_status(self, task_id: str, status: str, s3_path: str | None = None) -> None:
        report = await self.get_by_task_id(task_id)
        if report:
            report.status = status
            if s3_path is not None:
                report.s3_path = s3_path
            await self._session.flush()
