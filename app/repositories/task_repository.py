from datetime import datetime
from typing import Sequence

from sqlalchemy import select

from app.models.task import Task
from app.repositories.base import BaseRepository


class TaskRepository(BaseRepository[Task]):
    """Репозиторий для работы с записями задач в базе данных."""

    _model = Task

    async def list_since(self, since: datetime) -> Sequence[Task]:
        result = await self._session.execute(
            select(Task).where(Task.collected_at >= since).order_by(Task.collected_at.desc())
        )
        return result.scalars().all()

    async def list_between(self, from_dt: datetime, to_dt: datetime) -> Sequence[Task]:
        result = await self._session.execute(
            select(Task)
            .where(Task.collected_at >= from_dt, Task.collected_at <= to_dt)
            .order_by(Task.collected_at)
        )
        return result.scalars().all()
