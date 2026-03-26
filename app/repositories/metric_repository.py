from datetime import datetime
from typing import Sequence

from sqlalchemy import select

from app.models.metric import Metric
from app.repositories.base import BaseRepository


class MetricRepository(BaseRepository[Metric]):
    """Репозиторий для работы с записями системных метрик в базе данных."""

    _model = Metric

    async def list_since(self, since: datetime) -> Sequence[Metric]:
        result = await self._session.execute(
            select(Metric).where(Metric.collected_at >= since).order_by(Metric.collected_at.desc())
        )
        return result.scalars().all()

    async def list_between(self, from_dt: datetime, to_dt: datetime) -> Sequence[Metric]:
        result = await self._session.execute(
            select(Metric)
            .where(Metric.collected_at >= from_dt, Metric.collected_at <= to_dt)
            .order_by(Metric.collected_at)
        )
        return result.scalars().all()
