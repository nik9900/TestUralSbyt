from datetime import datetime

from pydantic import BaseModel


class RawTaskData(BaseModel):
    """Схема сырых данных задачи, полученных из внешнего API."""

    id: int
    title: str
    status: str = "unknown"
    hours_spent: float = 0.0
    created_at: datetime | None = None


class RawMetricData(BaseModel):
    """Схема сырых данных метрики, полученных из внешнего API."""

    sensor_id: str
    cpu_load: float = 0.0
    memory_usage: float = 0.0
    timestamp: datetime | None = None
