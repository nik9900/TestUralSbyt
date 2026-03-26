from datetime import datetime

from pydantic import BaseModel


class SystemLoadSchema(BaseModel):
    """Схема агрегированных показателей нагрузки системы."""

    avg_cpu: float
    max_memory: float


class AnalyticsSummarySchema(BaseModel):
    """Схема сводной аналитики за последние 24 часа."""

    period: str = "last_24h"
    total_tasks_collected: int
    total_tasks_completed: int
    average_hours_per_task: float
    system_load: SystemLoadSchema
    generated_at: datetime
