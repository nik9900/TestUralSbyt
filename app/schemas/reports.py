from datetime import datetime

from pydantic import BaseModel, Field


class ReportGenerateRequest(BaseModel):
    """Схема запроса на генерацию CSV-отчёта."""

    period_from: datetime = Field(..., description="Начало периода выгрузки данных (UTC)")
    period_to: datetime = Field(..., description="Конец периода выгрузки данных (UTC)")


class ReportGenerateResponse(BaseModel):
    """Схема ответа на запрос постановки отчёта в очередь генерации."""

    task_id: str
    status: str
    message: str


class ReportStatusResponse(BaseModel):
    """Схема ответа с текущим статусом задачи генерации отчёта."""

    task_id: str
    status: str
    s3_path: str | None = None
    created_at: datetime
