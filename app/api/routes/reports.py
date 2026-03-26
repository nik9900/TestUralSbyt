from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.storage import S3Client
from app.models.report import Report
from app.repositories.metric_repository import MetricRepository
from app.repositories.report_repository import ReportRepository
from app.repositories.task_repository import TaskRepository
from app.schemas.reports import ReportGenerateRequest, ReportGenerateResponse, ReportStatusResponse
from app.services.report_service import ReportService

router = APIRouter(prefix="/reports", tags=["Экспорт отчетов"])


def _get_report_service(db: AsyncSession = Depends(get_db)) -> ReportService:
    return ReportService(
        task_repo=TaskRepository(db),
        metric_repo=MetricRepository(db),
        report_repo=ReportRepository(db),
    )


async def _get_report_or_404(task_id: str, service: ReportService) -> Report:
    report = await service.get_status(task_id)
    if report is None:
        raise HTTPException(
            status_code=404, detail=f"Отчёт с идентификатором {task_id!r} не найден"
        )
    return report


@router.post(
    "/generate",
    response_model=ReportGenerateResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Поставить в очередь задачу генерации CSV-отчета",
)
async def generate_report(
    body: ReportGenerateRequest,
    service: ReportService = Depends(_get_report_service),
) -> ReportGenerateResponse:
    from app.tasks.worker import generate_report as celery_task

    task_id = await service.schedule_report(body.period_from, body.period_to)
    celery_task.delay(task_id)

    return ReportGenerateResponse(
        task_id=task_id,
        status="pending",
        message="Создание отчета поставлено в очередь. Для получения информации о "
        "статусе обратитесь к каталогу /reports/{task_id}",
    )


@router.get(
    "/{task_id}",
    response_model=ReportStatusResponse,
    summary="Проверить статус формирования отчета",
)
async def get_report_status(
    task_id: str,
    service: ReportService = Depends(_get_report_service),
) -> ReportStatusResponse:
    report = await _get_report_or_404(task_id, service)
    return ReportStatusResponse(
        task_id=report.task_id,
        status=report.status,
        s3_path=report.s3_path,
        created_at=report.created_at,
    )


@router.get(
    "/{task_id}/download",
    summary="Скачать готовый CSV-отчёт",
)
async def download_report(
    task_id: str,
    service: ReportService = Depends(_get_report_service),
) -> Response:
    report = await _get_report_or_404(task_id, service)
    if report.status != "done" or not report.s3_path:
        raise HTTPException(status_code=400, detail=f"Отчёт ещё не готов (статус: {report.status})")

    key = report.s3_path.split("/", 3)[-1]
    data = S3Client().get_object(key)
    filename = key.split("/")[-1]
    return Response(
        content=data,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
