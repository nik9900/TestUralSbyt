from fastapi import APIRouter, status

router = APIRouter(prefix="/ingestion", tags=["Сбор данных"])


@router.post(
    "/trigger",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Запустите процесс приема вручную.",
)
async def trigger_ingestion() -> dict[str, str]:
    """Запускает немедленный цикл загрузки данных через Celery.

    Отправляет задачу Сбор данных  в очередь без ожидания планировщика.
    В штатном режиме планировщик запускает загрузку каждые 10 минут,
    но этот эндпоинт позволяет инициировать её вручную.

    Returns:
        dict: Словарь с task_id запущенной задачи и её статусом.
    """
    from app.tasks.worker import run_ingestion

    task = run_ingestion.delay()
    return {"task_id": task.id, "status": "queued"}
