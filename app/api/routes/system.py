from fastapi import APIRouter
from fastapi.responses import FileResponse

router = APIRouter(tags=["Система"])


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/dashboard", include_in_schema=False)
async def dashboard() -> FileResponse:
    return FileResponse("app/static/index.html")
