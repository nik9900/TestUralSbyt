from datetime import datetime

from sqlalchemy import DateTime, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class Report(Base):
    """ORM-модель для отслеживания задач генерации отчётов и их расположения в S3."""

    __tablename__ = "reports"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(String(256), nullable=False, unique=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    s3_path: Mapped[str] = mapped_column(String(1024), nullable=True)
    period_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    period_to: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    def __repr__(self) -> str:
        """Возвращает строковое представление объекта Report.

        Returns:
            str: Строка с идентификатором задачи и статусом отчёта.
        """
        return f"<Отчёт ид_задачи={self.task_id!r} статус={self.status!r}>"
