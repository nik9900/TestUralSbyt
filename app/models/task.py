from datetime import datetime

from sqlalchemy import DateTime, Float, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class Task(Base):
    """ORM-модель записи задачи, загруженной из внешнего API задач."""

    __tablename__ = "tasks"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    external_id: Mapped[int] = mapped_column(nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[str] = mapped_column(String(64), nullable=False, default="unknown")
    hours_spent: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    external_created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    def __repr__(self) -> str:
        """Возвращает строковое представление объекта Task.

        Returns:
            str: Строка с идентификатором, внешним ID и статусом задачи.
        """
        return f"<Задача ид={self.id} внешний_ид={self.external_id} статус={self.status!r}>"
