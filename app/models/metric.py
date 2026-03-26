from datetime import datetime

from sqlalchemy import DateTime, Float, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class Metric(Base):
    """ORM-модель записи системной метрики, загруженной из внешнего API метрик."""

    __tablename__ = "metrics"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    sensor_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    cpu_load: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    memory_usage: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    external_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    def __repr__(self) -> str:
        """Возвращает строковое представление объекта Metric.

        Returns:
            str: Строка с идентификатором, ID сенсора и значением нагрузки CPU.
        """
        return f"<Метрика ид={self.id} ид_сенсора={self.sensor_id!r} нагрузка_цпу={self.cpu_load}>"
