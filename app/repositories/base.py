from abc import ABC, abstractmethod
from datetime import datetime
from typing import ClassVar, Generic, Sequence, TypeVar

from sqlalchemy.ext.asyncio import AsyncSession

ModelT = TypeVar("ModelT")


class AbstractRepository(ABC, Generic[ModelT]):
    """Обобщённый CRUD-контракт для всех репозиториев приложения."""

    @abstractmethod
    async def add(self, entity: ModelT) -> ModelT: ...

    @abstractmethod
    async def get_by_id(self, entity_id: int) -> ModelT | None: ...

    @abstractmethod
    async def list_since(self, since: datetime) -> Sequence[ModelT]: ...


class BaseRepository(AbstractRepository[ModelT]):
    """Базовая реализация с общими методами — init, add, get_by_id, bulk_add.

    Подклассы указывают атрибут класса _model и реализуют list_since.
    """

    _model: ClassVar[type]

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, entity: ModelT) -> ModelT:
        self._session.add(entity)
        await self._session.flush()
        await self._session.refresh(entity)
        return entity

    async def get_by_id(self, entity_id: int) -> ModelT | None:
        return await self._session.get(self._model, entity_id)

    async def bulk_add(self, entities: list[ModelT]) -> None:
        self._session.add_all(entities)
        await self._session.flush()
