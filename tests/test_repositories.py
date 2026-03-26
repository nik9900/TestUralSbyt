from datetime import datetime, timedelta, timezone

from app.repositories.metric_repository import MetricRepository
from app.repositories.report_repository import ReportRepository
from app.repositories.task_repository import TaskRepository
from tests.conftest import make_metric, make_report, make_task


class TestTaskRepository:
    async def test_add_and_get_by_id(self, session):
        repo = TaskRepository(session)
        task = make_task(external_id=42, title="Buy milk")

        saved = await repo.add(task)
        await session.commit()

        fetched = await repo.get_by_id(saved.id)
        assert fetched is not None
        assert fetched.external_id == 42
        assert fetched.title == "Buy milk"

    async def test_get_by_id_not_found(self, session):
        repo = TaskRepository(session)
        result = await repo.get_by_id(9999)
        assert result is None

    async def test_list_since_returns_recent(self, session):
        repo = TaskRepository(session)
        now = datetime.now(tz=timezone.utc)
        old = make_task(external_id=1, collected_at=now - timedelta(hours=25))
        recent = make_task(external_id=2, collected_at=now - timedelta(hours=1))

        await repo.add(old)
        await repo.add(recent)
        await session.commit()

        results = await repo.list_since(now - timedelta(hours=24))
        ids = [task_record.external_id for task_record in results]
        assert 2 in ids
        assert 1 not in ids

    async def test_list_since_empty(self, session):
        repo = TaskRepository(session)
        results = await repo.list_since(datetime.now(tz=timezone.utc))
        assert list(results) == []

    async def test_bulk_add(self, session):
        repo = TaskRepository(session)
        tasks = [make_task(external_id=i) for i in range(5)]
        await repo.bulk_add(tasks)
        await session.commit()

        results = await repo.list_since(datetime.now(tz=timezone.utc) - timedelta(minutes=1))
        assert len(results) == 5

    async def test_list_between(self, session):
        repo = TaskRepository(session)
        now = datetime.now(tz=timezone.utc)
        inside = make_task(external_id=10, collected_at=now - timedelta(hours=5))
        outside = make_task(external_id=20, collected_at=now - timedelta(hours=30))

        await repo.bulk_add([inside, outside])
        await session.commit()

        results = await repo.list_between(now - timedelta(hours=10), now)
        ids = [task_record.external_id for task_record in results]
        assert 10 in ids
        assert 20 not in ids


class TestMetricRepository:
    async def test_add_and_get_by_id(self, session):
        repo = MetricRepository(session)
        metric = make_metric(sensor_id="srv-99", cpu_load=77.5)

        saved = await repo.add(metric)
        await session.commit()

        fetched = await repo.get_by_id(saved.id)
        assert fetched is not None
        assert fetched.sensor_id == "srv-99"
        assert fetched.cpu_load == 77.5

    async def test_list_since_filters_correctly(self, session):
        repo = MetricRepository(session)
        now = datetime.now(tz=timezone.utc)
        old = make_metric(sensor_id="old", collected_at=now - timedelta(hours=48))
        fresh = make_metric(sensor_id="new", collected_at=now - timedelta(minutes=30))

        await repo.bulk_add([old, fresh])
        await session.commit()

        results = await repo.list_since(now - timedelta(hours=24))
        sensor_ids = [metric_record.sensor_id for metric_record in results]
        assert "new" in sensor_ids
        assert "old" not in sensor_ids

    async def test_bulk_add_multiple(self, session):
        repo = MetricRepository(session)
        metrics = [make_metric(sensor_id=f"srv-{i:02d}", cpu_load=float(i * 10)) for i in range(3)]
        await repo.bulk_add(metrics)
        await session.commit()

        results = await repo.list_since(datetime.now(tz=timezone.utc) - timedelta(minutes=1))
        assert len(results) == 3

    async def test_list_between_range(self, session):
        repo = MetricRepository(session)
        now = datetime.now(tz=timezone.utc)
        metric_in = make_metric(sensor_id="in", collected_at=now - timedelta(hours=2))
        metric_out = make_metric(sensor_id="out", collected_at=now - timedelta(hours=50))

        await repo.bulk_add([metric_in, metric_out])
        await session.commit()

        results = await repo.list_between(now - timedelta(hours=5), now)
        assert len(results) == 1
        assert results[0].sensor_id == "in"


class TestReportRepository:
    async def test_add_and_get_by_task_id(self, session):
        repo = ReportRepository(session)
        report = make_report(task_id="abc-123")

        await repo.add(report)
        await session.commit()

        fetched = await repo.get_by_task_id("abc-123")
        assert fetched is not None
        assert fetched.task_id == "abc-123"
        assert fetched.status == "pending"

    async def test_get_by_task_id_not_found(self, session):
        repo = ReportRepository(session)
        result = await repo.get_by_task_id("nonexistent")
        assert result is None

    async def test_update_status(self, session):
        repo = ReportRepository(session)
        report = make_report(task_id="upd-001")
        await repo.add(report)
        await session.commit()

        await repo.update_status("upd-001", "processing")
        await session.commit()

        updated = await repo.get_by_task_id("upd-001")
        assert updated.status == "processing"

    async def test_update_status_with_s3_path(self, session):
        repo = ReportRepository(session)
        report = make_report(task_id="upd-002")
        await repo.add(report)
        await session.commit()

        await repo.update_status("upd-002", "done", s3_path="s3://reports/upd-002.csv")
        await session.commit()

        updated = await repo.get_by_task_id("upd-002")
        assert updated.status == "done"
        assert updated.s3_path == "s3://reports/upd-002.csv"

    async def test_update_status_nonexistent_task(self, session):
        repo = ReportRepository(session)
        await repo.update_status("ghost-id", "done")

    async def test_list_since(self, session):
        repo = ReportRepository(session)
        now = datetime.now(tz=timezone.utc)
        old = make_report(task_id="old-1")
        old.created_at = now - timedelta(hours=48)
        recent = make_report(task_id="new-1")
        recent.created_at = now - timedelta(minutes=5)

        await repo.add(old)
        await repo.add(recent)
        await session.commit()

        results = await repo.list_since(now - timedelta(hours=24))
        task_ids = [report_record.task_id for report_record in results]
        assert "new-1" in task_ids
        assert "old-1" not in task_ids
