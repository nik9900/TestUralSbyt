from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.config import Settings
from app.schemas.analytics import AnalyticsSummarySchema
from app.services.analytics_service import AnalyticsService
from app.services.ingestion_service import IngestionService
from app.services.report_service import ReportService
from tests.conftest import make_metric, make_report, make_task


class TestAnalyticsService:
    def _make_service(self, tasks=None, metrics=None):
        task_repo = AsyncMock()
        metric_repo = AsyncMock()
        task_repo.list_since.return_value = tasks or []
        metric_repo.list_since.return_value = metrics or []
        return AnalyticsService(task_repo=task_repo, metric_repo=metric_repo)

    async def test_summary_empty_data(self):
        svc = self._make_service()
        result = await svc.get_summary()

        assert isinstance(result, AnalyticsSummarySchema)
        assert result.total_tasks_collected == 0
        assert result.total_tasks_completed == 0
        assert result.average_hours_per_task == 0.0
        assert result.system_load.avg_cpu == 0.0
        assert result.system_load.max_memory == 0.0

    async def test_summary_counts_tasks_correctly(self):
        tasks = [
            make_task(status="completed", hours_spent=4.0),
            make_task(status="completed", hours_spent=2.0),
            make_task(status="pending", hours_spent=1.0),
        ]
        svc = self._make_service(tasks=tasks)
        result = await svc.get_summary()

        assert result.total_tasks_collected == 3
        assert result.total_tasks_completed == 2
        assert result.average_hours_per_task == pytest.approx(7.0 / 3, rel=1e-2)

    async def test_summary_system_load_aggregation(self):
        metrics = [
            make_metric(cpu_load=30.0, memory_usage=0.5),
            make_metric(cpu_load=50.0, memory_usage=0.9),
            make_metric(cpu_load=40.0, memory_usage=0.7),
        ]
        svc = self._make_service(metrics=metrics)
        result = await svc.get_summary()

        assert result.system_load.avg_cpu == pytest.approx(40.0, rel=1e-2)
        assert result.system_load.max_memory == pytest.approx(0.9, rel=1e-2)

    async def test_summary_period_label(self):
        svc = self._make_service()
        result = await svc.get_summary()
        assert result.period == "last_24h"

    async def test_summary_generated_at_is_recent(self):
        svc = self._make_service()
        before = datetime.now(tz=timezone.utc)
        result = await svc.get_summary()
        after = datetime.now(tz=timezone.utc)
        assert before <= result.generated_at <= after

    async def test_all_tasks_completed(self):
        tasks = [make_task(status="completed", hours_spent=8.0) for _ in range(4)]
        svc = self._make_service(tasks=tasks)
        result = await svc.get_summary()

        assert result.total_tasks_completed == 4
        assert result.average_hours_per_task == pytest.approx(8.0, rel=1e-2)


class TestReportService:
    def _make_service(self, tasks=None, metrics=None, report=None):
        task_repo = AsyncMock()
        metric_repo = AsyncMock()
        report_repo = AsyncMock()
        s3_client = MagicMock()

        task_repo.list_between.return_value = tasks or []
        metric_repo.list_between.return_value = metrics or []
        report_repo.get_by_task_id.return_value = report
        report_repo.add = AsyncMock(return_value=report)
        report_repo.update_status = AsyncMock()
        s3_client.upload_file.return_value = "s3://reports/test.csv"

        return ReportService(
            task_repo=task_repo,
            metric_repo=metric_repo,
            report_repo=report_repo,
            s3_client=s3_client,
        )

    async def test_schedule_report_returns_uuid(self):
        report_repo = AsyncMock()
        report_repo.add = AsyncMock()
        svc = ReportService(
            task_repo=AsyncMock(),
            metric_repo=AsyncMock(),
            report_repo=report_repo,
            s3_client=MagicMock(),
        )
        now = datetime.now(tz=timezone.utc)
        task_id = await svc.schedule_report(now, now)

        assert isinstance(task_id, str)
        assert len(task_id) == 36
        report_repo.add.assert_called_once()

    async def test_generate_and_upload_success(self):
        now = datetime.now(tz=timezone.utc)
        report = make_report(task_id="rep-001", period_from=now - timedelta(hours=1), period_to=now)
        tasks = [make_task(external_id=1, status="done", hours_spent=3.0)]
        metrics = [make_metric(sensor_id="srv-01", cpu_load=55.0)]

        svc = self._make_service(tasks=tasks, metrics=metrics, report=report)

        result = await svc.generate_and_upload("rep-001")

        assert result == "s3://reports/test.csv"
        svc._report_repo.update_status.assert_any_call("rep-001", "processing")
        svc._report_repo.update_status.assert_any_call("rep-001", "done", "s3://reports/test.csv")

    async def test_generate_and_upload_not_found(self):
        svc = self._make_service(report=None)
        svc._report_repo.get_by_task_id.return_value = None

        with pytest.raises(ValueError, match="not found"):
            await svc.generate_and_upload("ghost-id")

    async def test_generate_and_upload_marks_failed_on_error(self):
        now = datetime.now(tz=timezone.utc)
        report = make_report(task_id="fail-001", period_from=now, period_to=now)
        svc = self._make_service(report=report)
        svc._s3.upload_file.side_effect = RuntimeError("S3 unavailable")

        with pytest.raises(RuntimeError):
            await svc.generate_and_upload("fail-001")

        svc._report_repo.update_status.assert_any_call("fail-001", "failed")

    async def test_get_status_delegates_to_repo(self):
        report = make_report(task_id="st-001", status="done")
        svc = self._make_service(report=report)
        svc._report_repo.get_by_task_id.return_value = report

        result = await svc.get_status("st-001")

        assert result is report
        svc._report_repo.get_by_task_id.assert_called_once_with("st-001")

    async def test_csv_contains_task_and_metric_sections(self):
        now = datetime.now(tz=timezone.utc)
        report = make_report(task_id="csv-001", period_from=now - timedelta(hours=1), period_to=now)
        tasks = [make_task(title="Alpha task")]
        metrics = [make_metric(sensor_id="srv-X")]

        svc = self._make_service(tasks=tasks, metrics=metrics, report=report)
        csv_bytes = await svc._build_csv(now - timedelta(hours=1), now)
        csv_text = csv_bytes.decode("utf-8")

        assert "TASKS" in csv_text
        assert "METRICS" in csv_text
        assert "Alpha task" in csv_text
        assert "srv-X" in csv_text

    async def test_csv_empty_data_shows_no_records_message(self):
        now = datetime.now(tz=timezone.utc)
        report = make_report(task_id="csv-002", period_from=now, period_to=now)
        svc = self._make_service(report=report)

        csv_bytes = await svc._build_csv(now, now)
        csv_text = csv_bytes.decode("utf-8")

        assert "No task records" in csv_text
        assert "No metric records" in csv_text


class TestIngestionService:
    def _make_settings(self) -> Settings:
        return Settings(
            database_url="postgresql+asyncpg://x:x@localhost/x",
            celery_broker_url="amqp://x:x@localhost//",
            celery_result_backend="rpc://",
            s3_endpoint_url="http://localhost:9000",
            s3_access_key="key",
            s3_secret_key="secret",
            s3_bucket_name="bucket",
            ingestion_interval_seconds=600,
            tasks_api_url="https://example.com/todos",
            metrics_api_url="https://example.com/posts",
        )

    def _make_service(self):
        task_repo = AsyncMock()
        metric_repo = AsyncMock()
        task_repo.bulk_add = AsyncMock()
        metric_repo.bulk_add = AsyncMock()
        settings = self._make_settings()
        return IngestionService(task_repo=task_repo, metric_repo=metric_repo, settings=settings)

    async def test_ingest_all_calls_both_sources(self):
        svc = self._make_service()
        mock_response = [{"id": i, "title": f"Task {i}", "completed": False} for i in range(1, 5)]

        with patch.object(svc, "_fetch_json", new=AsyncMock(return_value=mock_response)):
            counts = await svc.ingest_all()

        assert "tasks" in counts
        assert "metrics" in counts
        assert counts["tasks"] > 0
        assert counts["metrics"] > 0

    async def test_ingest_tasks_persists_records(self):
        svc = self._make_service()
        mock_todos = [
            {"id": 1, "title": "Write tests", "completed": True},
            {"id": 2, "title": "Deploy app", "completed": False},
        ]

        with patch.object(svc, "_fetch_json", new=AsyncMock(return_value=mock_todos)):
            count = await svc._ingest_tasks()

        assert count == 2
        svc._task_repo.bulk_add.assert_called_once()
        saved_tasks = svc._task_repo.bulk_add.call_args[0][0]
        statuses = {task_record.status for task_record in saved_tasks}
        assert "completed" in statuses
        assert "pending" in statuses

    async def test_ingest_tasks_skips_malformed_records(self):
        svc = self._make_service()
        mock_todos = [
            {"id": None, "title": "Bad record"},
            {"id": 5, "title": "Good record", "completed": False},
        ]

        with patch.object(svc, "_fetch_json", new=AsyncMock(return_value=mock_todos)):
            count = await svc._ingest_tasks()

        assert count == 1

    async def test_ingest_metrics_uses_mock_data(self):
        svc = self._make_service()
        count = await svc._ingest_metrics()

        assert count == 10
        svc._metric_repo.bulk_add.assert_called_once()

    async def test_fetch_json_raises_on_http_error(self):
        svc = self._make_service()

        import httpx

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_response = MagicMock()
            mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
                "500", request=MagicMock(), response=MagicMock()
            )
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value.get = AsyncMock(return_value=mock_response)
            mock_client_class.return_value = mock_client

            with pytest.raises(httpx.HTTPStatusError):
                await svc._fetch_json("https://example.com/fail")
