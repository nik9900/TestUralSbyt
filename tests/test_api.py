from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from app.schemas.analytics import AnalyticsSummarySchema, SystemLoadSchema
from tests.conftest import make_report


class TestHealth:
    async def test_health_returns_ok(self, client):
        response = await client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


class TestAnalyticsEndpoint:
    def _mock_summary(self):
        return AnalyticsSummarySchema(
            period="last_24h",
            total_tasks_collected=10,
            total_tasks_completed=7,
            average_hours_per_task=3.5,
            system_load=SystemLoadSchema(avg_cpu=42.0, max_memory=0.8),
            generated_at=datetime.now(tz=timezone.utc),
        )

    async def test_summary_returns_200(self, client):
        with patch(
            "app.services.analytics_service.AnalyticsService.get_summary",
            new=AsyncMock(return_value=self._mock_summary()),
        ):
            response = await client.get("/analytics/summary")

        assert response.status_code == 200

    async def test_summary_response_shape(self, client):
        with patch(
            "app.services.analytics_service.AnalyticsService.get_summary",
            new=AsyncMock(return_value=self._mock_summary()),
        ):
            response = await client.get("/analytics/summary")

        data = response.json()
        assert data["period"] == "last_24h"
        assert data["total_tasks_collected"] == 10
        assert data["total_tasks_completed"] == 7
        assert data["average_hours_per_task"] == 3.5
        assert data["system_load"]["avg_cpu"] == 42.0
        assert data["system_load"]["max_memory"] == 0.8
        assert "generated_at" in data


class TestReportGenerateEndpoint:
    async def test_generate_returns_202(self, client):
        with (
            patch(
                "app.services.report_service.ReportService.schedule_report",
                new=AsyncMock(return_value="task-uuid-001"),
            ),
            patch("app.tasks.worker.generate_report") as mock_celery,
        ):
            mock_celery.delay = MagicMock()
            response = await client.post(
                "/reports/generate",
                json={
                    "period_from": "2024-01-01T00:00:00Z",
                    "period_to": "2024-01-31T23:59:59Z",
                },
            )

        assert response.status_code == 202

    async def test_generate_returns_task_id(self, client):
        with (
            patch(
                "app.services.report_service.ReportService.schedule_report",
                new=AsyncMock(return_value="task-uuid-001"),
            ),
            patch("app.tasks.worker.generate_report") as mock_celery,
        ):
            mock_celery.delay = MagicMock()
            response = await client.post(
                "/reports/generate",
                json={
                    "period_from": "2024-01-01T00:00:00Z",
                    "period_to": "2024-01-31T23:59:59Z",
                },
            )

        data = response.json()
        assert data["task_id"] == "task-uuid-001"
        assert data["status"] == "pending"
        assert "message" in data

    async def test_generate_invalid_body_returns_422(self, client):
        response = await client.post("/reports/generate", json={"bad_field": "value"})
        assert response.status_code == 422

    async def test_generate_missing_period_to_returns_422(self, client):
        response = await client.post(
            "/reports/generate",
            json={"period_from": "2024-01-01T00:00:00Z"},
        )
        assert response.status_code == 422


class TestReportStatusEndpoint:
    async def test_status_pending(self, client):
        now = datetime.now(tz=timezone.utc)
        report = make_report(task_id="abc-001", status="pending", period_from=now, period_to=now)
        report.created_at = now

        with patch(
            "app.services.report_service.ReportService.get_status",
            new=AsyncMock(return_value=report),
        ):
            response = await client.get("/reports/abc-001")

        assert response.status_code == 200
        data = response.json()
        assert data["task_id"] == "abc-001"
        assert data["status"] == "pending"
        assert data["s3_path"] is None

    async def test_status_done_with_s3_path(self, client):
        now = datetime.now(tz=timezone.utc)
        report = make_report(task_id="abc-002", status="done", period_from=now, period_to=now)
        report.s3_path = "s3://reports/abc-002.csv"
        report.created_at = now

        with patch(
            "app.services.report_service.ReportService.get_status",
            new=AsyncMock(return_value=report),
        ):
            response = await client.get("/reports/abc-002")

        data = response.json()
        assert data["status"] == "done"
        assert data["s3_path"] == "s3://reports/abc-002.csv"

    async def test_status_not_found_returns_404(self, client):
        with patch(
            "app.services.report_service.ReportService.get_status",
            new=AsyncMock(return_value=None),
        ):
            response = await client.get("/reports/nonexistent-id")

        assert response.status_code == 404


class TestIngestionTriggerEndpoint:
    async def test_trigger_returns_202(self, client):
        mock_task = MagicMock()
        mock_task.id = "celery-task-id-123"

        with patch("app.tasks.worker.run_ingestion") as mock_celery:
            mock_celery.delay.return_value = mock_task
            response = await client.post("/ingestion/trigger")

        assert response.status_code == 202

    async def test_trigger_returns_task_id(self, client):
        mock_task = MagicMock()
        mock_task.id = "celery-task-id-456"

        with patch("app.tasks.worker.run_ingestion") as mock_celery:
            mock_celery.delay.return_value = mock_task
            response = await client.post("/ingestion/trigger")

        data = response.json()
        assert data["task_id"] == "celery-task-id-456"
        assert data["status"] == "queued"
