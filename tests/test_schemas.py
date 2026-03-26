from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app.schemas.analytics import AnalyticsSummarySchema, SystemLoadSchema
from app.schemas.ingestion import RawMetricData, RawTaskData
from app.schemas.reports import ReportGenerateRequest, ReportGenerateResponse, ReportStatusResponse


class TestSystemLoadSchema:
    def test_valid(self):
        s = SystemLoadSchema(avg_cpu=42.5, max_memory=0.8)
        assert s.avg_cpu == 42.5
        assert s.max_memory == 0.8

    def test_zero_values(self):
        s = SystemLoadSchema(avg_cpu=0.0, max_memory=0.0)
        assert s.avg_cpu == 0.0


class TestAnalyticsSummarySchema:
    def test_valid(self):
        now = datetime.now(tz=timezone.utc)
        s = AnalyticsSummarySchema(
            total_tasks_collected=10,
            total_tasks_completed=5,
            average_hours_per_task=3.0,
            system_load=SystemLoadSchema(avg_cpu=30.0, max_memory=0.6),
            generated_at=now,
        )
        assert s.period == "last_24h"
        assert s.total_tasks_collected == 10

    def test_default_period(self):
        now = datetime.now(tz=timezone.utc)
        s = AnalyticsSummarySchema(
            total_tasks_collected=0,
            total_tasks_completed=0,
            average_hours_per_task=0.0,
            system_load=SystemLoadSchema(avg_cpu=0.0, max_memory=0.0),
            generated_at=now,
        )
        assert s.period == "last_24h"

    def test_missing_required_field(self):
        with pytest.raises(ValidationError):
            AnalyticsSummarySchema(
                total_tasks_collected=5,
            )


class TestReportGenerateRequest:
    def test_valid(self):
        r = ReportGenerateRequest(
            period_from=datetime(2024, 1, 1, tzinfo=timezone.utc),
            period_to=datetime(2024, 1, 31, tzinfo=timezone.utc),
        )
        assert r.period_from.year == 2024

    def test_missing_period_from(self):
        with pytest.raises(ValidationError):
            ReportGenerateRequest(period_to=datetime(2024, 1, 31, tzinfo=timezone.utc))

    def test_missing_period_to(self):
        with pytest.raises(ValidationError):
            ReportGenerateRequest(period_from=datetime(2024, 1, 1, tzinfo=timezone.utc))

    def test_from_iso_string(self):
        r = ReportGenerateRequest(
            period_from="2024-06-01T00:00:00Z",
            period_to="2024-06-30T23:59:59Z",
        )
        assert r.period_from.month == 6


class TestReportGenerateResponse:
    def test_valid(self):
        r = ReportGenerateResponse(task_id="uuid-123", status="pending", message="Queued")
        assert r.task_id == "uuid-123"
        assert r.status == "pending"

    def test_missing_task_id(self):
        with pytest.raises(ValidationError):
            ReportGenerateResponse(status="pending", message="Queued")


class TestReportStatusResponse:
    def test_done_with_s3_path(self):
        r = ReportStatusResponse(
            task_id="t-001",
            status="done",
            s3_path="s3://reports/t-001.csv",
            created_at=datetime.now(tz=timezone.utc),
        )
        assert r.s3_path == "s3://reports/t-001.csv"

    def test_pending_no_s3_path(self):
        r = ReportStatusResponse(
            task_id="t-002",
            status="pending",
            created_at=datetime.now(tz=timezone.utc),
        )
        assert r.s3_path is None


class TestRawTaskData:
    def test_valid_minimal(self):
        task_data = RawTaskData(id=1, title="Do something")
        assert task_data.id == 1
        assert task_data.status == "unknown"
        assert task_data.hours_spent == 0.0

    def test_valid_full(self):
        task_data = RawTaskData(
            id=5,
            title="Deploy",
            status="completed",
            hours_spent=3.5,
            created_at=datetime.now(tz=timezone.utc),
        )
        assert task_data.status == "completed"
        assert task_data.hours_spent == 3.5

    def test_missing_id_raises(self):
        with pytest.raises(ValidationError):
            RawTaskData(title="No id")

    def test_missing_title_raises(self):
        with pytest.raises(ValidationError):
            RawTaskData(id=1)


class TestRawMetricData:
    def test_valid_minimal(self):
        metric_data = RawMetricData(sensor_id="srv-01")
        assert metric_data.cpu_load == 0.0
        assert metric_data.memory_usage == 0.0

    def test_valid_full(self):
        metric_data = RawMetricData(
            sensor_id="srv-02",
            cpu_load=75.5,
            memory_usage=0.9,
            timestamp=datetime.now(tz=timezone.utc),
        )
        assert metric_data.cpu_load == 75.5
        assert metric_data.memory_usage == 0.9

    def test_missing_sensor_id_raises(self):
        with pytest.raises(ValidationError):
            RawMetricData(cpu_load=50.0)
