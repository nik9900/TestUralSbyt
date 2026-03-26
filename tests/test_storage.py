from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from app.core.storage import S3Client


def _make_client(head_raises=False):
    """Build an S3Client with a fully mocked boto3 client."""
    mock_boto = MagicMock()

    if head_raises:
        mock_boto.head_bucket.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadBucket"
        )

    with patch("app.core.storage.boto3.client", return_value=mock_boto):
        with patch("app.core.storage.get_settings") as mock_settings:
            mock_settings.return_value = MagicMock(
                s3_bucket_name="test-bucket",
                s3_endpoint_url="http://localhost:9000",
                s3_access_key="key",
                s3_secret_key="secret",
            )
            client = S3Client()

    client._client = mock_boto
    return client, mock_boto


class TestS3Client:
    def test_ensure_bucket_creates_if_not_exists(self):
        mock_boto = MagicMock()
        mock_boto.head_bucket.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadBucket"
        )
        with patch("app.core.storage.boto3.client", return_value=mock_boto):
            with patch("app.core.storage.get_settings") as mock_settings:
                mock_settings.return_value = MagicMock(
                    s3_bucket_name="new-bucket",
                    s3_endpoint_url="http://localhost:9000",
                    s3_access_key="key",
                    s3_secret_key="secret",
                )
                S3Client()

        mock_boto.create_bucket.assert_called_once_with(Bucket="new-bucket")

    def test_ensure_bucket_skips_create_if_exists(self):
        mock_boto = MagicMock()
        mock_boto.head_bucket.return_value = {}  # bucket exists
        with patch("app.core.storage.boto3.client", return_value=mock_boto):
            with patch("app.core.storage.get_settings") as mock_settings:
                mock_settings.return_value = MagicMock(
                    s3_bucket_name="existing-bucket",
                    s3_endpoint_url="http://localhost:9000",
                    s3_access_key="key",
                    s3_secret_key="secret",
                )
                S3Client()

        mock_boto.create_bucket.assert_not_called()

    def test_upload_file_calls_put_object(self):
        client, mock_boto = _make_client()
        result = client.upload_file("reports/test.csv", b"col1,col2\n1,2")

        mock_boto.put_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="reports/test.csv",
            Body=b"col1,col2\n1,2",
            ContentType="text/csv",
        )
        assert result == "s3://test-bucket/reports/test.csv"

    def test_upload_file_returns_s3_path(self):
        client, _ = _make_client()
        path = client.upload_file("some/key.csv", b"data")
        assert path.startswith("s3://")
        assert "some/key.csv" in path

    def test_generate_presigned_url(self):
        client, mock_boto = _make_client()
        mock_boto.generate_presigned_url.return_value = "https://minio/presigned"

        url = client.generate_presigned_url("reports/test.csv", expires_in=300)

        mock_boto.generate_presigned_url.assert_called_once_with(
            "get_object",
            Params={"Bucket": "test-bucket", "Key": "reports/test.csv"},
            ExpiresIn=300,
        )
        assert url == "https://minio/presigned"

    def test_generate_presigned_url_default_expiry(self):
        client, mock_boto = _make_client()
        mock_boto.generate_presigned_url.return_value = "https://minio/presigned"

        client.generate_presigned_url("key.csv")

        call_kwargs = mock_boto.generate_presigned_url.call_args[1]
        assert call_kwargs["ExpiresIn"] == 3600
