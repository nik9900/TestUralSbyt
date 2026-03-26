import boto3
from botocore.exceptions import ClientError

from app.core.config import get_settings


class S3Client:
    """Инкапсулирует взаимодействие с S3/MinIO хранилищем."""

    def __init__(self) -> None:
        """Инициализация клиента S3 с параметрами из настроек приложения.

        Создаёт boto3-клиент и при необходимости создаёт бакет хранилища.
        """
        settings = get_settings()
        self._bucket = settings.s3_bucket_name
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
        )
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        """Проверяет существование бакета и создаёт его при отсутствии."""
        try:
            self._client.head_bucket(Bucket=self._bucket)
        except ClientError:
            self._client.create_bucket(Bucket=self._bucket)

    def upload_file(self, key: str, data: bytes, content_type: str = "text/csv") -> str:
        """Загружает файл в S3 хранилище по указанному ключу.

        Args:
            key: Путь (ключ) объекта в бакете.
            data: Байтовое содержимое загружаемого файла.
            content_type: MIME-тип загружаемого файла.

        Returns:
            str: Путь к объекту в формате s3://bucket/key.
        """
        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
        return f"s3://{self._bucket}/{key}"

    def get_object(self, key: str) -> bytes:
        """Скачивает объект из S3 и возвращает его содержимое.

        Args:
            key: Путь (ключ) объекта в бакете.

        Returns:
            bytes: Содержимое объекта.
        """
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        return bytes(response["Body"].read())

    def generate_presigned_url(self, key: str, expires_in: int = 3600) -> str:
        """Генерирует предподписанный URL для временного доступа к объекту.

        Args:
            key: Путь (ключ) объекта в бакете.
            expires_in: Время жизни URL в секундах (по умолчанию 3600).

        Returns:
            str: Предподписанный URL для доступа к объекту.
        """
        return str(
            self._client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self._bucket, "Key": key},
                ExpiresIn=expires_in,
            )
        )
