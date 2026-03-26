# DataCollector

Сервис сбора метрик и задач, генерации аналитики и CSV-отчётов.

## Запуск

```bash
cp .env.example .env
# заполнить .env 
docker-compose up --build
```

## Адреса после запуска

| Сервис | Адрес |
|--------|-------|
| Дашборд | http://localhost:180/dashboard |
| Swagger | http://localhost:180/docs |
| RabbitMQ UI | http://localhost:16572 |
| MinIO Console | http://localhost:19001 |

## Основные эндпоинты

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/health` | Проверка работоспособности |
| GET | `/analytics/summary` | Статистика за 24 часа |
| POST | `/reports/generate` | Сформировать CSV-отчёт |
| GET | `/reports/{task_id}` | Статус отчёта |
| GET | `/reports/{task_id}/download` | Скачать готовый отчёт |
| POST | `/ingestion/trigger` | Запустить загрузку данных вручную |

## Переменные окружения

Скопируй `.env.example` в `.env` и заполни:

| Переменная | Описание |
|------------|----------|
| `POSTGRES_USER/PASSWORD/DB` | Подключение к PostgreSQL |
| `RABBITMQ_USER/PASSWORD` | Брокер очередей |
| `MINIO_ACCESS_KEY/SECRET_KEY` | Хранилище файлов |
| `S3_BUCKET_NAME` | Имя бакета для отчётов |
| `TASKS_API_URL` | Источник задач |
| `METRICS_API_URL` | Источник метрик |
