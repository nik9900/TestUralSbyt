#!/bin/bash
set -e

echo "Применение миграций Alembic..."
alembic upgrade head
echo "Миграции применены."

exec "$@"
