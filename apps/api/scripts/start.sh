#!/usr/bin/env sh

set -e

echo "Starting OneHaven Market API container..."

echo "Running Alembic migrations..."
alembic upgrade head

echo "Starting FastAPI server..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
