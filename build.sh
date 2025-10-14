#!/bin/bash
set -e

# -----------------------------
# Настройка портов и контейнеров
# -----------------------------
PG_PORT=15432
CH_TCP_PORT=19000
CH_HTTP_PORT=18123

PG_CONTAINER=test_postgres
CH_CONTAINER=test_clickhouse

# -----------------------------
# Установка зависимостей
# -----------------------------
sudo apt-get update
sudo apt-get install -y \
    python3-dev \
    python3-venv \
    libpq-dev \
    postgresql-server-dev-all \
    cmake \
    g++ \
    git \
    docker.io \
    docker-compose \
    wget

# -----------------------------
# Python-окружение
# -----------------------------
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install pybind11

# -----------------------------
# Очистка и сборка проекта
# -----------------------------
echo "=== Cleaning build ==="
rm -rf build
mkdir build
cd build

echo "=== Configuring CMake ==="
cmake .. || { echo "CMake configuration failed"; exit 1; }

echo "=== Building ==="
make -j$(nproc) || { echo "Build failed"; exit 1; }

cd ..

echo "=== Checking Python module ==="
if python3 -c "import sql_executor"; then
    echo "✓ Python import successful"
else
    echo "✗ Python import failed"
    exit 1
fi

# -----------------------------
# Запуск контейнеров
# -----------------------------
echo "=== Starting PostgreSQL container ==="
sudo docker run -d --rm --name $PG_CONTAINER -e POSTGRES_PASSWORD=postgres -p $PG_PORT:5432 postgres:15

echo "=== Starting ClickHouse container ==="
sudo docker run -d --rm --name $CH_CONTAINER -p $CH_TCP_PORT:9000 -p $CH_HTTP_PORT:8123 clickhouse/clickhouse-server:23.8

# -----------------------------
# Ожидание готовности СУБД
# -----------------------------
echo "=== Waiting for PostgreSQL to be ready ==="
until sudo docker exec $PG_CONTAINER pg_isready -U postgres >/dev/null 2>&1; do
    sleep 1
done

echo "=== Waiting for ClickHouse to be ready ==="
until curl -s "http://localhost:$CH_HTTP_PORT/ping" >/dev/null 2>&1; do
    sleep 1
done

# -----------------------------
# Запуск тестов
# -----------------------------
echo "=== Running tests ==="
cd build
ctest --output-on-failure

# -----------------------------
# Завершение и очистка
# -----------------------------
echo "=== Stopping containers ==="
sudo docker stop $PG_CONTAINER $CH_CONTAINER || true

echo "✓ Build and tests completed successfully"
