#!/bin/bash

sudo apt-get update
sudo apt-get install -y \
    python3-dev \
    python3-venv \
    libpq-dev \
    postgresql-server-dev-all \
    cmake \
    g++ \
    git

python3 -m venv venv
source venv/bin/activate

pip install pybind11

echo "=== Cleaning build ==="
rm -rf build
mkdir build
cd build

echo "=== Configuring CMake ==="
cmake .. || { echo "CMake configuration failed"; exit 1; }

echo "=== Building ==="
make -j4 || { echo "Build failed"; exit 1; }

echo "=== Checking output ==="
cd ..
if [ -f sql_executor*.so ]; then
    echo "✓ Build successful! Module created:"
    ls -la sql_executor*.so
else
    echo "✗ Build failed - no module created"
    exit 1
fi

echo "=== Testing Python import ==="
python3 -c "import sql_executor; print('✓ Python import successful')" || echo "✗ Python import failed"