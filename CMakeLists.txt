cmake_minimum_required(VERSION 3.20)
project(sql_executor)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
cmake_policy(SET CMP0135 NEW)

# -----------------------------
# Python
# -----------------------------
find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)

# -----------------------------
# PostgreSQL
# -----------------------------
find_package(PostgreSQL REQUIRED)

# -----------------------------
# pybind11
# -----------------------------
include(FetchContent)
FetchContent_Declare(
        pybind11
        URL https://github.com/pybind/pybind11/archive/refs/tags/v2.10.4.tar.gz
)
FetchContent_MakeAvailable(pybind11)

# -----------------------------
# clickhouse-cpp
# -----------------------------
FetchContent_Declare(
        clickhouse-cpp
        GIT_REPOSITORY https://github.com/ClickHouse/clickhouse-cpp
        GIT_TAG v2.4.0
)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build shared libraries" FORCE)
FetchContent_MakeAvailable(clickhouse-cpp)

# -----------------------------
# Catch2 (CMake target)
# -----------------------------
include(FetchContent)
FetchContent_Declare(
        catch2
        GIT_REPOSITORY https://github.com/catchorg/Catch2.git
        GIT_TAG v3.4.0
)
FetchContent_MakeAvailable(catch2)

# -----------------------------
# Общая библиотека (ядро)
# -----------------------------
add_library(sql_executor_core
        src/clickhouse_connector.cpp
        src/postgres_connector.cpp
)

target_include_directories(sql_executor_core
        PUBLIC
        include
        ${PostgreSQL_INCLUDE_DIRS}
        ${clickhouse-cpp_SOURCE_DIR}
)

target_link_libraries(sql_executor_core
        PUBLIC
        ${PostgreSQL_LIBRARIES}
        clickhouse-cpp-lib
)

# -----------------------------
# Python-модуль (обёртка)
# -----------------------------
pybind11_add_module(sql_executor src/python_bindings.cpp)
target_include_directories(sql_executor PRIVATE include)
target_link_libraries(sql_executor PRIVATE sql_executor_core)
set_target_properties(sql_executor PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}
)

# -----------------------------
# Тесты
# -----------------------------
enable_testing()

add_executable(sql_executor_tests
        tests/test_common.cpp
        tests/test_clickhouse_connector.cpp
        tests/test_postgres_connector.cpp
)

target_include_directories(sql_executor_tests PRIVATE
        include
        ${PostgreSQL_INCLUDE_DIRS}
        ${clickhouse-cpp_SOURCE_DIR}
)

target_link_libraries(sql_executor_tests
        PRIVATE
        sql_executor_core
        ${PostgreSQL_LIBRARIES}
        clickhouse-cpp-lib
        Catch2::Catch2WithMain
)

include(CTest)
include(Catch)
catch_discover_tests(sql_executor_tests)
