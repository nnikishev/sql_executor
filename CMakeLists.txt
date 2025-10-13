cmake_minimum_required(VERSION 3.10)
project(sql_executor)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)  # Важно: включаем PIC для всех целей

find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)

include(FetchContent)
FetchContent_Declare(
    pybind11
    URL https://github.com/pybind/pybind11/archive/refs/tags/v2.10.4.tar.gz
)
FetchContent_MakeAvailable(pybind11)

find_package(PostgreSQL REQUIRED)

include(FetchContent)
FetchContent_Declare(
    clickhouse-cpp
    GIT_REPOSITORY https://github.com/ClickHouse/clickhouse-cpp
    GIT_TAG v2.4.0
)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build shared libraries" FORCE)
set(CMAKE_POSITION_INDEPENDENT_CODE ON CACHE BOOL "Position independent code" FORCE)
FetchContent_MakeAvailable(clickhouse-cpp)

pybind11_add_module(sql_executor
    src/sql_executor.cpp
    src/postgres_connector.cpp
    src/clickhouse_connector.cpp
)

target_include_directories(sql_executor 
    PRIVATE 
    ${PostgreSQL_INCLUDE_DIRS}
    src/
    ${clickhouse-cpp_SOURCE_DIR}
)

target_link_libraries(sql_executor 
    PRIVATE
    ${PostgreSQL_LIBRARIES}
    clickhouse-cpp-lib
)

set_target_properties(sql_executor PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}
)