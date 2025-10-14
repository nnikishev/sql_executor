#include <pybind11/pybind11.h>
#include "clickhouse_connector.h"
#include "postgres_connector.h"

namespace py = pybind11;

PYBIND11_MODULE(sql_executor, m) {
    m.doc() = "Python bindings for SQL Executor";

    py::class_<PostgresConnector>(m, "PostgresConnector")
        .def(py::init<>())
        .def("connect", &PostgresConnector::connect)
        .def("disconnect", &PostgresConnector::disconnect)
        .def("is_connected", &PostgresConnector::is_connected)
        .def("execute", &PostgresConnector::execute);

    py::class_<ClickHouseConnector>(m, "ClickHouseConnector")
        .def(py::init<>())
        .def("connect", &ClickHouseConnector::connect)
        .def("disconnect", &ClickHouseConnector::disconnect)
        .def("is_connected", &ClickHouseConnector::is_connected)
        .def("execute", &ClickHouseConnector::execute);
}
