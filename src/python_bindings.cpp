#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "clickhouse_connector.h"
#include "postgres_connector.h"

namespace py = pybind11;

py::object json_string_to_python_dict(const std::string& json_str) {
    py::module json_module = py::module::import("json");
    return json_module.attr("loads")(json_str);
}

PYBIND11_MODULE(sql_executor, m) {
    m.doc() = "Python bindings for SQL Executor";

    py::class_<PostgresConnector>(m, "PostgresConnector")
        .def(py::init<>())
        .def("connect", &PostgresConnector::connect, py::arg("conninfo"))
        .def("disconnect", &PostgresConnector::disconnect)
        .def("is_connected", &PostgresConnector::is_connected)
        .def("execute", [](PostgresConnector& self, const std::string& query) {
            std::string json_result = self.execute_to_json(query);
            return json_string_to_python_dict(json_result);
        }, py::arg("query"))
        .def("begin_transaction", &PostgresConnector::begin_transaction)
        .def("get_current_transaction_id", &PostgresConnector::get_current_transaction_id)
        .def("commit_transaction", &PostgresConnector::commit_transaction)
        .def("rollback_transaction", &PostgresConnector::rollback_transaction)
        .def("is_in_transaction", &PostgresConnector::is_in_transaction)
        .def("execute_batch", &PostgresConnector::execute_batch, py::arg("queries"));

    py::class_<ClickHouseConnector>(m, "ClickHouseConnector")
        .def(py::init<>())
        .def("connect", &ClickHouseConnector::connect,
             py::arg("host"), py::arg("port"), 
             py::arg("database") = "default", 
             py::arg("user") = "default", 
             py::arg("password") = "")
        .def("disconnect", &ClickHouseConnector::disconnect)
        .def("is_connected", &ClickHouseConnector::is_connected)
        .def("execute", [](ClickHouseConnector& self, const std::string& query) {
            std::string json_result = self.execute_to_json(query);
            return json_string_to_python_dict(json_result);
        }, py::arg("query"));
}
