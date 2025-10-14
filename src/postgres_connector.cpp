#include "postgres_connector.h"
#include <iostream>
#include <stdexcept>
#include <unordered_map>

PostgresConnector::PostgresConnector() {
    connection_ = nullptr;
    in_transaction_ = false;
}

PostgresConnector::~PostgresConnector() {
    if (in_transaction_ && connection_) {
        rollback_transaction();
    }
    disconnect();
}

bool PostgresConnector::connect(const std::string& conninfo) {
    connection_ = PQconnectdb(conninfo.c_str());
    in_transaction_ = false;
    return PQstatus(connection_) == CONNECTION_OK;
}

void PostgresConnector::disconnect() {
    if (connection_ != nullptr) {
        if (in_transaction_) {
            rollback_transaction();
        }
        PQfinish(connection_);
        connection_ = nullptr;
        in_transaction_ = false;
    }
}

bool PostgresConnector::is_connected() const {
    return connection_ != nullptr && PQstatus(connection_) == CONNECTION_OK;
}

bool PostgresConnector::is_in_transaction() const {
    return in_transaction_;
}

bool PostgresConnector::execute_simple_query(const std::string& query) {
    if (!is_connected()) {
        return false;
    }

    PGresult* res = PQexec(connection_, query.c_str());
    bool success = (PQresultStatus(res) == PGRES_COMMAND_OK || 
                    PQresultStatus(res) == PGRES_TUPLES_OK);
    
    if (!success) {
        std::cerr << "Query failed: " << PQresultErrorMessage(res) << std::endl;
    }
    
    PQclear(res);
    return success;
}

bool PostgresConnector::begin_transaction() {
    if (!is_connected()) {
        std::cerr << "Cannot begin transaction: not connected" << std::endl;
        return false;
    }
    
    if (in_transaction_) {
        std::cerr << "Transaction already active" << std::endl;
        return false;
    }
    
    if (execute_simple_query("BEGIN")) {
        in_transaction_ = true;
        std::cout << "✓ Transaction started" << std::endl;
        return true;
    }
    
    return false;
}

int64_t PostgresConnector::get_current_transaction_id() {
    if (!in_transaction_) {
        return -1;
    }
    
    try {
        QueryResult result = execute("SELECT txid_current()");
        if (!result.rows.empty() && !result.rows[0].empty()) {
            const Value& txid_value = result.rows[0][0];
            
            if (std::holds_alternative<int64_t>(txid_value)) {
                return std::get<int64_t>(txid_value);
            } else if (std::holds_alternative<std::string>(txid_value)) {
                return std::stoll(std::get<std::string>(txid_value));
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Failed to get transaction ID: " << e.what() << std::endl;
    }
    
    return -1;
}

bool PostgresConnector::commit_transaction() {
    if (!is_connected()) {
        std::cerr << "Cannot commit transaction: not connected" << std::endl;
        return false;
    }
    
    if (!in_transaction_) {
        std::cerr << "No active transaction to commit" << std::endl;
        return false;
    }
    
    if (execute_simple_query("COMMIT")) {
        in_transaction_ = false;
        std::cout << "✓ Transaction committed" << std::endl;
        return true;
    }
    
    return false;
}

bool PostgresConnector::rollback_transaction() {
    if (!is_connected()) {
        std::cerr << "Cannot rollback transaction: not connected" << std::endl;
        return false;
    }
    
    if (!in_transaction_) {
        std::cerr << "No active transaction to rollback" << std::endl;
        return false;
    }
    
    if (execute_simple_query("ROLLBACK")) {
        in_transaction_ = false;
        std::cout << "✓ Transaction rolled back" << std::endl;
        return true;
    }
    
    return false;
}

bool PostgresConnector::execute_batch(const std::vector<std::string>& queries) {
    if (!is_connected()) {
        std::cerr << "Cannot execute batch: not connected" << std::endl;
        return false;
    }
    
    bool was_in_transaction = in_transaction_;
    
    if (!was_in_transaction && !begin_transaction()) {
        return false;
    }
    
    try {
        for (const auto& query : queries) {
            PGresult* res = PQexec(connection_, query.c_str());
            bool success = (PQresultStatus(res) == PGRES_COMMAND_OK || 
                           PQresultStatus(res) == PGRES_TUPLES_OK);
            
            if (!success) {
                std::string error = PQresultErrorMessage(res);
                PQclear(res);

                if (!was_in_transaction) {
                    rollback_transaction();
                } else {
                    rollback_transaction();
                }

                throw std::runtime_error("Batch query failed: " + query + " - " + error);
            }

            PQclear(res);
        }

        if (!was_in_transaction) {
            return commit_transaction();
        }

        return true;

    } catch (const std::exception& e) {
        std::cerr << "Batch execution failed: " << e.what() << std::endl;
        if (in_transaction_) rollback_transaction();
        return false;
    }
}

std::string PostgresConnector::oid_to_type_name(Oid type_oid) const {
    static const std::unordered_map<Oid, std::string> type_map = {
        {16, "bool"}, {17, "bytea"}, {18, "char"}, {20, "int8"}, {21, "int2"},
        {23, "int4"}, {26, "oid"}, {700, "float4"}, {701, "float8"}, {1700, "numeric"},
        {25, "text"}, {1043, "varchar"}, {1042, "bpchar"}, {19, "name"}, {2275, "cstring"},
        {1082, "date"}, {1083, "time"}, {1114, "timestamp"}, {1184, "timestamptz"},
        {1186, "interval"}, {1266, "timetz"}, {114, "json"}, {3802, "jsonb"},
        {1000, "bool[]"}, {1005, "int2[]"}, {1007, "int4[]"}, {1016, "int8[]"},
        {1021, "float4[]"}, {1022, "float8[]"}, {1009, "text[]"}, {1015, "varchar[]"},
        {1231, "numeric[]"}, {600, "point"}, {601, "line"}, {602, "lseg"}, {603, "box"},
        {604, "path"}, {628, "line"}, {869, "inet"}, {650, "cidr"}, {1040, "macaddr"},
        {1041, "macaddr8"}, {142, "xml"}, {2950, "uuid"}, {2278, "bytea"}
    };

    auto it = type_map.find(type_oid);
    if (it != type_map.end()) {
        return it->second;
    }

    if (is_connected()) {
        std::string query = "SELECT typname FROM pg_type WHERE oid = " + std::to_string(type_oid);
        PGresult* res = PQexec(connection_, query.c_str());

        if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
            std::string type_name = PQgetvalue(res, 0, 0);
            PQclear(res);
            return type_name;
        }
        PQclear(res);
    }

    return "oid_" + std::to_string(type_oid);
}

QueryResult PostgresConnector::execute(const std::string& query) {
    if (!is_connected()) {
        throw std::runtime_error("Not connected to PostgreSQL");
    }

    std::string wrapped_query;
    wrapped_query.reserve(query.size() + 128);

    std::string limit_clause;
    {
        // Регэксп ищет "LIMIT <число> [OFFSET <число>]" в конце строки
        std::regex limit_regex(R"((?is)\s+LIMIT\s+\d+(\s+OFFSET\s+\d+)?\s*;?\s*$)");
        std::smatch match;

        if (std::regex_search(query, match, limit_regex)) {
            limit_clause = match.str();                            // сохраняем LIMIT / OFFSET
            query.erase(match.position(), match.length());          // вырезаем из подзапроса
        }
    }

    if (query.find("__total_count") != std::string::npos ||
    query.find("COUNT(") != std::string::npos) {
        wrapped_query = query;
    } else if (query.starts_with("SELECT") || query.starts_with("select")) {
        wrapped_query = "SELECT subq.*, COUNT(*) OVER() AS __total_count FROM (" + query + ") AS subq";
        if (!limit_clause.empty()) {
            wrapped_query += " " + limit_clause; // добавляем лимит снаружи
        }
    } else {
        wrapped_query = query;
    }

    // Выполняем запрос
    PGresult* res = PQexec(connection_, wrapped_query.c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::string error = PQresultErrorMessage(res);
        PQclear(res);
        throw std::runtime_error("Query failed: " + error);
    }

    const int num_cols = PQnfields(res);
    const int num_rows = PQntuples(res);

    QueryResult result;
    result.rows.reserve(num_rows);
    result.columns.reserve(num_cols - 1);

    int total_count_col = -1;

    // Формируем информацию о колонках
    for (int i = 0; i < num_cols; ++i) {
        std::string col_name = PQfname(res, i);
        if (col_name == "__total_count") {
            total_count_col = i;
        } else {
            ColumnInfo col;
            col.name = std::move(col_name);
            Oid type_oid = PQftype(res, i);
            col.type = oid_to_type_name(type_oid);
            result.columns.push_back(std::move(col));
        }
    }

    // Читаем данные
    for (int i = 0; i < num_rows; ++i) {
        std::vector<Value> row;
        row.reserve(num_cols - 1);

        for (int j = 0; j < num_cols; ++j) {
            if (j == total_count_col) continue;

            if (PQgetisnull(res, i, j)) {
                row.push_back(nullptr);
                continue;
            }

            const char* val = PQgetvalue(res, i, j);
            const std::string& type = result.columns[j < total_count_col ? j : j - 1].type;

            if (type == "bool") {
                row.push_back(val[0] == 't');
            } else if (type == "int2" || type == "int4" || type == "int8") {
                row.push_back(std::stoll(val));
            } else if (type == "float4" || type == "float8" || type == "numeric") {
                row.push_back(std::stod(val));
            } else {
                row.push_back(std::string(val));
            }
        }

        result.rows.push_back(std::move(row));
    }

    if (total_count_col >= 0 && num_rows > 0 && !PQgetisnull(res, 0, total_count_col)) {
        result.count = std::stoll(PQgetvalue(res, 0, total_count_col));
    } else {
        result.count = num_rows;
    }

    PQclear(res);
    return result;
}

std::string PostgresConnector::execute_to_json(const std::string& query) {
    QueryResult result = execute(query);
    return result.to_json();
}