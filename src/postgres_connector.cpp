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
            if (!execute_simple_query(query)) {
                throw std::runtime_error("Batch query failed: " + query);
            }
        }
        
        if (!was_in_transaction) {
            return commit_transaction();
        }
        
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Batch execution failed: " << e.what() << std::endl;
        
        if (!was_in_transaction) {
            rollback_transaction();
        }
        
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
    QueryResult result;
    
    if (!is_connected()) {
        throw std::runtime_error("Not connected to PostgreSQL");
    }

    // COUNT запрос
    std::string count_query;
    
    if (query.find("SELECT") != std::string::npos && 
        query.find("COUNT(") == std::string::npos) {
        
        size_t from_pos = query.find("FROM");
        if (from_pos != std::string::npos) {
            count_query = "SELECT COUNT(*) " + query.substr(from_pos);
            
            size_t order_pos = count_query.find("ORDER BY");
            if (order_pos != std::string::npos) {
                count_query = count_query.substr(0, order_pos);
            }
            
            size_t limit_pos = count_query.find("LIMIT");
            if (limit_pos != std::string::npos) {
                count_query = count_query.substr(0, limit_pos);
            }
            
            size_t offset_pos = count_query.find("OFFSET");
            if (offset_pos != std::string::npos) {
                count_query = count_query.substr(0, offset_pos);
            }
        }
    }
    
    if (count_query.empty()) {
        count_query = query;
    }
    
    // Выполняем COUNT запрос
    try {
        PGresult* count_res = PQexec(connection_, count_query.c_str());
        if (PQresultStatus(count_res) == PGRES_TUPLES_OK && PQntuples(count_res) > 0) {
            char* count_str = PQgetvalue(count_res, 0, 0);
            if (count_str) {
                result.count = std::stoul(count_str);
            }
        }
        PQclear(count_res);
    } catch (...) {
        result.count = 0;
    }

    // Основной запрос
    PGresult* res = PQexec(connection_, query.c_str());
    
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::string error = PQresultErrorMessage(res);
        PQclear(res);
        throw std::runtime_error("Query failed: " + error);
    }

    // Получаем информацию о колонках
    int num_cols = PQnfields(res);
    for (int i = 0; i < num_cols; ++i) {
        ColumnInfo col;
        col.name = PQfname(res, i);
        Oid type_oid = PQftype(res, i);
        col.type = oid_to_type_name(type_oid);
        result.columns.push_back(col);
    }

    // Получаем данные - ИСПРАВЛЕННАЯ ЧАСТЬ
    int num_rows = PQntuples(res);
    for (int i = 0; i < num_rows; ++i) {
        std::vector<Value> row;  // Теперь используем Value
        
        for (int j = 0; j < num_cols; ++j) {
            char* value = PQgetvalue(res, i, j);
            if (value != nullptr) {
                row.push_back(std::string(value));  // std::string в variant
            } else {
                row.push_back(nullptr);  // nullptr_t в variant
            }
        }
        result.rows.push_back(row);  // Теперь типы совпадают
    }

    if (result.count == 0) {
        result.count = num_rows;
    }

    PQclear(res);
    return result;
}

std::string PostgresConnector::execute_to_json(const std::string& query) {
    QueryResult result = execute(query);
    return result.to_json();
}